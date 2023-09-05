# coding=utf-8
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Define and operate on tasks.

A 'Task' is a collection of attributes that defines a set of metrics that have
been used to score a test set, along with a method for comparing their
performance. Tasks can be run to produce correlation values and clustered rank
assignments resulting from pairwise significance tests over correlations.

A 'TaskSet' is a set of tasks that can be constructed using a compact syntax to
specify different combinations of attributes. TaskSets can be combined to
produce larger sets.

The 'TaskResults' and 'TaskSetResults' classes contain the results of running
Tasks and TaskSets, and provide operations like weighted rank averaging to
summarize the performance of different metrics across tasks.
"""

from __future__ import annotations

import dataclasses
import io
import itertools
import json
from typing import Any, Optional, Union
from mt_metrics_eval import data
from mt_metrics_eval import meta_info
from mt_metrics_eval import stats
import numpy as np
import scipy.stats


CORRELATION_FUNCTIONS = {
    'pearson': scipy.stats.pearsonr,
    'kendall': scipy.stats.kendalltau,
    'spearman': scipy.stats.spearmanr,
    'accuracy': None,  # Implicit in CompareMetricsWithGlobalAccuracy
    'KendallLike': stats.KendallLike,
    'KendallVariants': stats.KendallVariants,
    'KendallWithTiesOpt': stats.KendallWithTiesOpt,
}


def Attributes():
  """List task attributes in canonical order."""
  return list(Task.__annotations__.keys())


@dataclasses.dataclass()
class Task:
  """Parameters for data.GetCorrelations and data.CompareMetrics*."""
  test_set: str = 'wmt22'
  lang: str = 'en-de'
  domain: Optional[str] = None
  level: str = 'sys'
  human: bool = True
  avg_by: str = 'none'
  corr_fcn: str = 'pearson'
  k: int = 1000
  # None selects standard values for the following three parameters.
  gold: Optional[Union[list[str], str]] = None
  refs: Optional[Union[list[set[str]], set[str]]] = None
  close_refs: Optional[Union[list[set[str]], set[str]]] = None
  use_outliers: bool = False
  primary: bool = True
  pval: float = 0.05
  block_size: int = 100
  early_min: float = 0.02
  early_max: float = 0.50
  replace_nans_with_zeros: bool = False
  perm_test: str = 'scores'
  corr_fcn_args: Optional[dict[str, Any]] = None

  def _StdGold(self, lang, level):
    return meta_info.DATA[self.test_set][lang].std_gold[level]

  def _StdRefs(self, lang):
    return {meta_info.DATA[self.test_set][lang].std_ref}

  def __post_init__(self):
    """Check and fill in some default values."""
    test_set, lang, level = self.test_set, self.lang, self.level
    assert test_set in meta_info.DATA
    assert self.corr_fcn in CORRELATION_FUNCTIONS

    sub_langs = lang.split(',')
    if self.corr_fcn == 'accuracy':
      # Special case: system-level accuracy over multiple language pairs.
      assert self.level == 'sys'
      for sl in sub_langs:
        assert sl in meta_info.DATA[test_set], sl
      if self.gold is None:
        self.gold = [self._StdGold(sl, level) for sl in sub_langs]
      if self.refs is None:
        self.refs = [self._StdRefs(sl) for sl in sub_langs]
      if self.close_refs is None:
        self.close_refs = [set() for _ in sub_langs]
      assert len(self.gold) == len(sub_langs)
      assert len(self.refs) == len(sub_langs)
      assert len(self.close_refs) == len(sub_langs)
    else:
      # Standard correlation over single language pair.
      assert len(sub_langs) == 1
      assert lang in meta_info.DATA[test_set], lang
      assert level in meta_info.DATA[test_set][lang].std_gold, level
      if self.gold is None:
        self.gold = self._StdGold(lang, level)
      if self.refs is None:
        self.refs = self._StdRefs(lang)
      if self.close_refs is None:
        self.close_refs = set()
      assert isinstance(self.gold, str)
      assert isinstance(self.refs, set)
      assert isinstance(self.close_refs, set)
    if self.corr_fcn_args is None:
      self.corr_fcn_args = {}
    # Canonical order for comparisons.
    self.corr_fcn_args = dict(sorted(self.corr_fcn_args.items()))

  @property
  def name(self):
    """Single string attr=value representation."""
    return ' '.join(f'{a}={self.StrVal(a)}' for a in Attributes())

  def StrVal(self, attr):
    return f'{getattr(self, attr)}'.replace(' ', '')

  def Run(self, eval_set_dict=None, parallel_file=None) -> TaskResults:
    """Generate metric correlations and pairwise significance results."""

    def _Evs(lp):
      if eval_set_dict is None:
        return data.EvalSet(self.test_set, lp, read_stored_metric_scores=True)
      else:
        return eval_set_dict[(self.test_set, lp)]

    psd = stats.PermutationSigDiffParams(
        self.block_size, self.early_min, self.early_max)

    if self.corr_fcn == 'accuracy':
      evs_list = [_Evs(lp) for lp in self.lang.split(',')]
      res = data.CompareMetricsWithGlobalAccuracy(
          evs_list, self.refs, self.close_refs, self.human,
          self.use_outliers, self.gold, self.primary,
          self.domain, self.k, psd, self.pval,
          parallel_file=parallel_file)
    else:
      corr_fcn = CORRELATION_FUNCTIONS[self.corr_fcn]
      corrs = data.GetCorrelations(
          _Evs(self.lang), self.level, self.refs, self.close_refs, self.human,
          self.use_outliers, self.gold, self.primary, self.domain,
          metric_format='spreadsheet')
      res = data.CompareMetrics(
          corrs, corr_fcn, self.avg_by, self.k, psd, self.pval,
          self.replace_nans_with_zeros, self.perm_test,
          parallel_file=parallel_file, **self.corr_fcn_args)
    return TaskResults(self, res)


class TaskResults:
  """Results from running a Task."""

  def __init__(self, task=None, compare_metrics_results=None):
    """Construct from task and results from CompareMetrics*()."""
    if not task:
      self.name, self.pval = '', 0
      self.corr_ranks, self.matrix = {}, np.array([])
    else:
      self.name, self.pval = task.name, task.pval
      self.corr_ranks, self.matrix = compare_metrics_results

  def __eq__(self, other):
    return (self.name == other.name and
            self.pval == other.pval and
            self.corr_ranks == other.corr_ranks and
            np.array_equal(self.matrix, other.matrix))

  @property
  def attr_vals(self) -> dict[str, str]:
    """Return attr:val representation of task."""
    return dict(av.split('=') for av in self.name.split())

  @property
  def metrics(self) -> list[str]:
    """Metrics in descending order by correlation."""
    return list(self.corr_ranks.keys())

  def Corr(self, metric) -> float:
    """Correlation for metric (by name or index)."""
    if isinstance(metric, int): metric = self.metrics[metric]
    return self.corr_ranks[metric][0]

  def Rank(self, metric: str) -> int:
    """Cluster rank for index (by name or index)."""
    if isinstance(metric, int): metric = self.metrics[metric]
    return self.corr_ranks[metric][1]

  def Sig(self, m1, m2) -> bool:
    """Corr(m1) - Corr(m2) is significant. Difference assumed to be >= 0."""
    if isinstance(m1, str): m1 = self.metrics.index(m1)
    if isinstance(m2, str): m2 = self.metrics.index(m2)
    return self.matrix[m1, m2] < self.pval

  def Str(self, probs=False):
    """Return a string representation."""
    fh = io.StringIO()
    data.PrintMetricComparison(
        self.corr_ranks, self.matrix, self.pval, file=fh, probs=probs)
    return fh.getvalue()

  def Write(self, file):
    elems = (self.name, self.pval, self.corr_ranks, self.matrix.tolist())
    json.dump(elems, file)

  def Read(self, file):
    name, pval, corr_ranks, matrix = json.load(file)
    self.name = name
    self.pval = pval
    self.corr_ranks = corr_ranks
    self.matrix = np.asarray(matrix)
    return self


class TaskSet:
  """Convenience class to create and operate on sets of tasks."""

  def __init__(
      self, attr_combs: Optional[dict[str, list[Any]]] = None, **attrs):
    """Construct with given attribute/value combinations.

    Args:
      attr_combs: Dictionary mapping attributes to lists of values. One Task
        will be created for each complete attribute/value combination (zero
        tasks if attr_combs is None).
      **attrs: Remaining attribute/value pairs to pass to the Task constructor,
        for each task specified by attr_combs.
    """
    self.tasks = []
    self.eval_set_dict = {}  # Lazily set by Run.
    if not attr_combs: return
    for vals in itertools.product(*attr_combs.values()):
      comb = dict(zip(attr_combs.keys(), vals))
      self.tasks.append(Task(**comb, **attrs))

  def _BuildEvalSetDict(self):
    for task in self.tasks:
      for lang in task.lang.split(','):
        if (task.test_set, lang) not in self.eval_set_dict:
          self.eval_set_dict[(task.test_set, lang)] = data.EvalSet(
              task.test_set, lang, True)

  def __len__(self):
    return len(self.tasks)

  def __add__(self, other):
    """Combine tasks sets. Any duplicate tasks will get repeated."""
    res = TaskSet()
    res.tasks = self.tasks + other.tasks
    res.eval_set_dict = {**self.eval_set_dict, **other.eval_set_dict}
    return res

  def __iter__(self):
    return iter(self.tasks)

  def Append(self, task: Task):
    self.tasks.append(task)

  def Run(self) -> TaskSetResults:
    """Run all tasks."""
    self._BuildEvalSetDict()
    return TaskSetResults([task.Run(self.eval_set_dict) for task in self.tasks])


class TaskSetResults:
  """Operations on results from running a TaskSet."""

  def __init__(self, results: list[TaskResults]):
    self.results = results

  def __len__(self):
    return len(self.results)

  def __add__(self, other):
    return TaskSetResults(self.results + other.results)

  def __iter__(self):
    return iter(self.results)

  def Append(self, result: TaskResults):
    self.results.append(result)

  def SplitByAttr(self, attr: str) -> dict[str, TaskSetResults]:
    """Partition into subsets by values of an attribute."""
    subsets = {}
    for result in self.results:
      val = result.attr_vals[attr]
      if val not in subsets:
        subsets[val] = TaskSetResults([])
      subsets[val].Append(result)
    return subsets

  def AssignWeights(
      self, attrs: list[str], total_wt: float = 1.0) -> list[float]:
    """Assign weights to tasks.

    This evenly distributes total_wt across values for the first attribute in
    the attrs list, then recurses. If the attribute list is empty, all remaining
    tasks are equally weighted.

    Args:
      attrs: List of attributes in Attributes().
      total_wt: Total weight to be assigned to tasks.

    Returns:
      List of weights for tasks in results, in order. Weights sum to total_wt.
    """
    if not attrs:
      return [total_wt / len(self.results)] * len(self.results)
    weights = {r.name: 0 for r in self.results}
    subsets = self.SplitByAttr(attrs[0]).values()
    for subset in subsets:
      subweights = subset.AssignWeights(attrs[1:], total_wt / len(subsets))
      # Subset isn't necessarily contiguous within self.results.
      for r, w in zip(subset.results, subweights):
        weights[r.name] = w
    return list(weights.values())

  def AverageRanks(self, weights=None) -> dict[str, float]:
    """Return sorted average weighted rank of metrics available in all tasks.

    Args:
      weights: List of weights, as returned by AssignWeights(). If None, use
        uniform weights.

    Returns:
      Map from metric names to average ranks, ordered by increasing rank. Only
      metrics that appear in all tasks are included.
    """
    if weights is None:
      weights = [1 / len(self)] * len(self)
    ranks = {}
    for res, weight in zip(self.results, weights):
      for metric in res.metrics:
        if metric not in ranks:
          ranks[metric] = []
        ranks[metric].append(res.Rank(metric) * weight)
    ranks = {m: sum(ranks[m]) for m in ranks if len(ranks[m]) == len(self)}
    return dict(sorted(ranks.items(), key=lambda x: x[1]))

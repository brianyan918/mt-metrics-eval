import os
import argparse
from collections import defaultdict
from pathlib import Path
import shutil

def read_lines(file):
    with open(file, encoding='utf-8') as f:
        return [line.rstrip('\n') for line in f]

def write_lines(file, lines):
    with open(file, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, help="e.g., wmt23")
    parser.add_argument("--lp", required=True, help="e.g., en-de")
    parser.add_argument("--mtme_data_path", required=True, help="e.g., ~/.mt-metrics-eval/mt-metrics-eval-v2/")
    parser.add_argument("--type", required=True, help="e.g., mqm or esa")
    return parser.parse_args()

def build_segment_to_doc_map(docs_lines):
    doc_map = defaultdict(list)
    seg_to_doc = {}
    for idx, line in enumerate(docs_lines):
        domain, doc_id = line.split('\t')
        full_doc_id = f"{domain}__{doc_id}"
        doc_map[full_doc_id].append(idx)
        seg_to_doc[idx] = full_doc_id
    return doc_map, seg_to_doc

def merge_text_lines_by_map(lines, doc_map):
    merged = {}
    for doc_id, indices in doc_map.items():
        merged[doc_id] = "\n".join([lines[i] for i in indices])
    return merged

def merge_scores_by_map(score_lines, doc_map):
    merged = defaultdict(lambda: defaultdict(list))
    for line in score_lines:
        system, score = line.strip().split('\t')
        score = float(score) if score != 'None' else 'None'
        seg_id = merge_scores_by_map.seg_counter % merge_scores_by_map.total_segments
        doc_id = merge_scores_by_map.seg_to_doc[seg_id]
        merged[system][doc_id].append(score)
        merge_scores_by_map.seg_counter += 1
    merged_scores = defaultdict(lambda: defaultdict(float))
    count = 0
    for system in merged.keys():
        for doc_id in merged[system].keys():
            try:
                merged_scores[system][doc_id] = sum(merged[system][doc_id])
                # tmp = [x for x in merged[system][doc_id] if x != 'None']
                # assert len(tmp) != 0
                # merged_scores[system][doc_id] = sum(tmp)
            except:
                for seg in merged[system][doc_id]:
                    try:
                        assert seg == 'None'
                    except:
                        count += 1
                        # print(doc_id, system, merged[system][doc_id])
                        break
                merged_scores[system][doc_id] = 'None'
    print(f"defaulted to None on {count} docs")
    return merged_scores

def merge_system_outputs(system_dir, output_dir, doc_map):
    os.makedirs(output_dir, exist_ok=True)
    for system_file in Path(system_dir).glob("*"):
        lines = read_lines(system_file)
        assert len(lines) == merge_system_outputs.total_segments
        merged = merge_text_lines_by_map(lines, doc_map)
        out_file = output_dir / system_file.name
        write_lines(out_file, [merged[doc_id] for doc_id in sorted(merged)])

def main():
    args = parse_args()
    base_path = Path(args.mtme_data_path) / args.year
    docs_file = base_path / "documents" / f"{args.lp}.docs"
    sources_file = base_path / "sources" / f"{args.lp}.txt"
    references_file = base_path / "references" / f"{args.lp}.refA.txt"
    scores_file = base_path / "human-scores" / f"{args.lp}.{args.type}.seg.score"
    systems_dir = base_path / "system-outputs" / args.lp
    out_lp = f"{args.lp}_doc"

    # Step 1: Build segment to document mapping
    docs_lines = read_lines(docs_file)
    doc_map, seg_to_doc = build_segment_to_doc_map(docs_lines)
    total_segments = len(docs_lines)
    print(f"✅ Loaded {total_segments} segment mappings into {len(doc_map)} documents.")

    # Step 2: Merge sources
    source_lines = read_lines(sources_file)
    assert len(source_lines) == total_segments
    merged_sources = merge_text_lines_by_map(source_lines, doc_map)
    write_lines(base_path / "sources" / f"{out_lp}.txt", [merged_sources[doc_id] for doc_id in sorted(merged_sources)])

    # Step 3: Merge references
    ref_lines = read_lines(references_file)
    assert len(ref_lines) == total_segments
    merged_refs = merge_text_lines_by_map(ref_lines, doc_map)
    write_lines(base_path / "references" / f"{out_lp}.refA.txt", [merged_refs[doc_id] for doc_id in sorted(merged_refs)])

    # Step 4: Merge scores
    score_lines = read_lines(scores_file)
    systems = set(line.split('\t')[0] for line in score_lines)
    expected_lines = len(systems) * total_segments
    assert len(score_lines) == expected_lines, "Mismatch in score lines vs expected lines"

    merge_scores_by_map.total_segments = total_segments
    merge_scores_by_map.seg_counter = 0
    merge_scores_by_map.seg_to_doc = seg_to_doc
    merged_scores = merge_scores_by_map(score_lines, doc_map)

    with open(base_path / "human-scores" / f"{out_lp}.{args.type}.seg.score", 'w') as out_f:
        for system in sorted(merged_scores.keys()):
            for doc_id in sorted(merged_scores[system].keys()):
                out_f.write(f"{system}\t{merged_scores[system][doc_id]}\n")

    # Step 5: Merge system outputs
    system_out_dir = base_path / "system-outputs" / out_lp
    merge_system_outputs.total_segments = total_segments
    merge_system_outputs(doc_map=doc_map, system_dir=systems_dir, output_dir=system_out_dir)

    # Step 6: Merge .docs file
    merged_docs_lines = []
    for doc_id in sorted(doc_map):
        domain, raw_doc_id = doc_id.split('__', 1)
        merged_docs_lines.append(f"{domain}\t{raw_doc_id}")
    write_lines(base_path / "documents" / f"{out_lp}.docs", merged_docs_lines)

    # Step 7: Copy sys-level and domain-level MQM scores
    score_path = base_path / "human-scores"
    sys_score_file = score_path / f"{args.lp}.{args.type}.sys.score"
    domain_score_file = score_path / f"{args.lp}.{args.type}.domain.score"

    shutil.copy(sys_score_file, score_path / f"{out_lp}.{args.type}.sys.score")
    shutil.copy(domain_score_file, score_path / f"{out_lp}.{args.type}.domain.score")

    print("✅ Done. All files converted to document-level.")

if __name__ == "__main__":
    main()

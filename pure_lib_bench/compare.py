#!/usr/bin/env python3
"""Compare pure-library snapshots with the valkey-search VDB-order snapshots.

Reads the smaps files produced by bench_hnsw/bench_svs at the "stage3_indexed"
and "stage4_after_drop_input" checkpoints and prints a side-by-side bucket
breakdown. Also pulls VmRSS from the valkey-search run for reference.
"""
import os
import re
import sys
from pathlib import Path


def parse_smaps(path):
    buckets = {}
    cur = None
    pat = re.compile(
        r'^([0-9a-f]+)-([0-9a-f]+)\s+\S+\s+\S+\s+\S+\s+\S+\s*(.*)$')
    with open(path) as f:
        for line in f:
            m = pat.match(line)
            if m:
                if cur is not None:
                    b = cur['path'] or '(anon)'
                    if b == '(anon)':
                        b = '(anon, mmap)'
                    elif '[heap]' in b:
                        b = '[heap] (glibc main arena)'
                    elif 'libsvs_runtime' in b:
                        b = 'libsvs_runtime.so (code)'
                    elif '.so' in b:
                        b = 'other .so (code)'
                    elif '[' in b:
                        pass
                    else:
                        b = b.rsplit('/', 1)[-1]
                    buckets.setdefault(b, {'virt': 0, 'rss': 0})
                    buckets[b]['virt'] += cur['size']
                    buckets[b]['rss'] += cur.get('rss', 0)
                sz = int(m.group(2), 16) - int(m.group(1), 16)
                cur = {'size': sz, 'path': m.group(3).strip()}
            elif line.startswith('Rss:'):
                cur['rss'] = int(line.split()[1]) * 1024
    return buckets


def parse_vmrss(status_path):
    with open(status_path) as f:
        for line in f:
            if line.startswith('VmRSS:'):
                return int(line.split()[1])
    return 0


def report(label, smaps_path):
    if not smaps_path.exists():
        print(f"  [{label}] missing: {smaps_path}")
        return
    b = parse_smaps(smaps_path)
    total = sum(v['rss'] for v in b.values()) / 1048576
    print(f"  [{label}] total RSS ≈ {total:.1f} MiB")
    for k, v in sorted(b.items(), key=lambda kv: -kv[1]['rss']):
        if v['rss'] / 1048576 < 1:
            continue
        print(f"    {k:<40} virt={v['virt']/1048576:8.1f} MiB  rss={v['rss']/1048576:8.1f} MiB")


def main():
    N = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
    root = Path('/tmp/pure_lib_bench') / str(N)

    print(f"=== N={N} (pure-library) ===\n")

    configs = [('hnsw', 'hnsw'),
               ('svs_fp32', 'svs_fp32'),
               ('svs_lvq4x8', 'svs_lvq4x8'),
               ('svs_lvq4x8 block_exp=22', 'svs_lvq4x8_block22')]

    for label, dirname in configs:
        d = root / dirname
        if not d.exists():
            print(f"{label}: not run")
            continue
        print(f"--- {label} ---")
        status_file = d / 'stage3_indexed' / 'status.txt'
        if status_file.exists():
            vmrss = parse_vmrss(status_file)
            print(f"  stage3_indexed VmRSS={vmrss} kB ({vmrss/1024:.1f} MiB)")
        status4 = d / 'stage4_after_drop_input' / 'status.txt'
        if status4.exists():
            vmrss = parse_vmrss(status4)
            print(f"  stage4_after_drop_input VmRSS={vmrss} kB ({vmrss/1024:.1f} MiB)")
        smaps4 = d / 'stage4_after_drop_input' / 'smaps.txt'
        report('stage4 smaps', smaps4)
        print()

    # Cross-reference the valkey-search VDB-order numbers (whole-process).
    vkb = Path('/tmp/memory_experiment_vdb')
    if vkb.exists() and N == 100000:
        print("=== Reference: valkey-search VDB-order stage2_indexed VmRSS ===")
        for algo in ('hnsw', 'svs_fp32', 'svs_lvq4x8'):
            f = vkb / f"{algo}_stage2_indexed.txt"
            if not f.exists():
                continue
            with open(f) as fh:
                for line in fh:
                    if line.startswith('VmRSS:'):
                        kb = int(line.split()[1])
                        print(f"  valkey-search {algo:12s} VmRSS={kb} kB ({kb/1024:.1f} MiB)")
                        break


if __name__ == '__main__':
    main()

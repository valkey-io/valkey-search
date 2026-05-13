// Shared utilities for the two benchmarks: a deterministic vector generator
// and a snapshot helper that writes VmRSS / smaps to disk.

#pragma once
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <random>
#include <string>
#include <vector>

inline std::vector<float> generate_vectors(size_t n, size_t dim,
                                           uint32_t seed = 1) {
  std::vector<float> v(n * dim);
  std::mt19937 rng(seed);
  std::uniform_real_distribution<float> d(0.0f, 1.0f);
  for (size_t i = 0; i < v.size(); ++i) v[i] = d(rng);
  return v;
}

inline size_t read_vmrss_kb() {
  std::ifstream f("/proc/self/status");
  std::string line;
  while (std::getline(f, line)) {
    if (line.rfind("VmRSS:", 0) == 0) {
      long v = 0;
      std::sscanf(line.c_str(), "VmRSS: %ld kB", &v);
      return static_cast<size_t>(v);
    }
  }
  return 0;
}

// Copy /proc/self/{status,smaps} to a destination directory and print a
// one-line summary. The smaps file is post-processed into bucketed totals
// by a Python helper (compare.py) — we just dump the raw data here.
// Read a /proc/self/* file in-process (no fork/cp) and write to dest.
// Using `cp` would capture cp's smaps, not ours.
inline void copy_proc_file(const char* src, const std::string& dst) {
  std::ifstream in(src);
  std::ofstream out(dst);
  out << in.rdbuf();
}

inline void snapshot(const std::string& out_dir, const std::string& label) {
  std::string mkdir_cmd = "mkdir -p '" + out_dir + "'";
  (void)std::system(mkdir_cmd.c_str());

  copy_proc_file("/proc/self/status", out_dir + "/status.txt");
  copy_proc_file("/proc/self/smaps", out_dir + "/smaps.txt");

  size_t rss_kb = read_vmrss_kb();
  std::printf("  [%s] VmRSS=%zu kB (%.1f MiB)\n", label.c_str(), rss_kb,
              rss_kb / 1024.0);
  std::fflush(stdout);

  std::ofstream summary(out_dir + "/summary.txt");
  summary << label << " VmRSS=" << rss_kb << " kB ("
          << (rss_kb / 1024.0) << " MiB)\n";
}

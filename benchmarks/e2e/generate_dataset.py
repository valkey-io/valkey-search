#!/usr/bin/env python3
"""
Generates 10,000 rich text documents with high-cardinality vocabulary and multi-value tags.
Each document contains exactly 1,500 words (15M words total) plus 5 TAG values.
Saved into benchmarks/e2e/data/documents.txt (one doc per line formatted as: tags\tbody).
Generates search queries in benchmarks/e2e/queries.txt (text, prefix, tag, and compound queries).
"""

import os
import random
import math
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_FILE = os.path.join(DATA_DIR, "documents.txt")
QUERIES_FILE = os.path.join(os.path.dirname(__file__), "queries.txt")
NUM_DOCS = 10000
WORDS_PER_DOC = 1500
RANDOM_SEED = 42

def load_or_generate_vocabulary():
    dict_path = "/usr/share/dict/words"
    all_words = []
    
    if os.path.exists(dict_path):
        with open(dict_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                w = line.strip().lower()
                if w.isalpha() and 2 <= len(w) <= 20:
                    all_words.append(w)
    
    # Ensure wide vocabulary by adding synthetic terms if needed
    if len(all_words) < 100000:
        base_stems = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
                      "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi", "rho",
                      "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega", "search",
                      "index", "alloc", "pmr", "radix", "vector", "node", "tree", "memory"]
        for stem in base_stems:
            for i in range(2000):
                all_words.append(f"{stem}{i:04d}")
                
    words_by_len = defaultdict(list)
    for w in all_words:
        words_by_len[len(w)].append(w)
        
    return all_words, words_by_len

def generate_documents():
    random.seed(RANDOM_SEED)
    os.makedirs(DATA_DIR, exist_ok=True)
    
    all_words, words_by_len = load_or_generate_vocabulary()
    available_lengths = sorted(words_by_len.keys())
    
    mu, sigma = 5.1, 2.2
    
    # Tag pools for high-cardinality tag indexing
    categories = [f"cat_{i:03d}" for i in range(100)]
    statuses = ["active", "pending", "archived", "verified", "draft", "reviewed"]
    regions = [f"region_{i:02d}" for i in range(50)]
    departments = [f"dept_{i:03d}" for i in range(200)]
    
    sample_queries = []
    prefix_sample_words = []
    
    print(f"Generating {NUM_DOCS} documents (1,500 words each = {NUM_DOCS * WORDS_PER_DOC:,} words) with TAGs...")
    with open(DB_FILE, "w", encoding="utf-8") as out_f:
        for doc_idx in range(NUM_DOCS):
            # Generate 5 multi-value tags
            doc_tags = [
                random.choice(categories),
                random.choice(statuses),
                random.choice(regions),
                random.choice(departments),
                f"sku_{random.randint(1, 1000):04d}"
            ]
            tags_str = ",".join(doc_tags)
            
            # Generate 1500 words
            doc_words = []
            for _ in range(WORDS_PER_DOC):
                target_len = int(round(random.gauss(mu, sigma)))
                target_len = max(2, min(16, target_len))
                if target_len not in words_by_len:
                    target_len = min(available_lengths, key=lambda l: abs(l - target_len))
                w = random.choice(words_by_len[target_len])
                doc_words.append(w)
                
            body_text = " ".join(doc_words)
            # Format: tags<TAB>body
            out_f.write(f"{tags_str}\t{body_text}\n")
            
            if doc_idx < 1000:
                prefix_sample_words.extend(doc_words[:5])
                sample_queries.append(f"@tags:{{{doc_tags[0]}}}")
                sample_queries.append(f"@tags:{{{doc_tags[1]}}}")
                
            if (doc_idx + 1) % 2000 == 0:
                print(f"  Generated {doc_idx + 1}/{NUM_DOCS} documents...")

    print("Generating comprehensive queries (exact, prefix*, tag, compound)...")
    unique_words = list(set(prefix_sample_words))
    queries = []
    
    # 1. Exact text terms
    for w in unique_words[:100]:
        if len(w) >= 3:
            queries.append(w)
            
    # 2. Prefix wildcard queries (prefix*)
    for w in unique_words[100:250]:
        if len(w) >= 4:
            queries.append(f"{w[:3]}*")
            
    # 3. TAG filter queries
    queries.extend(list(set(sample_queries))[:80])
    
    # 4. Compound queries (@tags:{cat} text_prefix*)
    for i in range(min(50, len(unique_words) - 250)):
        cat = random.choice(categories)
        w = unique_words[250 + i]
        if len(w) >= 4:
            queries.append(f"@tags:{{{cat}}} {w[:3]}*")
            
    # 5. Multi-term conjunctions
    for i in range(0, min(100, len(unique_words) - 1), 2):
        queries.append(f"{unique_words[i]} {unique_words[i+1]}")
        
    random.shuffle(queries)
    with open(QUERIES_FILE, "w", encoding="utf-8") as f:
        for q in queries:
            f.write(q + "\n")
            
    print(f"Dataset generated! {NUM_DOCS} docs in {DB_FILE}, {len(queries)} queries in {QUERIES_FILE}")

if __name__ == "__main__":
    generate_documents()

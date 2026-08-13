#!/usr/bin/env python3
"""
Generates 10,000 documents with rich high-cardinality vocabulary and 5 multi-value TAGs.
Each document contains 500 words and 5 tags.
Saved in benchmarks/e2e/data/documents.txt (format: tags\tbody).
Generates search queries in benchmarks/e2e/queries.txt (prefix*, tags, and exact terms).
"""

import os
import random
import math
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_FILE = os.path.join(DATA_DIR, "documents.txt")
QUERIES_FILE = os.path.join(os.path.dirname(__file__), "queries.txt")
NUM_DOCS = 30000
WORDS_PER_DOC = 500
RANDOM_SEED = 42

def load_or_generate_vocabulary():
    dict_path = "/usr/share/dict/words"
    all_words = []
    
    if os.path.exists(dict_path):
        with open(dict_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                w = line.strip().lower()
                if w.isalpha() and 3 <= len(w) <= 15:
                    all_words.append(w)
    
    if len(all_words) < 50000:
        base_stems = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
                      "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi", "rho",
                      "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega", "search",
                      "index", "alloc", "pmr", "radix", "vector", "node", "tree", "memory"]
        for stem in base_stems:
            for i in range(1000):
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
    
    categories = [f"cat_{i:03d}" for i in range(100)]
    statuses = ["active", "pending", "archived", "verified", "draft", "reviewed"]
    regions = [f"region_{i:02d}" for i in range(50)]
    departments = [f"dept_{i:03d}" for i in range(200)]
    
    sample_queries = []
    prefix_sample_words = []
    
    print(f"Generating {NUM_DOCS} documents (500 words each = {NUM_DOCS * WORDS_PER_DOC:,} words) with TAGs...")
    with open(DB_FILE, "w", encoding="utf-8") as out_f:
        for doc_idx in range(NUM_DOCS):
            doc_tags = [
                random.choice(categories),
                random.choice(statuses),
                random.choice(regions),
                random.choice(departments),
                f"sku_{random.randint(1, 1000):04d}"
            ]
            tags_str = ",".join(doc_tags)
            
            doc_words = []
            for _ in range(WORDS_PER_DOC):
                target_len = int(round(random.gauss(mu, sigma)))
                target_len = max(3, min(15, target_len))
                if target_len not in words_by_len:
                    target_len = min(available_lengths, key=lambda l: abs(l - target_len))
                w = random.choice(words_by_len[target_len])
                doc_words.append(w)
                
            body_text = " ".join(doc_words)
            out_f.write(f"{tags_str}\t{body_text}\n")
            
            if doc_idx < 1000:
                prefix_sample_words.extend(doc_words[:5])
                sample_queries.append(f"@tags:{{{doc_tags[0]}}}")
                sample_queries.append(f"@tags:{{{doc_tags[1]}}}")
                
            if (doc_idx + 1) % 2000 == 0:
                print(f"  Generated {doc_idx + 1}/{NUM_DOCS} documents...")

    print("Generating clean queries...")
    unique_words = list(set(w for w in prefix_sample_words if w.isalpha() and len(w) >= 3))
    queries = []
    
    # 1. Prefix wildcard queries (prefix*)
    for w in unique_words[:200]:
        if len(w) >= 4:
            queries.append(f"{w[:3]}*")
            
    # 2. TAG queries
    queries.extend(list(set(sample_queries))[:100])
    
    # 3. Exact terms
    for w in unique_words[200:300]:
        queries.append(w)
            
    random.shuffle(queries)
    with open(QUERIES_FILE, "w", encoding="utf-8") as f:
        for q in queries:
            f.write(q + "\n")
            
    print(f"Dataset generated! {NUM_DOCS} docs in {DB_FILE}, {len(queries)} queries in {QUERIES_FILE}")

if __name__ == "__main__":
    generate_documents()

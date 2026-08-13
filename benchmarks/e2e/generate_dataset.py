#!/usr/bin/env python3
"""
Generates 10,000 text documents where each document contains exactly 500 words,
with word lengths following a normal distribution of English words (mean ~5.1, std ~2.2).
All documents are saved in a single database file: benchmarks/e2e/data/documents.txt
where each line represents a separate document.
Also generates representative search queries in benchmarks/e2e/queries.txt.
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
    words_by_len = defaultdict(list)
    
    if os.path.exists(dict_path):
        with open(dict_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                w = line.strip().lower()
                if w.isalpha() and 1 <= len(w) <= 20:
                    words_by_len[len(w)].append(w)
    
    # Fallback if dictionary missing or incomplete
    common_stems = ["the", "be", "to", "of", "and", "in", "that", "have", "with", "this",
                    "from", "they", "word", "what", "some", "time", "look", "more", "search",
                    "index", "memory", "alloc", "fast", "scale", "query", "system", "engine",
                    "cloud", "stream", "vector", "record", "prefix", "storage", "performance",
                    "radix", "custom", "resource", "structure", "concurrency", "optimization"]
    for w in common_stems:
        words_by_len[len(w)].append(w)
        
    return words_by_len

def generate_documents():
    random.seed(RANDOM_SEED)
    os.makedirs(DATA_DIR, exist_ok=True)
    
    words_by_len = load_or_generate_vocabulary()
    available_lengths = sorted(words_by_len.keys())
    
    # English word length distribution: Gaussian(mu=5.1, sigma=2.2)
    mu, sigma = 5.1, 2.2
    
    all_selected_words = []
    
    print(f"Generating {NUM_DOCS} documents into single file {DB_FILE}...")
    with open(DB_FILE, "w", encoding="utf-8") as out_f:
        for doc_idx in range(NUM_DOCS):
            doc_words = []
            for _ in range(WORDS_PER_DOC):
                target_len = int(round(random.gauss(mu, sigma)))
                target_len = max(1, min(16, target_len))
                
                if target_len not in words_by_len:
                    target_len = min(available_lengths, key=lambda l: abs(l - target_len))
                
                w = random.choice(words_by_len[target_len])
                doc_words.append(w)
                
            doc_text = " ".join(doc_words)
            out_f.write(doc_text + "\n")
            
            if doc_idx < 500:
                all_selected_words.extend(doc_words[:10])
                
            if (doc_idx + 1) % 2000 == 0:
                print(f"  Generated {doc_idx + 1}/{NUM_DOCS} documents...")

    print("Generating queries...")
    unique_words = list(set(all_selected_words))
    queries = []
    
    # Exact words
    for w in unique_words[:100]:
        if len(w) >= 3:
            queries.append(w)
            
    # Prefix queries (take first 3-4 chars + *)
    for w in unique_words[100:200]:
        if len(w) >= 4:
            queries.append(f"{w[:3]}*")
            
    # 2-word conjunctions
    for i in range(200, min(300, len(unique_words) - 1), 2):
        queries.append(f"{unique_words[i]} {unique_words[i+1]}")
        
    with open(QUERIES_FILE, "w", encoding="utf-8") as f:
        for q in queries:
            f.write(q + "\n")
            
    print(f"Dataset generated successfully! {NUM_DOCS} docs in {DB_FILE}, {len(queries)} queries in {QUERIES_FILE}")

if __name__ == "__main__":
    generate_documents()

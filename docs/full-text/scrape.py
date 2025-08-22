import requests
from bs4 import BeautifulSoup
import re
import time
import redis

# -------------------------------------------------------------------
# A simple Radix Tree implementation
# -------------------------------------------------------------------
class RadixTree:
    def __init__(self):
        self.children = {}
        self.postings = 0

    def insert(self, word):
        """
        Insert a single word into the radix tree.
        """
        if len(word) == 0:
            self.postings = self.postings + 1
        else:
            if word[0] not in self.children:
                self.children[word[0]] = RadixTree()
            self.children[word[0]].insert(word[1:])

    def count_nodes(self):
        """
        Returns total number of nodes in the tree (including the root).
        """
        count = len(self.children)
        if self.postings > 0:
            count = count + 1
        for c in self.children.values():
            count += c.count_nodes()
        return count

    def count_single_child_nodes(self):
        """
        Returns how many nodes have exactly one child.
        """
        count = 0
        if self.postings == 0 and len(self.children) == 1:
            count = 1
        for c in self.children.values():
            count += c.count_single_child_nodes()
        return count

# -------------------------------------------------------------------
# Helper function to fetch random Wikipedia text
# -------------------------------------------------------------------
def fetch_random_wikipedia_page_text():
    """
    Fetches a random Wikipedia page and returns its text content.
    """
    url = "https://en.wikipedia.org/wiki/Special:Random"
    headers = {
        "User-Agent": "valkey-search/0.1"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch page: {e}")
        return ""

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract text from <p> tags as a simple approach
    paragraphs = soup.find_all("p")
    page_text = ""
    for p in paragraphs:
        page_text += p.get_text() + "\n"

    # Simple cleaning
    # Remove extra whitespace, newlines, references in brackets, etc.
    page_text = re.sub(r"\[\d+\]", "", page_text)  # remove reference markers
    page_text = re.sub(r"\s+", " ", page_text).strip()
    return page_text

client = redis.Redis(host='localhost', port=6379, decoded_responses=True)

def memory_usage():
    x = client.execute_command("memory stats")
    return int(x["total.allocated"])

# -------------------------------------------------------------------
# Main script to scrape, build tree, and show statistics
# -------------------------------------------------------------------
def main():
    key_count = 0
    key_size = 0
    word_size = 0
    word_count = 0
    stop_word_count = 0
    postings_space4 = 0
    postings_space8 = 0
    reverse_space = 0
    
    NUM_PAGES = 10000
    NUM_STATS = 100
    tree = RadixTree()

    client.execute_command("flushall sync")
    client.execute_command("ft.create x on hash schema x text")

    start_usage = memory_usage()
    print("At startup memory = ", start_usage)
    
    stop_words = {"a":0, "is":0, "the":0, "an":0, "and":0, "are":0, "as":0, "at":0, "be":0, "but":0, "by":0, "for":0, "if":0, "in":0, "into":0, "it":0, "no":0, "not":0, "of":0, "on":0, "or":0, "such":0, "that":0, "their":0, "then":0, "there":0, "these":0, "they":0, "this":0, "to":0, "was":0, "will":0, "with":0}

    for i in range(NUM_PAGES):
        text = fetch_random_wikipedia_page_text()
        
        if not text:
            # If the text is empty (fetch failure)", skip
            continue
        
        # Split into sentences (very naive). You can improve with nltk sent_tokenize.
        sentences = re.split(r'[.!?]+', text)
        
        for sentence in sentences:
            # Further split into words (again naive)
            client.hset(str(key_count), {"x":sentence})
            words = re.split(r'\W+', sentence)
            words = [w.lower() for w in words if w]  # remove empty strings, to lower
            key_count = key_count + 1
            key_size = key_size + len(sentence)
            per_sentence = {}
            for word in words:
                word_count = word_count + 1
                word_size = word_size + len(word)
                if word in stop_words:
                    stop_word_count = stop_word_count + 1
                else:
                    if word in per_sentence:
                        postings_space4 = postings_space4 + 1
                        postings_space8 = postings_space8 + 1
                    else:
                        postings_space4 = postings_space4 + 5
                        postings_space8 = postings_space8 + 9
                    per_sentence[word] = True
                    tree.insert(word)
        if (i % NUM_STATS) == 0:
            # After inserting all words from pages, compute statistics
            total_nodes = tree.count_nodes()
            single_child_nodes = tree.count_single_child_nodes()
            word_nodes = total_nodes - single_child_nodes
            space8 = (word_nodes * 8) + \
                int(single_child_nodes * 1.5) + \
                 postings_space8
            space4 = (word_nodes * 8) + int(single_child_nodes * 1.5) + postings_space4 + \
                (key_count * 4)
                
            space44 = space4 - (word_nodes * 4)

            redis_usage = memory_usage() - start_usage
                
            print(f"Keys:{key_count} AvgKeySize:{key_size/key_count:.1f} Words:{word_count}/{stop_word_count} AvgWord:{word_size/word_count:.1f} Postings_space:{postings_space8//1024}/{postings_space4//1024}KB Space:{space8//1024}/{space4//1024}/{space44//1024}KB Space/Word:{space8/word_count:.1f}/{space4/word_count:.1f} Space/Corpus:{space8/key_size:.1f}/{space4/key_size:.1f}/{space44/key_size:.1f} Redis:{redis_usage//1024}KB /Key:{redis_usage//key_count} /Word:{redis_usage//word_count} Ratio:{space8/redis_usage:.1f}/{space4/redis_usage:.1f}" )

if __name__ == "__main__":
    main()



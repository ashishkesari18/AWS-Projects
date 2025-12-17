import time
import requests
from bs4 import BeautifulSoup
import json
from pathlib import Path
from datetime import datetime, timezone


MAX_POSTS = 100          
SLEEP_SECONDS = 8        

SEARCH_URLS = [
    "https://old.reddit.com/search?q=Amazon+Pharmacy+delay",
    "https://old.reddit.com/search?q=Amazon+Pharmacy+insurance",
    "https://old.reddit.com/search?q=Amazon+Pharmacy+prior+authorization",
    "https://old.reddit.com/search?q=Amazon+Pharmacy+refill",
    "https://old.reddit.com/search?q=Amazon+Pharmacy+customer+support",
    "https://old.reddit.com/search?q=Amazon+Pharmacy+stuck",
    "https://old.reddit.com/search?q=Amazon+Rx+delay",
    "https://old.reddit.com/search?q=Amazon+Rx+insurance",
    "https://old.reddit.com/search?q=Amazon+Rx+refill",
    "https://old.reddit.com/search?q=Amazon+Pharmacy+Zepbound",
    "https://old.reddit.com/search?q=Amazon+Pharmacy+Ozempic",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Educational Research Project)"
}

OUT_DIR = Path("data/bronze/posts")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUT_DIR / "reddit_posts.json"


# INGESTION

collected = []

for search_url in SEARCH_URLS:
    if len(collected) >= MAX_POSTS:
        break

    response = requests.get(search_url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(response.text, "html.parser")

    # old.reddit.com search results
    posts = soup.select("div.search-result")

    for post in posts:
        if len(collected) >= MAX_POSTS:
            break

        title_tag = post.select_one("a.search-title")
        title = title_tag.get_text(strip=True) if title_tag else ""

        if not title:
            continue

        collected.append({
            "source": "reddit",
            "created_date": datetime.now(timezone.utc).date().isoformat(),
            "title": title,
            "body_text": ""  
        })

        time.sleep(SLEEP_SECONDS)

# SAVE TO BRONZE
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(collected, f, indent=2, ensure_ascii=False)

print(f"Collected {len(collected)} posts ethically.")
print(f"Saved to {OUTPUT_FILE}")

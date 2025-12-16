import os, json, time, datetime
from pathlib import Path
from dotenv import load_dotenv
import praw

load_dotenv()

SUBREDDITS = ["AmazonPharmacy", "pharmacy", "insurance", "ChronicIllness", "amazonprime"]
KEYWORDS = ["amazon pharmacy", "prescription", "refill", "delay", "stuck", "prior authorization", "insurance", "copay", "support"]

BASE_DIR = Path("data/bronze")
WATERMARK_PATH = BASE_DIR / "_watermark.json"

def load_watermark():
    if WATERMARK_PATH.exists():
        return json.loads(WATERMARK_PATH.read_text()).get("last_created_utc", 0)
    return 0

def save_watermark(last_created_utc: int):
    WATERMARK_PATH.parent.mkdir(parents=True, exist_ok=True)
    WATERMARK_PATH.write_text(json.dumps({"last_created_utc": last_created_utc}, indent=2))

def today_partition():
    return datetime.date.today().isoformat()

def main(limit_per_query=50):
    reddit = praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ["REDDIT_USER_AGENT"],
    )

    last_seen = load_watermark()
    dt = today_partition()

    posts_out = BASE_DIR / f"posts/dt={dt}/posts.jsonl"
    comments_out = BASE_DIR / f"comments/dt={dt}/comments.jsonl"
    posts_out.parent.mkdir(parents=True, exist_ok=True)
    comments_out.parent.mkdir(parents=True, exist_ok=True)

    extracted_at = datetime.datetime.utcnow().isoformat()

    max_created = last_seen
    seen_posts = set()
    seen_comments = set()

    with posts_out.open("a", encoding="utf-8") as fp_posts, comments_out.open("a", encoding="utf-8") as fp_comments:
        for sub in SUBREDDITS:
            subreddit = reddit.subreddit(sub)
            for kw in KEYWORDS:
                for post in subreddit.search(kw, limit=limit_per_query, sort="new"):
                    created = int(post.created_utc)
                    if created <= last_seen:
                        continue
                    if post.id in seen_posts:
                        continue
                    seen_posts.add(post.id)
                    max_created = max(max_created, created)

                    record = {
                        "post_id": post.id,
                        "subreddit": str(post.subreddit),
                        "created_utc": created,
                        "title": post.title,
                        "body": post.selftext,
                        "score": post.score,
                        "num_comments": post.num_comments,
                        "permalink": post.permalink,
                        "extracted_at": extracted_at,
                    }
                    fp_posts.write(json.dumps(record, ensure_ascii=False) + "\n")

                    # Pull top-level comments (lightweight)
                    try:
                        post.comments.replace_more(limit=0)
                        for c in post.comments.list()[:200]:
                            if c.id in seen_comments:
                                continue
                            seen_comments.add(c.id)
                            fp_comments.write(json.dumps({
                                "comment_id": c.id,
                                "post_id": post.id,
                                "created_utc": int(c.created_utc),
                                "body": getattr(c, "body", ""),
                                "score": getattr(c, "score", None),
                                "extracted_at": extracted_at,
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass

                time.sleep(1)

    if max_created > last_seen:
        save_watermark(max_created)

    print(f"Done. New posts: {len(seen_posts)}, comments: {len(seen_comments)}. Watermark: {max_created}")

if __name__ == "__main__":
    main()
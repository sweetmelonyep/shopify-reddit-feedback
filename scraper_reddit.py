"""Reddit scraper for Shopify merchant feedback on Shopify ad products.

Uses Reddit's public .json endpoint — no API key required.
"""

import json
import logging
import random
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import requests
import yaml

logger = logging.getLogger(__name__)


def load_config():
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


import re as _re

# --- Quick relevance filter (Tier 0) ---

# Must mention at least one of these to be considered relevant
_SHOPIFY_RE = _re.compile(r"\bshopify\b", _re.IGNORECASE)

_AD_SIGNAL_PATTERNS = [
    _re.compile(p, _re.IGNORECASE) for p in [
        r"\baudiences?\b",
        r"\bshop\s+campaigns?\b",
        r"\bcollabs?\b",
        r"\bretarget",
        r"\b(?:ads?|advertising)\b",
        r"\bmarketing\b",
        r"\bROAS\b",
        r"\bCAC\b",
        r"\bAOV\b",
        r"\bad\s*spend\b",
        r"\bconversion\s*rate\b",
        r"\bCPM\b",
        r"\bCPC\b",
        r"\bCPA\b",
    ]
]

_SUBSTANCE_PATTERNS = [
    _re.compile(p, _re.IGNORECASE) for p in [
        # Has opinion / experience
        r"\b(?:worth|waste|results?|performance|worked|didn'?t work|tried|tested|switched)\b",
        r"\b(?:recommend|review|experience|feedback|opinion)\b",
        # Has data
        r"\b\d+\.?\d*\s*[xX%]\b",          # "3.5x" or "200%"
        r"\$\s*\d+",                          # dollar amounts
        r"\b(?:increased?|decreased?|improved?|dropped?|grew|declined?)\b",
        # Has comparison
        r"\b(?:vs\.?|versus|compared|better|worse|instead)\b",
        # Has plan mention
        r"\b(?:basic|advanced|plus|shopify plan)\b",
    ]
]


def is_relevant_post(title, body):
    """Quick heuristic filter. Returns True if post is worth keeping.

    Rules:
    1. Combined text must be >= 50 chars (skip empty/tiny posts)
    2. Must mention "Shopify"
    3. Must have at least 1 ad-related signal
    4. Must have at least 1 substance signal (opinion/data/comparison)
    """
    text = f"{title or ''} {body or ''}"

    # Too short — no substance possible
    if len(text.strip()) < 50:
        return False

    # Must mention Shopify
    if not _SHOPIFY_RE.search(text):
        return False

    # Must have ad-related signal
    ad_hits = sum(1 for p in _AD_SIGNAL_PATTERNS if p.search(text))
    if ad_hits == 0:
        return False

    # Must have substance (opinion, data, or comparison)
    substance_hits = sum(1 for p in _SUBSTANCE_PATTERNS if p.search(text))
    if substance_hits == 0:
        return False

    return True


class RedditJsonScraper:
    """Scrapes Reddit using public .json endpoints (no API key needed)."""

    BASE_URL = "https://www.reddit.com"

    def __init__(self, config: dict = None):
        self.config = config or load_config()
        self.db_path = self.config["database"]["path"]
        reddit_cfg = self.config["reddit"]

        self.start_date = datetime.strptime(
            reddit_cfg["date_range"]["start"], "%Y-%m-%d"
        )
        self.end_date = datetime.strptime(
            reddit_cfg["date_range"]["end"], "%Y-%m-%d"
        )
        self.min_delay = self.config["rate_limits"]["min_delay_seconds"]
        self.max_delay = self.config["rate_limits"]["max_delay_seconds"]
        self.max_retries = self.config["rate_limits"]["max_retries"]

        # User agents for rotation
        self.user_agents = self.config.get("user_agents", [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        ])

        # Session for connection pooling
        self.session = requests.Session()

        # Flatten subreddit lists
        self.subreddits = []
        for group in reddit_cfg["subreddits"].values():
            self.subreddits.extend(group)

        # Flatten keyword lists
        self.keywords = []
        for group in reddit_cfg["keywords"].values():
            self.keywords.extend(group)

    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "application/json",
        }

    def _request_json(self, url: str, params: dict = None):
        """Make a request to Reddit .json endpoint with retries."""
        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(
                    url, params=params, headers=self._get_headers(), timeout=30
                )
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:
                    wait = (2 ** attempt) * 5 + random.uniform(1, 3)
                    logger.warning("Rate limited (429). Waiting %.1fs...", wait)
                    time.sleep(wait)
                elif resp.status_code == 403:
                    logger.warning("Forbidden (403) for %s — skipping", url)
                    return None
                else:
                    logger.warning(
                        "HTTP %d for %s (attempt %d/%d)",
                        resp.status_code, url, attempt + 1, self.max_retries,
                    )
                    time.sleep(2 ** attempt)
            except requests.RequestException as e:
                logger.warning("Request error: %s (attempt %d/%d)", e, attempt + 1, self.max_retries)
                time.sleep(2 ** attempt)

        return None

    def _search_subreddit(self, subreddit: str, query: str, after: str = None):
        """Search a subreddit. Returns (posts_data, next_after_token)."""
        url = f"{self.BASE_URL}/r/{subreddit}/search.json"
        params = {
            "q": query,
            "sort": "relevance",
            "t": "all",
            "limit": 100,
            "restrict_sr": "on",
        }
        if after:
            params["after"] = after

        data = self._request_json(url, params)
        if not data or "data" not in data:
            return [], None

        children = data["data"].get("children", [])
        next_after = data["data"].get("after")
        return children, next_after

    def _get_post_comments(self, permalink: str, score: int = 0) -> list:
        """Fetch comments for a post using its permalink.

        High-value posts (score >= 10): top 20 comments, depth 2
        Normal posts: top 5 comments, depth 1
        """
        if score >= 10:
            limit, depth = 20, 2
        else:
            limit, depth = 5, 1

        url = f"{self.BASE_URL}{permalink}.json"
        params = {"limit": limit, "depth": depth, "sort": "top"}
        data = self._request_json(url, params)

        if not data or not isinstance(data, list) or len(data) < 2:
            return []

        comments_data = data[1].get("data", {}).get("children", [])
        return comments_data

    def _extract_image_urls(self, post_data: dict) -> list:
        """Extract image URLs from post data."""
        urls = []
        post_url = post_data.get("url", "")

        # Direct image link
        if any(post_url.lower().endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp")):
            urls.append(post_url)

        # Reddit gallery
        if post_data.get("is_gallery") and "media_metadata" in post_data:
            for item in post_data["media_metadata"].values():
                if item.get("status") == "valid" and "s" in item:
                    img_url = item["s"].get("u") or item["s"].get("gif")
                    if img_url:
                        urls.append(img_url.replace("&amp;", "&"))

        # Reddit preview images
        if "preview" in post_data:
            images = post_data["preview"].get("images", [])
            for img in images:
                source = img.get("source", {}).get("url", "")
                if source:
                    urls.append(source.replace("&amp;", "&"))

        # imgur links
        if "imgur.com" in post_url:
            urls.append(post_url)

        return urls

    def _save_post(self, post_data: dict, keyword: str) -> bool:
        """Save a Reddit post to DB. Returns True if newly inserted."""
        created_utc = post_data.get("created_utc", 0)
        post_date = datetime.utcfromtimestamp(created_utc)
        if post_date < self.start_date or post_date > self.end_date:
            return False

        post_id = post_data.get("name", "").replace("t3_", "") or post_data.get("id", "")
        if not post_id:
            return False

        # Relevance filter — skip low-value posts
        title = post_data.get("title", "")
        body = post_data.get("selftext", "")
        if not is_relevant_post(title, body):
            logger.debug("Skipped irrelevant post: %s", title[:60])
            return False

        image_urls = self._extract_image_urls(post_data)
        permalink = post_data.get("permalink", "")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO reddit_posts
                (id, subreddit, title, body, author, author_flair, created_utc,
                 score, num_comments, url, permalink, search_keyword,
                 has_image, image_urls)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    post_id,
                    post_data.get("subreddit", ""),
                    post_data.get("title", ""),
                    post_data.get("selftext", ""),
                    post_data.get("author", "[deleted]"),
                    post_data.get("author_flair_text"),
                    post_date.strftime("%Y-%m-%d %H:%M:%S"),
                    post_data.get("score", 0),
                    post_data.get("num_comments", 0),
                    post_data.get("url", ""),
                    f"https://www.reddit.com{permalink}" if permalink else "",
                    keyword,
                    len(image_urls) > 0,
                    json.dumps(image_urls) if image_urls else None,
                ),
            )
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            logger.error("DB error saving post %s: %s", post_id, e)
            return False
        finally:
            conn.close()

    def _save_comments(self, post_id: str, comments_data: list) -> int:
        """Save comments recursively. Returns count saved."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        saved = 0

        def _save_comment(comment_data: dict, depth: int = 0):
            nonlocal saved
            if comment_data.get("kind") != "t1":
                return
            cdata = comment_data.get("data", {})
            body = cdata.get("body", "")
            if not body or body in ("[deleted]", "[removed]"):
                return

            comment_id = cdata.get("id", "")
            if not comment_id:
                return

            try:
                created = datetime.utcfromtimestamp(cdata.get("created_utc", 0))
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO reddit_comments
                    (id, post_id, body, author, author_flair, created_utc,
                     score, parent_id, depth)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        comment_id,
                        post_id,
                        body,
                        cdata.get("author", "[deleted]"),
                        cdata.get("author_flair_text"),
                        created.strftime("%Y-%m-%d %H:%M:%S"),
                        cdata.get("score", 0),
                        cdata.get("parent_id", ""),
                        depth,
                    ),
                )
                if cursor.rowcount > 0:
                    saved += 1
            except sqlite3.Error as e:
                logger.warning("DB error saving comment %s: %s", comment_id, e)

            # Recurse into replies (max depth 3)
            if depth < 3:
                replies = cdata.get("replies")
                if isinstance(replies, dict):
                    for reply in replies.get("data", {}).get("children", []):
                        _save_comment(reply, depth + 1)

        for comment in comments_data:
            _save_comment(comment)

        conn.commit()
        conn.close()
        return saved

    def scrape(self, progress_callback=None) -> int:
        """Run Reddit scraper across all subreddit/keyword combinations.

        Args:
            progress_callback: Optional callable(dict) for real-time progress updates.
                dict keys: step, total_steps, subreddit, keyword, posts_found,
                           comments_found, status, skipped
        """
        total_combos = len(self.subreddits) * len(self.keywords)
        logger.info(
            "Starting Reddit scrape (JSON mode): %d subreddits x %d keywords = %d combos",
            len(self.subreddits), len(self.keywords), total_combos,
        )
        total_posts = 0
        total_comments = 0
        total_skipped = 0
        step = 0

        # Load already-scraped post IDs
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM reddit_posts")
        seen_ids = {row[0] for row in cursor.fetchall()}
        conn.close()
        logger.info("Already have %d posts in database", len(seen_ids))

        def _notify(status="searching", **extra):
            if progress_callback:
                progress_callback({
                    "step": step,
                    "total_steps": total_combos,
                    "subreddit": sub_name,
                    "keyword": keyword,
                    "posts_found": total_posts,
                    "comments_found": total_comments,
                    "skipped": total_skipped,
                    "status": status,
                    **extra,
                })

        for sub_name in self.subreddits:
            for keyword in self.keywords:
                step += 1
                logger.info("Searching r/%s for '%s' (%d/%d)", sub_name, keyword, step, total_combos)
                _notify("searching")

                after = None
                batch_new = 0
                pages = 0
                max_pages = 10  # Safety limit

                while pages < max_pages:
                    children, next_after = self._search_subreddit(sub_name, keyword, after)
                    if not children:
                        break

                    for child in children:
                        post_data = child.get("data", {})
                        post_id = post_data.get("id", "")
                        if post_id in seen_ids:
                            continue

                        if self._save_post(post_data, keyword):
                            seen_ids.add(post_id)
                            total_posts += 1
                            batch_new += 1
                            _notify("fetching_comments")

                            # Fetch comments — tiered by post score
                            permalink = post_data.get("permalink", "")
                            post_score = post_data.get("score", 0)
                            if permalink:
                                time.sleep(random.uniform(0.5, 1.5))
                                comments = self._get_post_comments(permalink, score=post_score)
                                comment_count = self._save_comments(post_id, comments)
                                total_comments += comment_count
                        else:
                            if post_id not in seen_ids:
                                total_skipped += 1

                    pages += 1

                    if not next_after:
                        break
                    after = next_after
                    # Delay between pagination
                    time.sleep(random.uniform(self.min_delay, self.max_delay))

                if batch_new > 0:
                    logger.info("  r/%s + '%s': %d new posts", sub_name, keyword, batch_new)

                _notify("done_combo", batch_new=batch_new)
                # Delay between searches
                time.sleep(random.uniform(self.min_delay, self.max_delay))

        logger.info(
            "Reddit scrape complete. New posts: %d, new comments: %d, skipped: %d",
            total_posts, total_comments, total_skipped,
        )
        _notify("complete")
        return total_posts


def run(config: dict = None) -> int:
    """Entry point for Reddit scraping."""
    scraper = RedditJsonScraper(config)
    count = scraper.scrape()
    if count == 0:
        logger.warning("Reddit scraping yielded 0 results.")
    return count


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    run()

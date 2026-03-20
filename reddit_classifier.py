"""Two-tier classification for Reddit Shopify ad product feedback.

Tier 1: Regex-based quick classification (ad product detection + sentiment keywords)
Tier 2: Claude API structured extraction (metrics, plan, category, duration)
"""

import json
import logging
import os
import re
import sqlite3
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# ---------- Tier 1: Regex patterns ----------

AD_PRODUCT_PATTERNS = {
    "shopify_audiences": [
        re.compile(r"\bshopify\s+audiences?\b", re.IGNORECASE),
    ],
    "shop_campaigns": [
        re.compile(r"\bshop\s+campaigns?\b", re.IGNORECASE),
    ],
    "shopify_collabs": [
        re.compile(r"\bshopify\s+collabs?\b", re.IGNORECASE),
    ],
    "shop_app_ads": [
        re.compile(r"\bshop\s+app\s+ads?\b", re.IGNORECASE),
    ],
    "shopify_retargeting": [
        re.compile(r"\bshopify\s+retarget", re.IGNORECASE),
    ],
    "shopify_ads_generic": [
        re.compile(r"\bshopify\b.*\b(?:ads?|advertising)\b", re.IGNORECASE),
    ],
    "shopify_marketing": [
        re.compile(r"\bshopify\s+marketing\b", re.IGNORECASE),
    ],
}

SENTIMENT_POSITIVE = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bworth it\b", r"\bgame.?changer\b", r"\bamazing results?\b",
        r"\bincreased?\s+ROAS\b", r"\bhighly recommend\b", r"\bgreat results?\b",
        r"\bimpressive\b", r"\bbest\s+(?:thing|feature|tool)\b",
        r"\bsaved?\s+(?:me|us)\b", r"\blove\s+(?:it|this)\b",
    ]
]

SENTIMENT_NEGATIVE = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bwaste\s+of\s+money\b", r"\bterrible\b", r"\bno\s+results?\b",
        r"\bswitched?\s+(?:away|from)\b", r"\bdisappoint", r"\bnot?\s+worth\b",
        r"\bhorrible\b", r"\bscam\b", r"\buseless\b", r"\bburning?\s+money\b",
        r"\bdon'?t\s+(?:use|bother|waste)\b",
    ]
]

METRIC_PATTERNS = {
    "roas": re.compile(
        r"ROAS\s*(?:of|is|was|=|:|around|~|about)?\s*(\d+\.?\d*)\s*x?",
        re.IGNORECASE,
    ),
    "aov": re.compile(
        r"AOV\s*(?:of|is|was|=|:|around|~|about)?\s*\$?\s*(\d+\.?\d*)",
        re.IGNORECASE,
    ),
    "cac": re.compile(
        r"CAC\s*(?:of|is|was|=|:|around|~|about)?\s*\$?\s*(\d+\.?\d*)",
        re.IGNORECASE,
    ),
    "cps": re.compile(
        r"(?:CPS|cost\s+per\s+sale)\s*(?:of|is|was|=|:|around|~|about)?\s*\$?\s*(\d+\.?\d*)",
        re.IGNORECASE,
    ),
}


def classify_text_tier1(text: str) -> dict:
    """Tier 1: regex-based quick classification.
    Returns dict with ad_products, sentiment_signals, metrics_detected."""
    if not text:
        return {"ad_products": [], "sentiment_signals": [], "metrics": {}}

    # Detect ad products
    ad_products = []
    for product, patterns in AD_PRODUCT_PATTERNS.items():
        for pat in patterns:
            if pat.search(text):
                ad_products.append(product)
                break

    # Sentiment signals
    pos = sum(1 for p in SENTIMENT_POSITIVE if p.search(text))
    neg = sum(1 for p in SENTIMENT_NEGATIVE if p.search(text))
    sentiment_signals = []
    if pos > 0:
        sentiment_signals.append(f"positive({pos})")
    if neg > 0:
        sentiment_signals.append(f"negative({neg})")

    # Metric extraction
    metrics = {}
    for metric_name, pattern in METRIC_PATTERNS.items():
        match = pattern.search(text)
        if match:
            try:
                metrics[metric_name] = float(match.group(1))
            except ValueError:
                pass

    return {
        "ad_products": ad_products,
        "sentiment_signals": sentiment_signals,
        "metrics": metrics,
    }


def run_regex_classification(config: dict = None):
    """Run Tier 1 regex classification on all Reddit posts and comments."""
    if config is None:
        config_path = Path(__file__).parent / "config.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

    db_path = config["database"]["path"]
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Classify posts
    cursor.execute("SELECT id, title, body FROM reddit_posts")
    rows = cursor.fetchall()
    updated = 0
    for row_id, title, body in rows:
        text = f"{title or ''} {body or ''}"
        result = classify_text_tier1(text)
        themes = {
            "ad_products": result["ad_products"],
            "sentiment_signals": result["sentiment_signals"],
            "metrics": result["metrics"],
        }
        cursor.execute(
            "UPDATE reddit_posts SET themes = ? WHERE id = ?",
            (json.dumps(themes), row_id),
        )
        updated += 1

    conn.commit()
    logger.info("Tier 1 classified %d Reddit posts", updated)

    # Classify comments
    cursor.execute("SELECT id, body FROM reddit_comments")
    rows = cursor.fetchall()
    updated = 0
    for row_id, body in rows:
        result = classify_text_tier1(body or "")
        themes = {
            "ad_products": result["ad_products"],
            "sentiment_signals": result["sentiment_signals"],
            "metrics": result["metrics"],
        }
        cursor.execute(
            "UPDATE reddit_comments SET themes = ? WHERE id = ?",
            (json.dumps(themes), row_id),
        )
        updated += 1

    conn.commit()
    conn.close()
    logger.info("Tier 1 classified %d Reddit comments", updated)


# ---------- Tier 2: Claude API extraction ----------

EXTRACTION_PROMPT = """You are analyzing a Reddit post/comment from a Shopify merchant about Shopify's advertising products.

First, judge the RELEVANCE of this text, then extract structured data. Return ONLY valid JSON.

Text to analyze:
---
{text}
---

Return this exact JSON structure (use null for unknown/not mentioned fields):
{{
  "relevance_score": 1-5 integer,
  "relevance_reason": "one-line explanation",
  "sentiment": "positive" | "negative" | "mixed" | "neutral",
  "sentiment_reason": "brief explanation of why the merchant feels this way",
  "shopify_plan": "Basic" | "Shopify" | "Advanced" | "Plus" | null,
  "product_category": "what the merchant sells, e.g. fashion, electronics, pet, beauty, supplements, home goods, etc." | null,
  "usage_duration": "how long they've used the ad product, e.g. 2 months, 1 year" | null,
  "ad_products_mentioned": ["list of specific Shopify ad products mentioned"],
  "roas": number or null,
  "aov": number or null,
  "cac": number or null,
  "cps": number or null,
  "ad_spend_monthly": number or null,
  "has_results_data": true if the post contains specific performance numbers or screenshots,
  "results_summary": "brief summary of the results/outcomes mentioned" | null,
  "is_shopify_native_ad": true if this is about Shopify's OWN ad products (Audiences, Shop Campaigns, Collabs, Shop app) rather than running Facebook/Google ads from a Shopify store
}}

RELEVANCE SCORING (1-5):
  5 = First-hand experience with specific Shopify ad product, includes data/metrics
  4 = First-hand experience with Shopify ad product, qualitative review with substance
  3 = Discussion of Shopify ad products with some useful context but no personal data
  2 = Tangential mention of Shopify ads, question without answers, or vague reference
  1 = Not about Shopify ad products at all, spam, or zero informational value

IMPORTANT: Distinguish between:
- Shopify's native ad products (Shopify Audiences, Shop Campaigns, Collabs, Shop app ads) → is_shopify_native_ad = true
- Running Facebook/Google/TikTok ads for a Shopify store → is_shopify_native_ad = false
Only extract metrics and details if is_shopify_native_ad is true.
If relevance_score <= 2, you can skip detailed extraction (set other fields to null)."""


def run_llm_extraction(config: dict = None, progress_callback=None):
    """Run Tier 2 LLM extraction on posts that mention Shopify ad products."""
    try:
        from openai import OpenAI
    except ImportError:
        logger.error("openai package not installed. Run: pip install openai")
        return

    if config is None:
        config_path = Path(__file__).parent / "config.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

    # LLM config (DeepSeek via OpenAI-compatible API)
    llm_cfg = config["reddit"].get("llm", {})
    api_key = llm_cfg.get("api_key") or os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        logger.error("No DeepSeek API key. Set in config.yaml reddit.llm.api_key.")
        return

    base_url = llm_cfg.get("base_url", "https://api.deepseek.com")
    model = llm_cfg.get("model", "deepseek-chat")
    client = OpenAI(api_key=api_key, base_url=base_url)

    db_path = config["database"]["path"]
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Find posts with ad product mentions that haven't been extracted yet
    cursor.execute("""
        SELECT p.id, p.title, p.body, 'post' as source_type
        FROM reddit_posts p
        LEFT JOIN reddit_extracted e ON e.source_id = p.id AND e.source_type = 'post'
        WHERE p.themes IS NOT NULL
          AND p.themes LIKE '%ad_products%'
          AND p.themes NOT LIKE '%"ad_products": []%'
          AND e.id IS NULL
    """)
    post_rows = cursor.fetchall()

    # Also find comments with ad product mentions
    cursor.execute("""
        SELECT c.id, '', c.body, 'comment' as source_type
        FROM reddit_comments c
        LEFT JOIN reddit_extracted e ON e.source_id = c.id AND e.source_type = 'comment'
        WHERE c.themes IS NOT NULL
          AND c.themes LIKE '%ad_products%'
          AND c.themes NOT LIKE '%"ad_products": []%'
          AND e.id IS NULL
    """)
    comment_rows = cursor.fetchall()

    all_rows = post_rows + comment_rows
    total = len(all_rows)
    logger.info("Tier 2 LLM extraction: %d items to process", total)

    extracted = 0
    skipped = 0
    for idx, (source_id, title, body, source_type) in enumerate(all_rows):
        text = f"{title}\n\n{body}" if title else (body or "")
        if not text.strip():
            continue

        # Truncate very long texts
        if len(text) > 3000:
            text = text[:3000] + "..."

        try:
            response = client.chat.completions.create(
                model=model,
                max_tokens=1024,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data extraction assistant. Always respond with valid JSON only.",
                    },
                    {
                        "role": "user",
                        "content": EXTRACTION_PROMPT.format(text=text),
                    },
                ],
            )
            result_text = response.choices[0].message.content.strip()

            # Parse JSON from response (handle markdown code blocks)
            if result_text.startswith("```"):
                result_text = re.sub(r"^```\w*\n?", "", result_text)
                result_text = re.sub(r"\n?```$", "", result_text)

            data = json.loads(result_text)

            # Skip if not about Shopify native ads
            if not data.get("is_shopify_native_ad", True):
                logger.debug("Skipping %s %s: not Shopify native ad", source_type, source_id)
                # Still save to avoid re-processing
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO reddit_extracted
                    (source_id, source_type, sentiment, has_results_data)
                    VALUES (?, ?, 'not_applicable', 0)
                    """,
                    (source_id, source_type),
                )
                conn.commit()
                continue

            cursor.execute(
                """
                INSERT OR REPLACE INTO reddit_extracted
                (source_id, source_type, relevance_score, relevance_reason,
                 sentiment, sentiment_reason, shopify_plan,
                 product_category, usage_duration, ad_products_mentioned,
                 roas, aov, cac, cps, ad_spend_monthly,
                 has_results_data, results_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    source_type,
                    data.get("relevance_score"),
                    data.get("relevance_reason"),
                    data.get("sentiment"),
                    data.get("sentiment_reason"),
                    data.get("shopify_plan"),
                    data.get("product_category"),
                    data.get("usage_duration"),
                    json.dumps(data.get("ad_products_mentioned", [])),
                    data.get("roas"),
                    data.get("aov"),
                    data.get("cac"),
                    data.get("cps"),
                    data.get("ad_spend_monthly"),
                    data.get("has_results_data", False),
                    data.get("results_summary"),
                ),
            )
            conn.commit()
            extracted += 1
            logger.info(
                "Extracted %s %s: sentiment=%s, products=%s",
                source_type, source_id,
                data.get("sentiment"),
                data.get("ad_products_mentioned"),
            )

        except json.JSONDecodeError as e:
            logger.warning("Failed to parse LLM response for %s %s: %s", source_type, source_id, e)
            skipped += 1
        except Exception as e:
            logger.warning("LLM extraction failed for %s %s: %s", source_type, source_id, e)
            skipped += 1

        # Progress callback
        if progress_callback:
            progress_callback({
                "current": idx + 1,
                "total": total,
                "extracted": extracted,
                "skipped": skipped,
                "source_type": source_type,
            })

    conn.close()
    logger.info("Tier 2 LLM extraction complete. Extracted: %d, Skipped: %d", extracted, skipped)


def run(config: dict = None):
    """Run both tiers of classification."""
    run_regex_classification(config)
    run_llm_extraction(config)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    run()

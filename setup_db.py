"""Database setup and org_events population."""

import sqlite3
import logging
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)


def load_config():
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def setup_database(db_path: str = None):
    """Create all tables and populate org_events."""
    if db_path is None:
        config = load_config()
        db_path = config["database"]["path"]

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Glassdoor reviews table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS glassdoor_reviews (
            id TEXT PRIMARY KEY,
            date DATE NOT NULL,
            rating_overall INTEGER,
            rating_culture INTEGER,
            rating_worklife_balance INTEGER,
            rating_senior_management INTEGER,
            rating_compensation INTEGER,
            rating_career_opportunities INTEGER,
            recommend_to_friend BOOLEAN,
            ceo_approval TEXT,
            business_outlook TEXT,
            title TEXT,
            pros TEXT,
            cons TEXT,
            advice_to_management TEXT,
            reviewer_role TEXT,
            reviewer_location TEXT,
            reviewer_employment_status TEXT,
            reviewer_tenure TEXT,
            themes TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Blind posts table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS blind_posts (
            id TEXT PRIMARY KEY,
            date DATE,
            title TEXT,
            body TEXT,
            company_tag TEXT,
            topic_tags TEXT,
            upvotes INTEGER,
            comments_count INTEGER,
            comments TEXT,
            themes TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # LinkedIn departures table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS linkedin_departures (
            id TEXT PRIMARY KEY,
            date DATE,
            author_name TEXT,
            author_title TEXT,
            author_tenure_years REAL,
            post_text TEXT,
            reactions_count INTEGER,
            comments_count INTEGER,
            post_url TEXT,
            themes TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Reddit posts table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reddit_posts (
            id TEXT PRIMARY KEY,
            subreddit TEXT NOT NULL,
            title TEXT,
            body TEXT,
            author TEXT,
            author_flair TEXT,
            created_utc TIMESTAMP,
            score INTEGER,
            num_comments INTEGER,
            url TEXT,
            permalink TEXT,
            search_keyword TEXT,
            has_image BOOLEAN DEFAULT 0,
            image_urls TEXT,
            screenshot_path TEXT,
            themes TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Reddit comments table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reddit_comments (
            id TEXT PRIMARY KEY,
            post_id TEXT NOT NULL,
            body TEXT,
            author TEXT,
            author_flair TEXT,
            created_utc TIMESTAMP,
            score INTEGER,
            parent_id TEXT,
            depth INTEGER,
            themes TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (post_id) REFERENCES reddit_posts(id)
        )
    """)

    # Reddit extracted structured data (LLM-parsed)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reddit_extracted (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            source_type TEXT NOT NULL,
            relevance_score INTEGER,
            relevance_reason TEXT,
            sentiment TEXT,
            sentiment_reason TEXT,
            shopify_plan TEXT,
            product_category TEXT,
            usage_duration TEXT,
            ad_products_mentioned TEXT,
            roas REAL,
            aov REAL,
            cac REAL,
            cps REAL,
            ad_spend_monthly REAL,
            has_results_data BOOLEAN,
            results_summary TEXT,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_id, source_type)
        )
    """)

    # Org events reference table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS org_events (
            date DATE NOT NULL,
            event_type TEXT NOT NULL,
            description TEXT NOT NULL,
            people_involved TEXT,
            significance TEXT
        )
    """)

    conn.commit()

    # Populate org_events if empty
    cursor.execute("SELECT COUNT(*) FROM org_events")
    if cursor.fetchone()[0] == 0:
        _populate_org_events(cursor)
        conn.commit()
        logger.info("Populated org_events table with %d events", cursor.rowcount)

    conn.close()
    logger.info("Database setup complete: %s", db_path)


def _populate_org_events(cursor):
    """Pre-populate org_events with known Shopify organizational events."""
    events = [
        ("2023-05-04", "layoff", "Layoff: 20% workforce (~2000 people)", "Tobi announcement", "high"),
        ("2023-06-01", "policy", 'Policy: "Chaos monkey" meetings elimination', "Tobi", "medium"),
        ("2024-07-01", "hire", "Board: Microsoft CTO joins board", "Kevin Scott", "medium"),
        ("2024-08-01", "hire", "Hire: CTO from Microsoft", "Mikhail Parakhin", "high"),
        ("2025-06-01", "layoff", "Layoff: Sales team fired (fraud)", "Bobby Morrison Slack msg", "high"),
        ("2025-07-01", "departure", "Departure: 4 VPs leave (summer wave)", "Mangini, Burrows, Coates, Podduturi", "high"),
        ("2025-09-12", "departure", "Departure: COO leaves for Opendoor", "Kaz Nejatian", "high"),
        ("2025-10-09", "reorg", "Hire/Departure: GC->COO, CRO+2 VPs out", "Hertz promoted; Morrison, Subramanian, Longfield out", "high"),
        ("2025-10-15", "departure", "Departure: VP Ops poached by Nejatian", "Giang LeGrice", "medium"),
        ("2025-11-27", "layoff", 'Layoff: "removed layers that created complexity"', "BetaKit report", "high"),
        ("2025-12-02", "policy", "Policy: Sales compensation overhaul", "Hertz announcement", "medium"),
    ]

    cursor.executemany(
        "INSERT INTO org_events (date, event_type, description, people_involved, significance) VALUES (?, ?, ?, ?, ?)",
        events,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    setup_database()
    print("Database setup complete.")

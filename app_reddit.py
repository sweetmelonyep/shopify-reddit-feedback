"""Streamlit app for Shopify merchant Reddit feedback on ad products.

Workflow: Scrape → Classify → Explore data
"""

import json
import logging
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

# --- Config & DB helpers ---

PROJECT_DIR = Path(__file__).parent
CONFIG_PATH = PROJECT_DIR / "config.yaml"


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def get_db_path():
    return str(PROJECT_DIR / load_config()["database"]["path"])


def query_db(sql, params=()):
    conn = sqlite3.connect(get_db_path())
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


def db_count(table):
    conn = sqlite3.connect(get_db_path())
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        n = cur.fetchone()[0]
    except sqlite3.OperationalError:
        n = 0
    conn.close()
    return n


# --- Scrape state (shared across reruns via session_state) ---

if "scrape_running" not in st.session_state:
    st.session_state.scrape_running = False
if "scrape_done" not in st.session_state:
    st.session_state.scrape_done = False
if "scrape_result" not in st.session_state:
    st.session_state.scrape_result = None
if "scrape_log" not in st.session_state:
    st.session_state.scrape_log = []
if "scrape_progress" not in st.session_state:
    st.session_state.scrape_progress = ""
if "scrape_progress_data" not in st.session_state:
    st.session_state.scrape_progress_data = {}


def _scrape_worker(config):
    """Background scraping thread."""
    import io

    # Capture logs
    log_list = st.session_state.scrape_log
    progress_ref = st.session_state

    class ListHandler(logging.Handler):
        def emit(self, record):
            msg = self.format(record)
            log_list.append(msg)
            # Update progress text with latest message
            progress_ref.scrape_progress = msg

    handler = ListHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%H:%M:%S"))

    scrape_logger = logging.getLogger("scraper_reddit")
    scrape_logger.addHandler(handler)
    scrape_logger.setLevel(logging.INFO)

    def _progress_cb(data):
        st.session_state.scrape_progress_data = data

    try:
        import scraper_reddit
        # Force reimport to pick up any code changes
        import importlib
        importlib.reload(scraper_reddit)
        scraper = scraper_reddit.RedditJsonScraper(config)
        count = scraper.scrape(progress_callback=_progress_cb)
        st.session_state.scrape_result = count
    except Exception as e:
        st.session_state.scrape_result = f"Error: {e}"
        log_list.append(f"ERROR: {e}")
    finally:
        scrape_logger.removeHandler(handler)
        st.session_state.scrape_running = False
        st.session_state.scrape_done = True


# --- Page config ---
st.set_page_config(
    page_title="Shopify Ad Feedback — Reddit",
    page_icon="🛒",
    layout="wide",
)

st.title("🛒 Shopify Ad Products — Reddit Merchant Feedback")

# --- Detect cloud (read-only) environment ---
import os
IS_CLOUD = os.environ.get("STREAMLIT_SHARING_MODE") or os.path.exists("/mount/src")

# --- Sidebar ---
with st.sidebar:
    st.header("Controls")
    if IS_CLOUD:
        pages = ["📊 Dashboard", "📋 Browse Posts", "📈 Extracted Data"]
    else:
        pages = ["📊 Dashboard", "🔍 Scrape", "🏷️ Classify", "📋 Browse Posts", "📈 Extracted Data"]
    page = st.radio("Navigate", pages)

    st.divider()

    # Live DB stats (no cache - always fresh)
    n_posts = db_count("reddit_posts")
    n_comments = db_count("reddit_comments")
    n_extracted = db_count("reddit_extracted")

    st.caption("Database Stats")
    col1, col2 = st.columns(2)
    col1.metric("Posts", n_posts)
    col2.metric("Comments", n_comments)
    st.metric("Extracted", n_extracted)

    # Show scraping status indicator
    if st.session_state.scrape_running:
        st.markdown("---")
        st.markdown("🔴 **Scraping in progress...**")
        st.markdown(f"```\n{st.session_state.scrape_progress}\n```")

    # Refresh button
    if st.button("🔄 Refresh"):
        st.rerun()


# ========== Dashboard ==========
if page == "📊 Dashboard":
    st.header("Dashboard")

    posts_df = query_db("SELECT * FROM reddit_posts ORDER BY created_utc DESC")
    if posts_df.empty:
        st.info("No posts yet. Go to **Scrape** to collect Reddit data.")
        st.stop()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Posts", len(posts_df))
    col2.metric("Subreddits", posts_df["subreddit"].nunique())
    col3.metric("With Images", int(posts_df["has_image"].sum()))

    extracted_df = query_db(
        "SELECT * FROM reddit_extracted WHERE sentiment != 'not_applicable'"
    )
    col4.metric("Extracted (native ads)", len(extracted_df))

    st.subheader("Posts Over Time")
    posts_df["date"] = pd.to_datetime(posts_df["created_utc"]).dt.to_period("M").astype(str)
    time_series = posts_df.groupby("date").size().reset_index(name="count")
    st.bar_chart(time_series.set_index("date"))

    st.subheader("By Subreddit")
    sub_counts = posts_df["subreddit"].value_counts().head(15)
    st.bar_chart(sub_counts)

    if not extracted_df.empty:
        st.subheader("Sentiment Distribution (Shopify Native Ads)")
        sentiment_counts = extracted_df["sentiment"].value_counts()
        st.bar_chart(sentiment_counts)

        st.subheader("Ad Products Mentioned")
        products = []
        for row in extracted_df["ad_products_mentioned"].dropna():
            try:
                prods = json.loads(row)
                products.extend(prods)
            except (json.JSONDecodeError, TypeError):
                pass
        if products:
            prod_series = pd.Series(products).value_counts()
            st.bar_chart(prod_series)

        roas_data = extracted_df[extracted_df["roas"].notna()]
        if not roas_data.empty:
            st.subheader("ROAS Distribution")
            st.bar_chart(roas_data["roas"].value_counts().sort_index())


# ========== Scrape ==========
elif page == "🔍 Scrape":
    st.header("Scrape Reddit")
    st.markdown("Uses Reddit's public `.json` endpoints — **no API key needed**.")

    config = load_config()
    reddit_cfg = config["reddit"]

    with st.expander("Subreddits & Keywords", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Subreddits**")
            for group_name, subs in reddit_cfg["subreddits"].items():
                st.markdown(f"*{group_name}*: {', '.join(f'r/{s}' for s in subs)}")
        with col2:
            st.markdown("**Keywords**")
            for group_name, kws in reddit_cfg["keywords"].items():
                st.markdown(f"*{group_name}*:")
                for kw in kws:
                    st.markdown(f"  - `{kw}`")

    # --- Scrape controls ---
    if st.session_state.scrape_running:
        # Show live progress
        prog = st.session_state.scrape_progress_data
        step = prog.get("step", 0)
        total = prog.get("total_steps", 1) or 1
        pct = step / total

        st.warning(f"⏳ Scraping in progress... ({step}/{total} search combos)")
        st.progress(pct)

        if prog.get("subreddit"):
            st.markdown(
                f"**Current:** r/{prog['subreddit']} — `{prog.get('keyword', '')}`"
            )

        # Live stats — 4 columns
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("✅ Posts saved", prog.get("posts_found", db_count("reddit_posts")))
        col2.metric("💬 Comments", prog.get("comments_found", db_count("reddit_comments")))
        col3.metric("🚫 Skipped (low value)", prog.get("skipped", 0))
        col4.metric("📊 Progress", f"{pct:.0%}")

        # Show latest log lines
        st.subheader("Live Log")
        log_lines = st.session_state.scrape_log[-20:]
        if log_lines:
            st.code("\n".join(log_lines), language="text")
        else:
            st.caption("Waiting for log output...")

        # Auto-refresh every 3 seconds
        time.sleep(3)
        st.rerun()

    elif st.session_state.scrape_done:
        # Show results
        result = st.session_state.scrape_result
        if isinstance(result, int):
            st.success(f"✅ Scraping complete! **{result}** new posts collected.")
        else:
            st.error(f"Scraping failed: {result}")

        # Show full log
        if st.session_state.scrape_log:
            with st.expander("Full Scrape Log", expanded=False):
                st.code("\n".join(st.session_state.scrape_log), language="text")

        # Reset button
        if st.button("🔄 Reset & Scrape Again"):
            st.session_state.scrape_done = False
            st.session_state.scrape_result = None
            st.session_state.scrape_log = []
            st.session_state.scrape_progress = ""
            st.rerun()

    else:
        # Start button
        if st.button("🚀 Start Scraping", type="primary"):
            st.session_state.scrape_running = True
            st.session_state.scrape_done = False
            st.session_state.scrape_result = None
            st.session_state.scrape_log = []
            st.session_state.scrape_progress = ""

            # Launch in background thread
            thread = threading.Thread(
                target=_scrape_worker, args=(config,), daemon=True
            )
            thread.start()
            st.rerun()


# ========== Classify ==========
elif page == "🏷️ Classify":
    st.header("Classify & Extract")

    n_posts = db_count("reddit_posts")
    n_extracted = db_count("reddit_extracted")

    if n_posts == 0:
        st.warning("No posts to classify. Scrape first.")
        st.stop()

    st.markdown(f"**{n_posts}** posts in DB, **{n_extracted}** already extracted.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Tier 1: Regex Classification")
        st.markdown("Fast regex-based detection of ad products, sentiment, and metrics.")
        if st.button("Run Regex Classification"):
            with st.spinner("Running regex classification..."):
                import reddit_classifier
                config = load_config()
                reddit_classifier.run_regex_classification(config)
            st.success("Regex classification complete!")
            st.rerun()

    with col2:
        st.subheader("Tier 2: LLM Extraction")
        st.markdown("Uses **DeepSeek V3** to extract structured data (sentiment, plan, ROAS, etc.).")

        config = load_config()
        llm_cfg = config.get("reddit", {}).get("llm", {})
        if llm_cfg.get("api_key"):
            st.caption(f"Model: `{llm_cfg.get('model', 'deepseek-chat')}`")
        else:
            st.warning("No API key configured in config.yaml → reddit.llm.api_key")

        if st.button("Run LLM Extraction"):
            import reddit_classifier
            import importlib
            importlib.reload(reddit_classifier)
            config = load_config()

            progress_bar = st.progress(0)
            status_text = st.empty()
            stats_cols = st.columns(3)

            def _llm_progress(data):
                pct = data["current"] / max(data["total"], 1)
                progress_bar.progress(pct)
                status_text.markdown(
                    f"Processing {data['current']}/{data['total']} — "
                    f"✅ Extracted: {data['extracted']} | 🚫 Skipped: {data['skipped']}"
                )

            with st.spinner("Running LLM extraction..."):
                reddit_classifier.run_llm_extraction(config, progress_callback=_llm_progress)
            st.success("LLM extraction complete!")
            st.rerun()


# ========== Browse Posts ==========
elif page == "📋 Browse Posts":
    st.header("Browse Posts")

    posts_df = query_db(
        "SELECT id, subreddit, title, body, author, created_utc, score, "
        "num_comments, permalink, has_image, image_urls, themes, search_keyword "
        "FROM reddit_posts ORDER BY created_utc DESC"
    )
    if posts_df.empty:
        st.info("No posts yet.")
        st.stop()

    col1, col2, col3 = st.columns(3)
    with col1:
        subs = ["All"] + sorted(posts_df["subreddit"].unique().tolist())
        selected_sub = st.selectbox("Subreddit", subs)
    with col2:
        search = st.text_input("Search title/body", "")
    with col3:
        only_images = st.checkbox("Only posts with images")

    filtered = posts_df.copy()
    if selected_sub != "All":
        filtered = filtered[filtered["subreddit"] == selected_sub]
    if search:
        mask = (
            filtered["title"].str.contains(search, case=False, na=False)
            | filtered["body"].str.contains(search, case=False, na=False)
        )
        filtered = filtered[mask]
    if only_images:
        filtered = filtered[filtered["has_image"] == 1]

    st.caption(f"Showing {len(filtered)} of {len(posts_df)} posts")

    for _, row in filtered.head(50).iterrows():
        with st.expander(
            f"[r/{row['subreddit']}] {row['title'][:100]} "
            f"(⬆{row['score']} 💬{row['num_comments']} "
            f"📅{str(row['created_utc'])[:10]})"
        ):
            st.markdown(f"**Author:** {row['author']} | **Keyword:** `{row['search_keyword']}`")
            if row["permalink"]:
                st.markdown(f"[Open on Reddit]({row['permalink']})")

            body = row["body"] or ""
            if len(body) > 2000:
                body = body[:2000] + "..."
            if body:
                # Escape $ to prevent LaTeX rendering
                st.markdown(body.replace("$", "\\$"))

            if row["has_image"] and row["image_urls"]:
                try:
                    urls = json.loads(row["image_urls"])
                    for url in urls[:5]:
                        st.image(url, use_container_width=True)
                except (json.JSONDecodeError, TypeError):
                    pass

            if row["themes"]:
                try:
                    themes = json.loads(row["themes"])
                    if themes.get("ad_products"):
                        st.markdown(f"**Ad Products:** {', '.join(themes['ad_products'])}")
                    if themes.get("sentiment_signals"):
                        st.markdown(f"**Sentiment:** {', '.join(themes['sentiment_signals'])}")
                    if themes.get("metrics"):
                        st.markdown(f"**Metrics:** {themes['metrics']}")
                except (json.JSONDecodeError, TypeError):
                    pass

            comments_df = query_db(
                "SELECT author, body, score, created_utc, depth "
                "FROM reddit_comments WHERE post_id = ? ORDER BY score DESC LIMIT 10",
                (row["id"],),
            )
            if not comments_df.empty:
                st.markdown("---")
                st.markdown(f"**Top Comments ({len(comments_df)})**")
                for _, c in comments_df.iterrows():
                    indent = "&nbsp;" * 4 * (c["depth"] or 0)
                    comment_body = c["body"].replace("$", "\\$")
                    if len(comment_body) > 500:
                        comment_body = comment_body[:500] + "..."
                    st.markdown(
                        f"{indent}**{c['author']}** (⬆{c['score']}): {comment_body}"
                    )


# ========== Extracted Data ==========
elif page == "📈 Extracted Data":
    st.header("Extracted Structured Data")

    extracted_df = query_db("""
        SELECT e.*, p.title as post_title, p.permalink, p.subreddit
        FROM reddit_extracted e
        LEFT JOIN reddit_posts p ON e.source_id = p.id AND e.source_type = 'post'
        WHERE e.sentiment != 'not_applicable'
        ORDER BY e.extracted_at DESC
    """)

    if extracted_df.empty:
        st.info("No extracted data yet. Run **Classify** first.")
        st.stop()

    col1, col2 = st.columns(2)
    with col1:
        sentiments = ["All"] + sorted(extracted_df["sentiment"].dropna().unique().tolist())
        sel_sentiment = st.selectbox("Sentiment", sentiments)
    with col2:
        has_data = st.checkbox("Only with performance data", value=False)

    filtered = extracted_df.copy()
    if sel_sentiment != "All":
        filtered = filtered[filtered["sentiment"] == sel_sentiment]
    if has_data:
        filtered = filtered[filtered["has_results_data"] == 1]

    st.caption(f"Showing {len(filtered)} entries")

    display_cols = [
        "post_title", "subreddit", "sentiment", "ad_products_mentioned",
        "shopify_plan", "product_category", "usage_duration",
        "roas", "aov", "cac", "cps", "ad_spend_monthly",
        "results_summary",
    ]
    available_cols = [c for c in display_cols if c in filtered.columns]
    st.dataframe(
        filtered[available_cols],
        use_container_width=True,
        hide_index=True,
    )

    if st.button("📥 Export to CSV"):
        csv = filtered.to_csv(index=False)
        st.download_button(
            "Download CSV",
            csv,
            file_name="shopify_reddit_extracted.csv",
            mime="text/csv",
        )

    st.divider()
    st.subheader("Detail View")
    for _, row in filtered.head(30).iterrows():
        title = row.get("post_title", row.get("source_id", "Unknown"))
        with st.expander(f"{title[:80]} — {row['sentiment']}"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Sentiment:** {row['sentiment']}")
                st.markdown(f"**Reason:** {row.get('sentiment_reason', 'N/A')}")
                st.markdown(f"**Shopify Plan:** {row.get('shopify_plan', 'N/A')}")
                st.markdown(f"**Product Category:** {row.get('product_category', 'N/A')}")
                st.markdown(f"**Usage Duration:** {row.get('usage_duration', 'N/A')}")
            with col2:
                st.markdown(f"**Ad Products:** {row.get('ad_products_mentioned', 'N/A')}")
                st.markdown(f"**ROAS:** {row.get('roas', 'N/A')}")
                st.markdown(f"**AOV:** {row.get('aov', 'N/A')}")
                st.markdown(f"**CAC:** {row.get('cac', 'N/A')}")
                st.markdown(f"**CPS:** {row.get('cps', 'N/A')}")
                st.markdown(f"**Monthly Ad Spend:** {row.get('ad_spend_monthly', 'N/A')}")
            if row.get("results_summary"):
                st.markdown(f"**Results:** {row['results_summary']}")
            if row.get("permalink"):
                st.markdown(f"[Open on Reddit]({row['permalink']})")

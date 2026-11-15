"""
AI enrichment — the local twin of ai/enrich_reviews.py ("LLM as a transformation step").

Reads review text from the DuckDB warehouse, labels each review with
sentiment + topic + key issue, and writes the result to AI.REVIEW_ENRICHED,
which dbt then models into mart_review_insights (tag:ai).

Two engines, picked automatically:
  * OPENAI_API_KEY set  -> gpt-4o-mini with JSON output (same prompt as the cloud
                           version). Capped by SAMPLE_N (default 25) so a run never
                           costs more than a few paise.
  * no key              -> a free, offline keyword classifier, so the whole
                           pipeline still runs end to end on any laptop.

It is idempotent: reviews already enriched are skipped. Use --reset to start over.

Usage:
    python local/enrich_reviews.py            # auto engine
    python local/enrich_reviews.py --reset    # wipe and re-enrich
"""
import argparse
import json
import os
import re
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(os.getenv("DUCKDB_PATH", ROOT / "warehouse" / "zomato.duckdb"))

try:  # optional: read OPENAI_API_KEY / SAMPLE_N from a .env file at the repo root
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

OPENAI_MODEL = "gpt-4o-mini"
RULES_MODEL = "offline-rules-v1"
TOPICS = ["food quality", "delivery", "pricing", "service", "packaging", "other"]

SYSTEM_PROMPT = f"""
You classify customer reviews for a food delivery app.

For the review you are given, return:
- sentiment_label: positive, negative, or neutral
- sentiment_score: a number between -1.0 and 1.0
- topic: one of {TOPICS}
- key_issue: a short phrase of 6 words or less that describes the main issue in the review, if any. If there is no issue, return null

Reply as JSON in this exact format:
{{
    "sentiment_label": "<sentiment_label>",
    "sentiment_score": <sentiment_score>,
    "topic": "<topic>",
    "key_issue": "<key_issue>"}}
"""

# ---------------- offline keyword classifier ----------------
TOPIC_WORDS = {
    "delivery": ["deliver", "rider", "late", "minutes", "on time", "quick", "delay", "forever", "wrong way"],
    "pricing": ["price", "expensive", "costly", "value for money", "worth", "priced", "fee", "taxes", "offer"],
    "packaging": ["packag", "spill", "leak", "container", "sealed"],
    "service": ["support", "wrong item", "missing", "refund", "instructions", "request", "gesture"],
    "food quality": ["delicious", "tasty", "taste", "stale", "cold", "oily", "salty", "undercooked", "fresh",
                     "flavour", "cooked", "quality", "portion"],
}
POSITIVE = ["delicious", "loved", "best", "great", "fresh", "quick", "polite", "on time", "impressed", "value",
            "reasonably", "resolved", "nice", "neat", "well sealed", "perfectly", "super", "fast", "eco-friendly"]
NEGATIVE = ["cold", "stale", "oily", "salty", "undercooked", "disappointed", "gone down", "late", "forever",
            "wrong", "delay", "expensive", "poor-value", "costly", "unhelpful-support", "missing", "no refund",
            "ignored", "poor", "leaked", "broken", "spilled"]
ISSUES = [("late", "late delivery"), ("forever", "slow delivery"), ("delay", "delivery delay not communicated"),
          ("wrong way", "rider lost"), ("cold", "food arrived cold"), ("stale", "stale food"),
          ("oily", "too oily / salty"), ("undercooked", "undercooked food"), ("gone down", "quality dropped"),
          ("expensive", "overpriced for portion"), ("poor-value", "poor value"), ("costly", "high fees and taxes"),
          ("wrong item", "wrong item delivered"), ("missing", "missing items, no refund"),
          ("ignored", "instructions ignored"), ("leaked", "packaging leaked"), ("broken", "broken container")]


NEGATIONS = {"nothing spilled": "neat", "not helpful": "unhelpful-support", "not worth": "poor-value"}


def classify_offline(text):
    t = text.lower()
    for phrase, replacement in NEGATIONS.items():   # handle simple negations before keyword matching
        t = t.replace(phrase, replacement)
    scores = {topic: sum(w in t for w in words) for topic, words in TOPIC_WORDS.items()}
    topic = max(scores, key=scores.get) if max(scores.values()) > 0 else "other"
    pos = sum(len(re.findall(re.escape(w), t)) for w in POSITIVE)
    neg = sum(len(re.findall(re.escape(w), t)) for w in NEGATIVE)
    score = 0.0 if pos + neg == 0 else round((pos - neg) / (pos + neg), 2)
    label = "positive" if score > 0.2 else "negative" if score < -0.2 else "neutral"
    issue = next((name for word, name in ISSUES if word in t), None) if label == "negative" else None
    return {"sentiment_label": label, "sentiment_score": score, "topic": topic, "key_issue": issue}


def classify_openai(client, text):
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": text}],
    )
    labels = json.loads(response.choices[0].message.content)
    if labels.get("key_issue") in ("null", ""):
        labels["key_issue"] = None
    return labels


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="drop existing enrichment and start over")
    parser.add_argument("--engine", choices=["auto", "openai", "offline"], default=os.getenv("AI_ENGINE", "auto"))
    args = parser.parse_args()

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    use_openai = args.engine == "openai" or (args.engine == "auto" and api_key)
    client = None
    if use_openai:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS ai")
    if args.reset:
        con.execute("DROP TABLE IF EXISTS ai.review_enriched")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ai.review_enriched (
            review_id       BIGINT,
            sentiment_label VARCHAR,
            sentiment_score DOUBLE,
            topic           VARCHAR,
            key_issue       VARCHAR,
            model           VARCHAR,
            enriched_at     TIMESTAMP DEFAULT current_timestamp
        )
    """)

    limit = f"LIMIT {int(os.getenv('SAMPLE_N', '25'))}" if use_openai else ""
    reviews = con.execute(f"""
        SELECT review_id, comment FROM raw.reviews
        WHERE comment IS NOT NULL
          AND review_id NOT IN (SELECT review_id FROM ai.review_enriched)
        ORDER BY review_id {limit}
    """).fetchall()

    engine = OPENAI_MODEL if use_openai else RULES_MODEL
    if not reviews:
        print("No new reviews to enrich.")
        return
    print(f"Enriching {len(reviews):,} reviews with {engine} ...")

    results = []
    for review_id, comment in reviews:
        try:
            labels = classify_openai(client, comment) if use_openai else classify_offline(comment)
            topic = labels["topic"] if labels["topic"] in TOPICS else "other"
            results.append((review_id, labels["sentiment_label"], float(labels["sentiment_score"]), topic,
                            labels.get("key_issue"), engine))
        except Exception as e:  # one bad review should not stop the batch
            print(f"  skipped review {review_id}: {e}")

    import pandas as pd
    new_rows = pd.DataFrame(results, columns=["review_id", "sentiment_label", "sentiment_score", "topic",
                                              "key_issue", "model"])
    con.register("new_rows", new_rows)
    con.execute("""
        INSERT INTO ai.review_enriched (review_id, sentiment_label, sentiment_score, topic, key_issue, model)
        SELECT * FROM new_rows
    """)
    total = con.execute("SELECT COUNT(*) FROM ai.review_enriched").fetchone()[0]
    con.close()
    print(f"Saved {len(results):,} enriched reviews (table now has {total:,}).")


if __name__ == "__main__":
    main()

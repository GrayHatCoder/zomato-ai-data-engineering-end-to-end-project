"""
Zomato AI Analytics - one Streamlit app on top of the warehouse the pipeline builds.

Tabs
  1. Business dashboard   - KPIs + the dbt marts (city revenue, delivery SLA, restaurants)
  2. Review insights (AI) - mart_review_insights built from the AI enrichment step
  3. Chat with reviews    - RAG: retrieve similar reviews, answer grounded in them
  4. Ask your data        - text-to-SQL over the marts, SELECT-only guard, read-only DB

Runs fully offline. If OPENAI_API_KEY is set, tabs 3 and 4 switch to gpt-4o-mini.

    streamlit run app/streamlit_app.py
"""
import json
import os
import re
import sys
import subprocess
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(os.getenv("DUCKDB_PATH", ROOT / "warehouse" / "zomato.duckdb"))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

# ---------------- project owner ----------------
AUTHOR_NAME = "Abhishek"
GITHUB_USER = "GrayHatCoder"
REPO_NAME = "zomato-ai-data-engineering-end-to-end-project"
REPO_URL = f"https://github.com/{GITHUB_USER}/{REPO_NAME}"

# ---------------- chart colours (validated reference palette) ----------------
BLUE = "#2a78d6"
RED = "#e34948"
GRAY = "#b9b8b0"
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
SENTIMENT_COLORS = {"negative": RED, "neutral": GRAY, "positive": BLUE}
CHART_CONFIG = {"displayModeBar": False}

st.set_page_config(page_title="Zomato AI Analytics", page_icon="🍽️", layout="wide")


def api_key():
    try:
        key = st.secrets.get("OPENAI_API_KEY", "")
    except Exception:
        key = ""
    return (key or os.getenv("OPENAI_API_KEY", "")).strip()


# ---------------- warehouse access ----------------
def ensure_warehouse():
    """First run (e.g. on Streamlit Cloud): build the warehouse with the pipeline."""
    if DB_PATH.exists():
        return
    with st.status("First run: building the warehouse (generate data -> load -> dbt -> AI)...", expanded=True):
        result = subprocess.run([sys.executable, str(ROOT / "run_pipeline.py")], cwd=ROOT,
                                capture_output=True, text=True, encoding="utf-8", errors="replace")
        st.code(result.stdout[-3000:] or result.stderr[-3000:])
        if result.returncode != 0:
            st.error("Pipeline failed - see the log above.")
            st.stop()


@st.cache_data(show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)   # read-only: nothing in the app can modify data
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def style(fig, height=340):
    fig.update_layout(height=height, margin=dict(l=8, r=8, t=8, b=8), hovermode="closest",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title=None),
                      font=dict(family='system-ui, -apple-system, "Segoe UI", sans-serif'))
    fig.update_xaxes(showgrid=False, title=None)
    fig.update_yaxes(gridcolor="rgba(137,135,129,0.25)", title=None, zeroline=False)
    return fig


def money(x):
    if x >= 1e7:
        return f"₹{x / 1e7:.2f} Cr"
    if x >= 1e5:
        return f"₹{x / 1e5:.2f} L"
    return f"₹{x:,.0f}"


ensure_warehouse()

# ---------------- sidebar ----------------
with st.sidebar:
    st.markdown(f"### 🍽️ Zomato AI Analytics\nBuilt by **{AUTHOR_NAME}** · [GitHub]({REPO_URL})")
    st.caption("End-to-end data engineering: lake → warehouse → dbt (medallion) → orchestration → AI.")
    counts = q("""
        select 'orders' t, count(*) n from marts.fct_orders
        union all select 'order items', count(*) from marts.fact_order_items
        union all select 'restaurants', count(*) from marts.dim_restaurants
        union all select 'customers', count(*) from marts.dim_customer
        union all select 'reviews (AI-labelled)', count(*) from ai.review_enriched
    """)
    st.markdown("**Warehouse**")
    for _, r in counts.iterrows():
        st.markdown(f"- {r.t}: **{int(r.n):,}**")
    engine = "OpenAI gpt-4o-mini" if api_key() else "offline mode (no API key)"
    st.markdown(f"**AI engine:** {engine}")
    st.caption("Synthetic Zomato-style data for learning. Not affiliated with Zomato.")

st.title("Zomato AI Analytics")

cities = q("select distinct city from marts.fct_orders order by 1").city.tolist()
picked = st.multiselect("Cities", cities, placeholder="All cities")
city_filter = "" if not picked else "where city in (" + ",".join("'" + c.replace("'", "''") + "'" for c in picked) + ")"
city_and = city_filter.replace("where", "and", 1)

tab_dash, tab_reviews, tab_rag, tab_sql = st.tabs(
    ["📊 Business dashboard", "💬 Review insights (AI)", "🔎 Chat with reviews", "🧠 Ask your data"])

# ======================= 1. dashboard =======================
with tab_dash:
    k = q(f"""
        select count(*) orders,
               sum(case when is_delivered then sales_amount else 0 end) gmv,
               avg(case when is_delivered then sales_amount end) aov,
               avg(case when order_status = 'Cancelled' then 1.0 else 0 end) cancel_rate,
               median(delivery_time_min) median_delivery,
               avg(customer_rating) rating
        from marts.fct_orders {city_filter}
    """).iloc[0]
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Orders", f"{int(k.orders):,}")
    c2.metric("GMV (delivered)", money(k.gmv))
    c3.metric("Avg order value", f"₹{k.aov:,.0f}")
    c4.metric("Cancel rate", f"{k.cancel_rate:.1%}")
    c5.metric("Median delivery", f"{k.median_delivery:.0f} min")
    c6.metric("Avg rating", f"{k.rating:.2f} ★")

    left, right = st.columns(2)
    with left:
        st.subheader("Monthly GMV")
        m = q(f"""
            select date_trunc('month', order_date) as month, sum(gmv) gmv, sum(orders) orders
            from marts.mart_daily_city_revenune {city_filter} group by 1 order by 1
        """)
        fig = px.line(m, x="month", y="gmv", markers=True, color_discrete_sequence=[BLUE],
                      custom_data=["orders"])
        fig.update_traces(line_width=2, marker_size=8,
                          hovertemplate="%{x|%b %Y}<br>GMV ₹%{y:,.0f}<br>%{customdata[0]:,} orders<extra></extra>")
        fig.update_layout(hovermode="x unified")
        fig.update_yaxes(rangemode="tozero", tickprefix="₹")
        st.plotly_chart(style(fig), width="stretch", config=CHART_CONFIG)
    with right:
        st.subheader("GMV by city")
        c = q(f"""
            select city, sum(gmv) gmv, round(sum(gmv) / nullif(sum(delivered_orders), 0), 0) aov
            from marts.mart_daily_city_revenune {city_filter} group by 1 order by gmv
        """)
        fig = px.bar(c, x="gmv", y="city", orientation="h", color_discrete_sequence=[BLUE], custom_data=["aov"])
        fig.update_traces(width=0.6, hovertemplate="%{y}<br>GMV ₹%{x:,.0f}<br>AOV ₹%{customdata[0]:,.0f}<extra></extra>")
        fig.update_xaxes(tickprefix="₹")
        st.plotly_chart(style(fig), width="stretch", config=CHART_CONFIG)

    st.subheader("Delivery SLA - p90 delivery minutes by city and hour")
    sla = q(f"select city, order_hour, p90, p50, delivered_orders from marts.mart_delivery_sla {city_filter}")
    pivot = sla.pivot(index="city", columns="order_hour", values="p90").sort_index(ascending=False)
    fig = go.Figure(go.Heatmap(z=pivot.values, x=[f"{h:02d}:00" for h in pivot.columns], y=pivot.index,
                               colorscale=[[i / (len(BLUE_RAMP) - 1), c] for i, c in enumerate(BLUE_RAMP)],
                               xgap=2, ygap=2, colorbar=dict(title="p90 min", thickness=10),
                               hovertemplate="%{y} at %{x}<br>p90 %{z} min<extra></extra>"))
    st.plotly_chart(style(fig, 360), width="stretch", config=CHART_CONFIG)
    st.caption("Peak lunch (12-13h) and dinner (19-21h) hours run slowest - the gap is the staffing opportunity.")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Top restaurants by revenue")
        top = q(f"""
            select restaurant_name as "Restaurant", city as "City", cuisine as "Cuisine", orders as "Orders",
                   revenue as "Revenue", avg_customer_rating as "Rating", avg_delivery_min as "Avg delivery (min)"
            from marts.mart_restaurant_performance {city_filter}
            order by revenue desc limit 15
        """)
        st.dataframe(top, hide_index=True, width="stretch",
                     column_config={"Revenue": st.column_config.NumberColumn(format="₹%d")})
    with right:
        st.subheader("Cancel rate by payment method")
        p = q(f"""
            select payment_method, count(*) orders,
                   avg(case when order_status = 'Cancelled' then 1.0 else 0 end) cancel_rate
            from marts.fct_orders {city_filter} group by 1 order by cancel_rate
        """)
        fig = px.bar(p, x="cancel_rate", y="payment_method", orientation="h", color_discrete_sequence=[BLUE],
                     custom_data=["orders"])
        fig.update_traces(width=0.6, hovertemplate="%{y}<br>%{x:.1%} cancelled<br>%{customdata[0]:,} orders<extra></extra>")
        fig.update_xaxes(tickformat=".0%")
        st.plotly_chart(style(fig), width="stretch", config=CHART_CONFIG)

# ======================= 2. review insights =======================
with tab_reviews:
    ri = q(f"select * from marts.mart_review_insights {city_filter}")
    total = int(ri.reviews.sum())
    neg = int(ri.loc[ri.sentiment_label == "negative", "reviews"].sum())
    c1, c2, c3 = st.columns(3)
    c1.metric("Reviews analysed", f"{total:,}")
    c2.metric("Negative share", f"{neg / max(total, 1):.1%}")
    c3.metric("Flagged issues", f"{int(ri.flagged_issues.sum()):,}")

    st.subheader("Sentiment by topic")
    t = ri.groupby(["topic", "sentiment_label"], as_index=False).reviews.sum()
    t["share"] = t.reviews / t.groupby("topic").reviews.transform("sum")
    order = (t[t.sentiment_label == "negative"].sort_values("share").topic.tolist()
             + [x for x in t.topic.unique() if x not in set(t[t.sentiment_label == "negative"].topic)])
    fig = px.bar(t, x="share", y="topic", color="sentiment_label", orientation="h",
                 category_orders={"sentiment_label": ["negative", "neutral", "positive"], "topic": order},
                 color_discrete_map=SENTIMENT_COLORS, custom_data=["reviews"])
    fig.update_traces(width=0.6, marker_line_width=2, marker_line_color="rgba(255,255,255,0.9)",
                      hovertemplate="%{y}: %{x:.0%} (%{customdata[0]:,} reviews)<extra></extra>")
    fig.update_xaxes(tickformat=".0%")
    st.plotly_chart(style(fig), width="stretch", config=CHART_CONFIG)

    left, right = st.columns(2)
    with left:
        st.subheader("Top complaints")
        issues = q(f"""
            select e.key_issue as issue, count(*) as reviews, round(avg(r.rating), 2) as avg_stars
            from ai.review_enriched e join staging.stg_reviews r using (review_id)
            where e.key_issue is not null {city_and}
            group by 1 order by 2 desc limit 10
        """)
        st.dataframe(issues, hide_index=True, width="stretch")
    with right:
        st.subheader("Latest labelled reviews")
        latest = q(f"""
            select strftime(r.review_date, '%Y-%m-%d') as review_date, r.city, r.rating, r.comment,
                   e.sentiment_label as sentiment, e.topic, e.model
            from ai.review_enriched e join staging.stg_reviews r using (review_id)
            where true {city_and}
            order by r.review_date desc limit 50
        """)
        st.dataframe(latest, hide_index=True, width="stretch", height=380)

# ======================= 3. RAG: chat with reviews =======================
@st.cache_resource(show_spinner="Indexing reviews...")
def review_index(use_openai: bool):
    df = q("""
        select r.review_id, r.city, r.rating, r.comment, e.sentiment_label, e.topic, e.key_issue
        from staging.stg_reviews r left join ai.review_enriched e using (review_id)
    """)
    if use_openai:
        from openai import OpenAI
        client = OpenAI(api_key=api_key())
        cache = ROOT / "warehouse" / "review_embeddings.parquet"
        if cache.exists():
            emb = pd.read_parquet(cache)
        else:
            sample = df.sample(min(500, len(df)), random_state=7)
            vecs = client.embeddings.create(model="text-embedding-3-small", input=sample.comment.tolist())
            emb = pd.DataFrame({"review_id": sample.review_id, "embedding": [d.embedding for d in vecs.data]})
            emb.to_parquet(cache)
        df = df.merge(emb, on="review_id")
        matrix = np.vstack(df.embedding.values)
        return df, matrix / np.linalg.norm(matrix, axis=1, keepdims=True), None
    from sklearn.feature_extraction.text import TfidfVectorizer
    vec = TfidfVectorizer(stop_words="english", ngram_range=(1, 2), sublinear_tf=True)
    return df, vec.fit_transform(df.comment), vec


NEG_CUES = ("unhappy", "complain", "complaint", "problem", "issue", "bad", "worst", "angry", "negative",
            "late", "poor", "disappoint", "why not", "fail", "wrong", "missing", "refund")
POS_CUES = ("love", "like", "praise", "happy", "best", "good", "great", "positive", "compliment")


def retrieve(question, k=8, city=None):
    use_openai = bool(api_key())
    df, matrix, vec = review_index(use_openai)
    if use_openai:
        from openai import OpenAI
        qv = OpenAI(api_key=api_key()).embeddings.create(model="text-embedding-3-small", input=[question]).data[0].embedding
        qv = np.array(qv) / np.linalg.norm(qv)
        scores = matrix @ qv
    else:
        scores = (matrix @ vec.transform([question]).T).toarray().ravel()
        # TF-IDF has no sense of tone, so use the AI sentiment labels: a question about complaints
        # should surface negative reviews, a question about praise the positive ones.
        ql = question.lower()
        wants = ("negative" if any(w in ql for w in NEG_CUES) else
                 "positive" if any(w in ql for w in POS_CUES) else None)
        if wants:
            scores = scores * np.where(df.sentiment_label.values == wants, 2.0, 0.5)
    out = df.assign(score=scores)
    if city:
        out = out[out.city.isin(city)]
    return out.nlargest(k, "score")


with tab_rag:
    st.subheader("Chat with your reviews")
    st.caption("Retrieval-augmented generation: find the most similar reviews, then answer only from them. "
               + ("Using OpenAI embeddings + gpt-4o-mini." if api_key() else
                  "Offline mode: TF-IDF retrieval and an extractive summary (add OPENAI_API_KEY for LLM answers)."))
    examples = ["Why are customers unhappy with delivery?", "What do people say about packaging?",
                "Is the food worth the price?", "Complaints about missing items or refunds"]
    ex = st.pills("Try", examples, key="rag_pills") if hasattr(st, "pills") else None
    question = st.text_input("Ask a question about the reviews", value=ex or "",
                             placeholder="e.g. What are the most common complaints about delivery?")
    if question:
        hits = retrieve(question, city=picked or None)
        if hits.empty or hits.score.max() <= 0:
            st.info("No reviews matched that question - try different words.")
        else:
            if api_key():
                from openai import OpenAI
                context = "\n".join(f"- ({r.city}, {r.rating} stars) {r.comment}" for r in hits.itertuples())
                resp = OpenAI(api_key=api_key()).chat.completions.create(
                    model="gpt-4o-mini", temperature=0.2,
                    messages=[{"role": "system", "content": "Answer ONLY using the customer reviews provided. "
                               "Be concise. If the reviews don't cover it, say so."},
                              {"role": "user", "content": f"Question: {question}\n\nReviews:\n{context}"}])
                st.markdown("**Answer**")
                st.write(resp.choices[0].message.content)
            else:
                mix = hits.sentiment_label.value_counts(normalize=True)
                issues = hits.key_issue.dropna().value_counts()
                st.markdown("**What the matching reviews say**")
                lines = [f"- {len(hits)} closest reviews · avg **{hits.rating.mean():.1f}★** · "
                         + ", ".join(f"{v:.0%} {k}" for k, v in mix.items())]
                if not issues.empty:
                    lines.append("- Recurring issues: " + ", ".join(f"**{i}** ({n})" for i, n in issues.head(4).items()))
                lines.append(f"- Main topic: **{hits.topic.mode().iat[0]}**; cities: "
                             + ", ".join(hits.city.value_counts().head(3).index))
                st.markdown("\n".join(lines))
            with st.expander("Reviews used for this answer", expanded=not api_key()):
                st.dataframe(hits[["city", "rating", "comment", "sentiment_label", "topic"]], hide_index=True,
                             width="stretch")

# ======================= 4. text-to-SQL =======================
FORBIDDEN = ["drop", "delete", "truncate", "alter", "update", "insert", "create", "replace", "grant", "revoke",
             "attach", "copy", "export", "install", "load", "pragma", "set ", "call"]
TEMPLATES = {
    "Top 10 cities by GMV": "select city, sum(gmv) as gmv from mart_daily_city_revenune group by 1 order by 2 desc limit 10",
    "Which cuisine has the most orders?": "select cuisine, count(*) as orders from fct_orders group by 1 order by 2 desc limit 10",
    "Average delivery time by city, worst first": "select city, round(avg(delivery_time_min), 1) as avg_delivery_min from fct_orders where is_delivered group by 1 order by 2 desc",
    "Cancel rate by payment method": "select payment_method, round(avg(case when order_status = 'Cancelled' then 1.0 else 0 end), 4) as cancel_rate from fct_orders group by 1 order by 2 desc",
    "Top 10 restaurants by revenue": "select restaurant_name, city, revenue from mart_restaurant_performance order by revenue desc limit 10",
    "Orders by age segment": "select c.age_segment, count(*) as orders from fct_orders f join dim_customer c using (customer_id) group by 1 order by 2 desc",
    "Weekend vs weekday GMV": "select d.is_weekend, sum(f.sales_amount) as gmv from fct_orders f join dim_date d on f.order_date = d.date_day where f.is_delivered group by 1",
}


@st.cache_data
def marts_schema():
    cols = q("""select table_name, string_agg(column_name, ', ' order by ordinal_position) cols
                from information_schema.columns where table_schema = 'marts' group by 1 order by 1""")
    return "\n".join(f"{r.table_name.upper()}({r.cols})" for r in cols.itertuples())


def is_safe(sql):
    s = sql.strip().lower()
    if not (s.startswith("select") or s.startswith("with")) or ";" in s.rstrip(";"):
        return False
    return not any(re.search(rf"\b{w.strip()}\b", s) for w in FORBIDDEN)


def run_sql(sql):
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        con.execute("set schema = 'marts'")
        return con.execute(sql).df()
    finally:
        con.close()


with tab_sql:
    st.subheader("Ask your data in plain English")
    has_key = bool(api_key())
    st.caption("Text-to-SQL over the Gold marts. Queries run on a read-only connection behind a SELECT-only guard. "
               + ("gpt-4o-mini writes the SQL." if has_key else
                  "Offline mode: pick a question (or write SQL yourself); add OPENAI_API_KEY for free-form English."))
    if has_key:
        question = st.text_input("Your question", placeholder="e.g. Top 10 restaurants by revenue in Bangalore")
        sql = ""
        if question:
            from openai import OpenAI
            system = ("You are a DuckDB SQL expert. Write ONE SELECT query that answers the question. "
                      "SELECT only, never modify data. Use bare table names. Add LIMIT 100 or less unless the "
                      "question asks for a single total. 'gmv' means delivered revenue. Prefer MART_ tables. "
                      'Reply as JSON: {"sql": "..."}\n\nTables:\n' + marts_schema())
            resp = OpenAI(api_key=api_key()).chat.completions.create(
                model="gpt-4o-mini", temperature=0, response_format={"type": "json_object"},
                messages=[{"role": "system", "content": system}, {"role": "user", "content": question}])
            sql = json.loads(resp.choices[0].message.content)["sql"].strip().rstrip(";")
    else:
        choice = st.selectbox("Question", list(TEMPLATES) + ["Write my own SQL"])
        sql = TEMPLATES.get(choice, "")
        if choice == "Write my own SQL":
            sql = st.text_area("SQL (SELECT only)", "select city, count(*) as orders from fct_orders group by 1",
                               height=120)
    if sql:
        st.code(sql, language="sql")
        if not is_safe(sql):
            st.error("Blocked: only a single SELECT query is allowed.")
        else:
            try:
                df = run_sql(sql)
                st.success(f"{len(df):,} rows")
                st.dataframe(df, hide_index=True, width="stretch")
                if len(df.columns) == 2 and len(df) > 1 and pd.api.types.is_numeric_dtype(df.iloc[:, 1]):
                    fig = px.bar(df, x=df.columns[0], y=df.columns[1], color_discrete_sequence=[BLUE])
                    fig.update_traces(width=0.6)
                    st.plotly_chart(style(fig), width="stretch", config=CHART_CONFIG)
            except Exception as e:
                st.error(f"Query failed: {e}")
    with st.expander("Tables you can query"):
        st.code(marts_schema())

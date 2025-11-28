import pandas as pd
import streamlit as st
import mysql.connector
import plotly.express as px
from cassandra.cluster import Cluster
from sklearn.metrics.pairwise import cosine_similarity

# -------------------------------------------------
# MySQL helpers
# -------------------------------------------------
def get_mysql_conn():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="epl"
    )

def load_mysql_live():
    conn = get_mysql_conn()
    df = pd.read_sql("""
        SELECT transaction_id, event_ts, fan_id, team, city, country,
               product_name, unit_price, quantity
        FROM fan_sales
        ORDER BY event_ts DESC
        LIMIT 500;
    """, conn)
    conn.close()

    if not df.empty:
        df["revenue"] = df["unit_price"] * df["quantity"]
        df["event_ts"] = pd.to_datetime(df["event_ts"])
    return df

# -------------------------------------------------
# Cassandra helpers
# -------------------------------------------------
def get_cass_session():
    cluster = Cluster(["localhost"])  # or "cassandra" in Docker net
    return cluster.connect("epl")

def load_cassandra_history():
    session = get_cass_session()
    rows = session.execute("""
        SELECT transaction_id, event_ts, fan_id, team, city, country,
               product_name, unit_price, quantity
        FROM fan_sales_cass;
    """)

    data = []
    for r in rows:
        data.append({
            "transaction_id": r.transaction_id,
            "event_ts": r.event_ts,
            "fan_id": r.fan_id,
            "team": r.team,
            "city": r.city,
            "country": r.country,
            "product_name": r.product_name,
            "unit_price": r.unit_price,
            "quantity": r.quantity,
        })
    df = pd.DataFrame(data)
    if not df.empty:
        df["revenue"] = df["unit_price"] * df["quantity"]
    return df

# -------------------------------------------------
# Recommendations
# -------------------------------------------------

def recommend_global_man_utd(top_n=3):
    """
    Simple popularity-based recommender for ALL Man Utd fans.
    """
    conn = get_mysql_conn()
    df = pd.read_sql("""
        SELECT product_name, SUM(quantity) AS total
        FROM fan_sales
        WHERE team = 'Man Utd'
        GROUP BY product_name
        ORDER BY total DESC;
    """, conn)
    conn.close()

    if df.empty:
        return ["No purchase history yet"]
    return df["product_name"].head(top_n).tolist()


def recommend_for_fan(fan_id, top_n=3):
    """
    Personalized recommender using collaborative filtering
    over Man Utd fans only.
    """
    conn = get_mysql_conn()
    df = pd.read_sql("""
        SELECT fan_id, product_name, SUM(quantity) AS qty
        FROM fan_sales
        WHERE team = 'Man Utd'
        GROUP BY fan_id, product_name;
    """, conn)
    conn.close()

    if df.empty or fan_id not in df["fan_id"].unique():
        return ["Not enough data for this fan yet"]

    pivot = df.pivot_table(
        index="fan_id",
        columns="product_name",
        values="qty",
        fill_value=0
    )

    # compute similarity matrix
    sim = cosine_similarity(pivot)
    sim_df = pd.DataFrame(sim, index=pivot.index, columns=pivot.index)

    # similar users (excluding the fan themself)
    similar_users = sim_df[fan_id].sort_values(ascending=False).iloc[1:6].index

    # aggregate purchases of similar users
    similar_purchases = pivot.loc[similar_users].sum().sort_values(ascending=False)

    # remove items already owned by this fan
    user_items = pivot.loc[fan_id]
    candidates = similar_purchases[user_items == 0]

    if candidates.empty:
        return ["No new items to recommend"]

    return list(candidates.head(top_n).index)

# -------------------------------------------------
# Streamlit UI
# -------------------------------------------------
st.set_page_config(page_title="EPL Real-Time Dashboard", layout="wide")

st.title("⚽ (MUFRS) Manchester United Fan Recommendation System")

st.write(
    "Data flows from Kafka producer → consumer → **MySQL & Cassandra**. "
    "This dashboard reads from both stores and provides **personalized recommendations** "
    "for Manchester United fans."
)

# --- Load data ---
live_df = load_mysql_live()
hist_df = load_cassandra_history()

tab1, tab2, tab3 = st.tabs(["🔴 Live", "📦 Historical", "⭐ Recommendations"])

# -------------------------------------------------
# TAB 1: LIVE VIEW (MySQL)
# -------------------------------------------------
with tab1:
    st.subheader("Live Data")
    if live_df.empty:
        st.warning("No live data yet. Make sure your producer and new_consumer.py are running.")
    else:
        col1, col2, col3 = st.columns(3)
        total_rev = live_df["revenue"].sum()
        total_tx = len(live_df)
        man_utd_tx = (live_df["team"] == "Man Utd").sum()

        col1.metric("Total Revenue (live)", f"${total_rev:,.2f}")
        col2.metric("Transactions (last 500)", total_tx)
        col3.metric("Man Utd Transactions", man_utd_tx)

        # Charts
        c1, c2 = st.columns(2)
        with c1:
            team_rev = live_df.groupby("team")["revenue"].sum().reset_index()
            fig_team = px.bar(team_rev, x="team", y="revenue", title="Revenue by Team (Live)")
            st.plotly_chart(fig_team, use_container_width=True)

        with c2:
            prod_rev = live_df.groupby("product_name")["revenue"].sum().reset_index()
            fig_prod = px.bar(prod_rev, x="product_name", y="revenue",
                              title="Revenue by Product (Live)")
            st.plotly_chart(fig_prod, use_container_width=True)

        st.markdown("#### Recent Transactions")
        st.dataframe(live_df.head(50))

# -------------------------------------------------
# TAB 2: HISTORICAL VIEW (Cassandra)
# -------------------------------------------------
with tab2:
    st.subheader("Historical / Scalable Storage (Cassandra)")
    if hist_df.empty:
        st.warning("No data in Cassandra yet. Let your new_consumer.py run a bit.")
    else:
        st.write("Below is an aggregate view built from **fan_sales_cass** in Cassandra.")

        c1, c2 = st.columns(2)
        with c1:
            team_hist = hist_df.groupby("team")["revenue"].sum().reset_index()
            fig_team_hist = px.bar(team_hist, x="team", y="revenue",
                                   title="Total Revenue by Team (Historical)")
            st.plotly_chart(fig_team_hist, use_container_width=True)

        with c2:
            prod_hist = hist_df.groupby("product_name")["revenue"].sum().reset_index()
            fig_prod_hist = px.bar(prod_hist, x="product_name", y="revenue",
                                   title="Total Revenue by Product (Historical)")
            st.plotly_chart(fig_prod_hist, use_container_width=True)

        st.markdown("#### Sample of Historical Data (Cassandra)")
        st.dataframe(hist_df.head(50))

# -------------------------------------------------
# TAB 3: RECOMMENDATIONS
# -------------------------------------------------
with tab3:
    st.subheader("Recommendations for Manchester United Fans")

    st.markdown("**Global Man Utd Recommendations (most popular products):**")
    global_recs = recommend_global_man_utd()
    st.write(global_recs)

    st.markdown("---")
    st.markdown("**Personalized Recommendations by Fan ID (Man Utd only):**")

    fan_id_input = st.number_input("Enter Fan ID", min_value=1, step=1, value=1779)
    if st.button("Get Recommendations"):
        recs = recommend_for_fan(int(fan_id_input))
        st.write(f"Recommendations for fan_id {int(fan_id_input)}:")
        st.write(recs)

st.info("Click 'Rerun' in the Streamlit menu to refresh data as new events stream in.")


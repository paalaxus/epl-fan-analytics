import pandas as pd
import streamlit as st
import mysql.connector
import plotly.express as px
from cassandra.cluster import Cluster

def record_recommended_purchase(fan_id, product_name, price=50.0, qty=1):
    """
    Create a synthetic purchase in MySQL with source='recommended'.
    """
    conn = get_mysql_conn()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO fan_sales (
            transaction_id, event_ts, fan_id, team, country,
            product_name, unit_price, quantity, source
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    import uuid
    from datetime import datetime

    cursor.execute(insert_sql, (
        str(uuid.uuid4()),
        datetime.utcnow().isoformat(),
        fan_id,
        "Man Utd",
        "UK",
        product_name,
        float(price),
        int(qty),
        "recommended",
    ))

    conn.commit()
    cursor.close()
    conn.close()

    return True


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
        SELECT transaction_id, event_ts, fan_id, team, country,
               product_name, unit_price, quantity, source
        FROM fan_sales
        WHERE team = 'Man Utd'
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
    cluster = Cluster(["localhost"])
    return cluster.connect("epl")

def load_cassandra_history():
    session = get_cass_session()
    rows = session.execute("""
        SELECT transaction_id, event_ts, fan_id, team, country,
               product_name, unit_price, quantity,source
        FROM fan_sales
        WHERE team = 'Man Utd' ALLOW FILTERING;
    """)

    data = []
    for r in rows:
        data.append({
            "transaction_id": r.transaction_id,
            "event_ts": r.event_ts,
            "fan_id": r.fan_id,
            "team": r.team,
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
# NO-ML CO-OCCURRENCE RECOMMENDER
# -------------------------------------------------
def build_cooccurrence_matrix(df):
    matrix = {}

    for fan_id, group in df.groupby("fan_id"):
        products = list(group["product_name"].unique())

        for p in products:
            matrix.setdefault(p, {})

        for i in range(len(products)):
            for j in range(i + 1, len(products)):
                a, b = products[i], products[j]

                matrix[a][b] = matrix[a].get(b, 0) + 1
                matrix.setdefault(b, {})
                matrix[b][a] = matrix[b].get(a, 0) + 1

    return matrix


def recommend_for_fan(fan_id, full_df, top_n=3):
    user_history = full_df[full_df["fan_id"] == fan_id]

    if user_history.empty:
        return ["Not enough data for this fan"]

    matrix = build_cooccurrence_matrix(full_df)
    purchased = list(user_history["product_name"].unique())
    scores = {}

    for p in purchased:
        if p not in matrix:
            continue
        for related, score in matrix[p].items():
            if related not in purchased:
                scores[related] = scores.get(related, 0) + score

    if not scores:
        return ["No new items to recommend"]

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [name for name, _ in ranked[:top_n]]


def recommend_global_man_utd(top_n=3):
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


# -------------------------------------------------
# Streamlit UI
# -------------------------------------------------
st.set_page_config(page_title="EPL Real-Time Dashboard", layout="wide")

st.title("🔴 Manchester United Fan Recommendation System")

# ---- SESSION STATE DEFAULTS ----
if "fan_id" not in st.session_state:
    st.session_state.fan_id = 1011

if "recs" not in st.session_state:
    st.session_state.recs = []

if "selected_item" not in st.session_state:
    st.session_state.selected_item = None



# --- Load data ---
live_df = load_mysql_live()
hist_df = load_cassandra_history()

# Combined dataset for recommendations
if live_df.empty and hist_df.empty:
    merged_df = pd.DataFrame()
else:
    merged_df = pd.concat([live_df, hist_df], ignore_index=True)

tab1, tab2, tab3, tab4 = st.tabs(["🔴 Live", "📦 Historical", "⭐ Recommendations", "📈 Conversion Analytics"])

# -------------------------------------------------
# TAB 1: LIVE VIEW (MySQL)
# -------------------------------------------------
with tab1:
    st.subheader("Live Man Utd Data")
    if live_df.empty:
        st.warning("No live data yet. Start the producer + consumer.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Revenue (live)", f"${live_df['revenue'].sum():,.2f}")
        col2.metric("Transactions", len(live_df))
        col3.metric("Unique Fans", live_df["fan_id"].nunique())

        c1, c2 = st.columns(2)
        with c1:
            fig1 = px.bar(
                live_df.groupby("product_name")["revenue"].sum().reset_index(),
                x="product_name", y="revenue",
                title="Revenue by Product (Live)"
            )
            st.plotly_chart(fig1, use_container_width=True)

        with c2:
            fig2 = px.line(
                live_df.sort_values("event_ts"),
                x="event_ts", y="revenue",
                title="Revenue Timeline (Live)"
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown("#### Recent Transactions")
        st.dataframe(live_df.head(50))

# -------------------------------------------------
# TAB 2: HISTORICAL VIEW (Cassandra)
# -------------------------------------------------
with tab2:
    st.subheader("Historical Man Utd Data (Cassandra)")
    if hist_df.empty:
        st.warning("No Cassandra data yet.")
    else:
        fig3 = px.bar(
            hist_df.groupby("product_name")["revenue"].sum().reset_index(),
            x="product_name", y="revenue",
            title="Historical Revenue by Product"
        )
        st.plotly_chart(fig3, use_container_width=True)

        st.markdown("#### Sample")
        st.dataframe(hist_df.head(50))

# -------------------------------------------------
# Cassandra product metadata lookup
# -------------------------------------------------
def load_product_info():
    session = get_cass_session()
    rows = session.execute("SELECT product_name, image_url, description FROM product_info;")
    data = {}
    for r in rows:
        data[r.product_name] = {
            "image_url": r.image_url,
            "description": r.description
        }
    return data

product_info = load_product_info()

# -------------------------------------------------
# TAB 3: RECOMMENDATIONS WITH IMAGES
# -------------------------------------------------
with tab3:
    st.subheader("🎯 Personalized Recommendations With Images")

    if merged_df.empty:
        st.warning("Not enough data for recommendations.")
    else:
        # Persist input
        st.session_state.fan_id = st.number_input(
            "Enter Fan ID",
            min_value=1,
            step=1,
            value=st.session_state.fan_id
        )

        # Get recommendations
        if st.button("Get Recommendations"):
            st.session_state.recs = recommend_for_fan(
                int(st.session_state.fan_id),
                merged_df
            )

        # Display recommendations as image cards
        if st.session_state.recs:
            st.write(f"Top Recommendations for Fan {st.session_state.fan_id}")

            cols = st.columns(3)

            for idx, item in enumerate(st.session_state.recs):
                col = cols[idx % 3]

                with col:
                    info = product_info.get(item, None)

                    if info and info["image_url"]:
                        st.image(info["image_url"], width=200)
                    else:
                        st.image("https://via.placeholder.com/200?text=No+Image")

                    st.markdown(f"### {item}")

                    if info and info["description"]:
                        st.caption(info["description"])

                    if st.button(f"Buy {item}", key=f"buy_{idx}"):
                        ok = record_recommended_purchase(
                            int(st.session_state.fan_id),
                            item
                        )
                        if ok:
                            st.success(f"Purchased {item} (recommended)")


# -------------------------------------------------
# TAB 4: CONVERSION ANALYTICS (Organic vs Recommended)
# -------------------------------------------------
with tab4:
    st.subheader("📈 Conversion Analytics (Organic vs Recommended)")

    # ---- Use MySQL live data (source included) ----
    if live_df.empty:
        st.warning("No data available yet.")
    else:
        # Group revenue by source
        source_rev = live_df.groupby("source")["revenue"].sum().reset_index()

        # Count transactions by source
        source_count = live_df.groupby("source")["transaction_id"].count().reset_index()
        source_count.columns = ["source", "count"]

        # ---- Revenue Chart ----
        st.markdown("### 💰 Revenue by Source")
        fig_rev = px.bar(
            source_rev,
            x="source",
            y="revenue",
            color="source",
            title="Revenue: Organic vs Recommended",
        )
        st.plotly_chart(fig_rev, use_container_width=True)

        # ---- Transaction Count Pie Chart ----
        st.markdown("### 🥧 Transaction Count by Source")
        fig_count = px.pie(
            source_count,
            names="source",
            values="count",
            title="Percentage of Transactions (Organic vs Recommended)",
        )
        st.plotly_chart(fig_count, use_container_width=True)

        # ---- Conversion Rate ----
        total_recommended = source_count[source_count["source"] == "recommended"]["count"].sum()
        total_organic = source_count[source_count["source"] == "organic"]["count"].sum()
        total = total_recommended + total_organic

        if total > 0:
            conversion_rate = (total_recommended / total) * 100
        else:
            conversion_rate = 0

        st.metric("📊 Recommendation Conversion Rate", f"{conversion_rate:.2f}%")

        # ---- Top Purchased Recommended Items ----
        st.markdown("### ⭐ Top Purchased Recommended Items")
        rec_items = live_df[live_df["source"] == "recommended"]
        
        if rec_items.empty:
            st.info("No recommended items purchased yet.")
        else:
            top_rec = (
                rec_items.groupby("product_name")["quantity"].sum()
                .reset_index()
                .sort_values("quantity", ascending=False)
            )

            fig_top_rec = px.bar(
                top_rec,
                x="product_name",
                y="quantity",
                title="Top Purchased Recommended Items",
                color="product_name",
            )
            st.plotly_chart(fig_top_rec, use_container_width=True)

            st.markdown("### 📋 Full Recommended Purchases")
            st.dataframe(rec_items.head(50))


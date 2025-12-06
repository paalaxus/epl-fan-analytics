import streamlit as st
import pandas as pd
import plotly.express as px

from utils import (
    load_mysql_live,
    load_cassandra_history,
    load_product_info
)

st.set_page_config(page_title="Retailer Dashboard", layout="wide")
st.title("📊 Retailer Sales & Recommendation Analytics")

# Load data
live_df = load_mysql_live()
hist_df = load_cassandra_history()
product_info = load_product_info()

tab1, tab2, tab3 = st.tabs([
    "🔴 Live Sales",
    "📦 Historical",
    "📈 Conversion Analytics"
])

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
# TAB 3: CONVERSION ANALYTICS
# -------------------------------------------------
with tab3:
    st.subheader("📈 Conversion Analytics (Organic vs Recommended)")

    if live_df.empty:
        st.warning("No data available yet.")
    else:
        # Group revenue by source
        source_rev = live_df.groupby("source")["revenue"].sum().reset_index()

        # Count transactions by source
        source_count = live_df.groupby("source")["transaction_id"].count().reset_index()
        source_count.columns = ["source", "count"]

        fig_rev = px.bar(
            source_rev,
            x="source",
            y="revenue",
            color="source",
            title="Revenue: Organic vs Recommended",
        )
        st.plotly_chart(fig_rev, use_container_width=True)

        fig_count = px.pie(
            source_count,
            names="source",
            values="count",
            title="Percentage of Transactions (Organic vs Recommended)",
        )
        st.plotly_chart(fig_count, use_container_width=True)

        total_recommended = source_count[source_count["source"] == "recommended"]["count"].sum()
        total = source_count["count"].sum()

        conversion_rate = (total_recommended / total) * 100 if total else 0

        st.metric("📊 Recommendation Conversion Rate", f"{conversion_rate:.2f}%")


import time
import random
import pandas as pd
import streamlit as st

st.set_page_config(page_title="EPL Fan Sales Dashboard", layout="wide")

st.title("⚽ EPL Fan Sales - Realtime Dashboard ")
st.write(
    "HELLO"
)

placeholder = st.empty()

def generate_fake_data(n=50):
    teams = ["Man Utd", "Chelsea", "Arsenal", "Liverpool"]
    products = ["Jersey", "Scarf", "Ticket", "Poster"]
    cities = ["London", "Manchester", "Liverpool"]
    data = []
    for _ in range(n):
        data.append(
            {
                "team": random.choice(teams),
                "product_name": random.choice(products),
                "city": random.choice(cities),
                "revenue": round(random.uniform(10, 100) * random.randint(1, 5), 2),
            }
        )
    return pd.DataFrame(data)

while True:
    df = generate_fake_data()

    with placeholder.container():
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Revenue by Team")
            st.bar_chart(df.groupby("team")["revenue"].sum())

        with col2:
            st.subheader("Revenue by Product")
            st.bar_chart(df.groupby("product_name")["revenue"].sum())

        st.subheader("Raw Sample Data")
        st.dataframe(df.head(20))

    time.sleep(3)

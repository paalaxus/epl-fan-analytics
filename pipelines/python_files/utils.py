import mysql.connector
import pandas as pd
from cassandra.cluster import Cluster
import uuid
from datetime import datetime


def recommend_from_cart(cart: dict, df: pd.DataFrame, top_n=5):
    """
    Recommend items based on what OTHER users purchased
    when they also bought the items currently in the cart.
    """
    if not cart or df.empty:
        return []

    cart_items = list(cart.keys())

    # Find all fans who purchased ANY item in the cart
    similar_buyers = df[df["product_name"].isin(cart_items)]["fan_id"].unique()

    # Get all purchases from those similar buyers
    similar_purchases = df[df["fan_id"].isin(similar_buyers)]

    # Count how often items appear
    co_counts = {}

    for _, row in similar_purchases.iterrows():
        item = row["product_name"]
        if item not in cart_items:   # don't recommend what's already in the cart
            co_counts[item] = co_counts.get(item, 0) + 1

    # Sort by frequency
    ranked = sorted(co_counts.items(), key=lambda x: x[1], reverse=True)

    return [name for name, _ in ranked[:top_n]]

# ---------------------------------------------
# MYSQL CONNECTION
# ---------------------------------------------
def get_mysql_conn():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="epl"
    )


# ---------------------------------------------
# LOAD LIVE MYSQL SALES
# ---------------------------------------------
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


# ---------------------------------------------
# CASSANDRA SESSION
# ---------------------------------------------
def get_cass_session():
    cluster = Cluster(["localhost"])
    return cluster.connect("epl")


# ---------------------------------------------
# LOAD CASSANDRA HISTORICAL SALES
# ---------------------------------------------
def load_cassandra_history():
    session = get_cass_session()
    rows = session.execute("""
        SELECT transaction_id, event_ts, fan_id, team, country,
               product_name, unit_price, quantity, source
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
            "source": getattr(r, "source", "organic")
        })
    df = pd.DataFrame(data)
    if not df.empty:
        df["revenue"] = df["unit_price"] * df["quantity"]
    return df


# ---------------------------------------------
# LOAD PRODUCT INFO (image + description)
# ---------------------------------------------
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


# ---------------------------------------------
# RECORD A RECOMMENDED PURCHASE
# (Used by fan_store.py)
# ---------------------------------------------
def record_recommended_purchase(fan_id, product_name, price=50.0, qty=1):
    conn = get_mysql_conn()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO fan_sales (
            transaction_id, event_ts, fan_id, team, country,
            product_name, unit_price, quantity, source
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

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
# RECOMMENDER: Build co-occurrence matrix
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


# -------------------------------------------------
# RECOMMENDER: Recommend top N items for a fan
# -------------------------------------------------
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


# -------------------------------------------------
# RECOMMENDER: Global top sellers (fallback)
# -------------------------------------------------
def recommend_global(top_n=3):
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


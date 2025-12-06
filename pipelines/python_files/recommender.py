#!/usr/bin/env python3
import mysql.connector
import pandas as pd


# ===================================================
# 1. LOAD DATA FROM MYSQL (Real-Time)
# ===================================================
def load_mysql_data():
    conn = mysql.connector.connect(
        host="mysql",     # IMPORTANT: container name
        user="root",
        password="root",
        database="epl"
    )

    df = pd.read_sql("""
        SELECT fan_id, product_name 
        FROM fan_sales
        WHERE fan_id IS NOT NULL 
          AND product_name IS NOT NULL
    """, conn)

    conn.close()
    return df


# ===================================================
# 2. BUILD CO-OCCURRENCE MATRIX
# ===================================================
def build_cooccurrence(df):
    """
    Builds a table showing how often two products 
    are purchased by the same fan.
    """
    # Self-join on fan_id
    pairs = df.merge(df, on="fan_id")

    # Remove self-matches
    pairs = pairs[pairs["product_name_x"] != pairs["product_name_y"]]

    # Count co-occurrence frequency
    cooccur = (
        pairs.groupby(["product_name_x", "product_name_y"])
             .size()
             .reset_index(name="count")
    )

    return cooccur


# ===================================================
# 3. RECOMMEND FOR A PRODUCT
# ===================================================
def recommend_for_product(product, cooccur, top_k=3):
    """
    Returns products most commonly bought with the given product.
    """
    recs = (
        cooccur[cooccur["product_name_x"] == product]
              .sort_values("count", ascending=False)
              .head(top_k)
    )

    return recs["product_name_y"].tolist()


# ===================================================
# 4. RECOMMEND FOR A SPECIFIC FAN
# ===================================================
def recommend_for_fan(fan_id, df, cooccur, top_k=3):
    """
    Generates personalized recommendations for a fan
    based on what similar fans have purchased.
    """
    # Products this fan purchased
    fan_items = df[df["fan_id"] == fan_id]["product_name"].unique()

    recs = []
    for item in fan_items:
        recs.extend(recommend_for_product(item, cooccur, top_k=top_k))

    # Unique, remove what the fan already bought
    recs = [r for r in set(recs) if r not in fan_items]

    return recs[:top_k]


# ===================================================
# 5. MAIN FUNCTION
# ===================================================
def main():
    print("Loading MySQL Data...")
    df = load_mysql_data()

    if df.empty:
        print("No purchase data found! Add events to MySQL first.")
        return

    print(f"Loaded {len(df)} rows.")

    print("Building co-occurrence matrix...")
    cooccur = build_cooccurrence(df)
    print("Co-occurrence pairs:", len(cooccur))

    # Pick a random fan to demo recommendations
    sample_fan = df["fan_id"].sample(1).iloc[0]
    print("\nSample Fan:", sample_fan)

    recs = recommend_for_fan(sample_fan, df, cooccur)
    print("Recommendations for fan", sample_fan, ":", recs)

    # Example: Recommend similar items to a product (for UI)
    example_product = df["product_name"].sample(1).iloc[0]
    product_recs = recommend_for_product(example_product, cooccur)
    print("\nProducts similar to", example_product, ":", product_recs)


if __name__ == "__main__":
    main()

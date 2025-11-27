import mysql.connector
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="epl"
)

df = pd.read_sql("""
    SELECT fan_id, product_name, SUM(quantity) AS qty
    FROM fan_sales
    WHERE team = 'Man Utd'
    GROUP BY fan_id, product_name;
""", conn)
conn.close()

# Pivot to user-item matrix
pivot = df.pivot_table(index='fan_id', columns='product_name', values='qty', fill_value=0)

# Similarity matrix
similarity = cosine_similarity(pivot)
similarity_df = pd.DataFrame(similarity, index=pivot.index, columns=pivot.index)

def recommend_items(fan_id, top_n=3):
    if fan_id not in pivot.index:
        return ["No history for this fan"]

    # Find similar users
    similar_users = similarity_df[fan_id].sort_values(ascending=False)[1:6].index

    # Aggregate their purchases
    recommendations = pivot.loc[similar_users].sum().sort_values(ascending=False)

    # Filter out items already purchased
    user_items = pivot.loc[fan_id]
    recommendations = recommendations[user_items == 0]

    return list(recommendations.head(top_n).index)

# EXAMPLE:
print(recommend_items(1779))


import streamlit as st
import pandas as pd
from utils import (
    get_mysql_conn,
    load_mysql_live,
    load_cassandra_history,
    recommend_from_cart,
    load_product_info,
    record_recommended_purchase
)

PLACEHOLDER = "https://via.placeholder.com/200?text=No+Image"

def safe_image(url):
    return url if url else PLACEHOLDER


st.set_page_config(page_title="Fan Storefront", layout="wide")
st.title("⚽ Fan Shopping & Personalized Recommendations")

# -------------------------
# CART INITIALIZATION
# -------------------------
if "cart" not in st.session_state:
    st.session_state.cart = {}

# -------------------------
# LOAD DATA
# -------------------------
live_df = load_mysql_live()
hist_df = load_cassandra_history()

merged_df = (
    pd.concat([live_df, hist_df], ignore_index=True)
    if not live_df.empty or not hist_df.empty
    else pd.DataFrame()
)

product_info = load_product_info()

# --- TWO COLUMN LAYOUT ---
left, right = st.columns([2, 1])

# ==========================================================
# LEFT COLUMN — SHOP ITEMS + CART DISPLAY
# ==========================================================
left.subheader("🛒 Browse Products")

for name, info in product_info.items():
    with left.container():
        left.image(safe_image(info.get("image_url")), width=180)
        left.markdown(f"**{name}**")
        left.caption(info.get("description", ""))

        # Add to cart button
        if left.button(f"Add to Cart: {name}", key=f"add_cart_{name}"):
            st.session_state.cart[name] = st.session_state.cart.get(name, 0) + 1
            left.success(f"Added {name} to cart")

left.markdown("---")
left.subheader("🛍️ Your Cart")

# LEFT COLUMN CART DISPLAY — NO CHECKOUT HERE
if not st.session_state.cart:
    left.info("Your cart is empty.")
else:
    total_cost = 0
    for item, qty in st.session_state.cart.items():
        price = 50
        item_total = price * qty
        total_cost += item_total
        left.write(f"**{item}** — Qty: {qty} — ${item_total:.2f}")

    left.markdown(f"### Total: ${total_cost:.2f}")


# ==========================================================
# RIGHT COLUMN — CART (TOP) + CART-BASED RECOMMENDATIONS
# ==========================================================

# -------------------------
# CART AT THE TOP RIGHT
# -------------------------
with right.container():
    right.subheader("🛍️ Your Cart")

    if not st.session_state.cart:
        right.info("Your cart is empty.")
    else:
        total_cost = 0
        for item, qty in st.session_state.cart.items():
            price = 50
            item_total = price * qty
            total_cost += item_total
            right.write(f"**{item}** — Qty: {qty} — ${item_total:.2f}")

        right.markdown(f"### Total: ${total_cost:.2f}")

        # Unique key for checkout button on right
        if right.button("Checkout!", key="checkout_right"):
            for item, qty in st.session_state.cart.items():
                record_recommended_purchase(
                    1011,    # TODO: replace with real dynamic fan ID if needed
                    item,
                    price=50,
                    qty=qty
                )
            right.success("Purchase complete!")
            st.session_state.cart = {}

right.markdown("---")

# -------------------------
# RECOMMENDATIONS BASED ON CART — TOP 2 ONLY
# -------------------------
right.subheader("🎯 People Also Bought")

# Get only top 2 recommendations
recs = recommend_from_cart(st.session_state.cart, merged_df)[:2]

if not recs:
    right.info("Add items to your cart to see smart recommendations.")
else:
    for rec in recs:
        info = product_info.get(rec, {})
        right.image(safe_image(info.get("image_url")), width=180)

        right.markdown(f"**{rec}**")
        right.caption(info.get("description", ""))

        if right.button(f"Add to Cart: {rec}", key=f"add_rec_cart_{rec}"):
            st.session_state.cart[rec] = st.session_state.cart.get(rec, 0) + 1
            right.success(f"Added {rec} to cart")


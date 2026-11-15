"""
Generate a Zomato-style food-delivery dataset that matches the exact CSV layout
the Snowflake RAW tables expect (see snowflake/04_raw_tables.sql).

Writes one CSV per table into a local "data lake" that mirrors the S3 layout:

    data/lake/raw/<table>/<table>.csv

The four dimension files are deliberately *messy* (like the real Swiggy/Zomato
scrape the cloud version uses) so the dbt staging layer has real cleaning to do:
  - a leading unnamed index column
  - ratings of "--", rating counts like "50+ ratings", costs like "₹ 200"
  - cities like "Koramangala, Bangalore" (city after the last comma)

The three fact files (orders, order_items, reviews) are clean and typed.

Usage:
    python local/generate_data.py                 # default size (~50K orders)
    python local/generate_data.py --orders 200000 # bigger run
"""
import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAKE = ROOT / "data" / "lake" / "raw"

CITIES = {
    "Bangalore": ["Koramangala", "Indiranagar", "HSR Layout", "Whitefield", "Jayanagar", "BTM Layout"],
    "Mumbai": ["Andheri West", "Bandra", "Powai", "Lower Parel", "Colaba", "Malad"],
    "Delhi": ["Connaught Place", "Hauz Khas", "Saket", "Rajouri Garden", "Karol Bagh"],
    "Hyderabad": ["Gachibowli", "Banjara Hills", "Madhapur", "Kondapur", "Jubilee Hills"],
    "Pune": ["Koregaon Park", "Baner", "Viman Nagar", "Kothrud", "Hinjewadi"],
    "Chennai": ["T Nagar", "Adyar", "Velachery", "Anna Nagar", "OMR"],
    "Kolkata": ["Park Street", "Salt Lake", "New Town", "Ballygunge"],
    "Ahmedabad": ["Navrangpura", "Satellite", "Bodakdev", "Prahlad Nagar"],
    "Jaipur": ["Malviya Nagar", "C Scheme", "Vaishali Nagar"],
    "Lucknow": ["Hazratganj", "Gomti Nagar", "Aliganj"],
}
CITY_WEIGHTS = [18, 17, 15, 12, 10, 9, 7, 5, 4, 3]

CUISINES = {
    "North Indian": ["Paneer Butter Masala", "Dal Makhani", "Butter Naan", "Chole Bhature", "Rajma Chawal",
                     "Butter Chicken", "Kadai Paneer"],
    "South Indian": ["Masala Dosa", "Idli Sambar", "Medu Vada", "Uttapam", "Rava Dosa", "Curd Rice"],
    "Chinese": ["Veg Hakka Noodles", "Chilli Chicken", "Veg Manchurian", "Fried Rice", "Spring Rolls",
                "Schezwan Noodles"],
    "Biryani": ["Chicken Biryani", "Mutton Biryani", "Veg Biryani", "Egg Biryani", "Hyderabadi Dum Biryani"],
    "Pizzas": ["Margherita Pizza", "Farmhouse Pizza", "Pepperoni Pizza", "Paneer Tikka Pizza", "Garlic Bread"],
    "Burgers": ["Aloo Tikki Burger", "Chicken Burger", "Veg Cheese Burger", "French Fries", "Peri Peri Fries"],
    "Desserts": ["Gulab Jamun", "Rasmalai", "Chocolate Brownie", "Kulfi", "Gajar Halwa"],
    "Street Food": ["Pav Bhaji", "Vada Pav", "Pani Puri", "Samosa", "Kathi Roll"],
    "Beverages": ["Masala Chai", "Cold Coffee", "Mango Lassi", "Fresh Lime Soda"],
    "Healthy Food": ["Quinoa Salad", "Grilled Chicken Bowl", "Fruit Bowl", "Sprouts Chaat"],
}
NON_VEG_WORDS = ("Chicken", "Mutton", "Egg", "Pepperoni", "Fish", "Prawn")
# price level per cuisine (multiplies the base menu price) so cuisines differ in order value
PRICE_LEVEL = {"North Indian": 1.15, "South Indian": 0.75, "Chinese": 1.0, "Biryani": 1.35, "Pizzas": 1.5,
               "Burgers": 0.95, "Desserts": 0.7, "Street Food": 0.55, "Beverages": 0.5, "Healthy Food": 1.25}

NAME_PREFIX = ["Spice", "Royal", "Tandoor", "Urban", "Desi", "Chai", "Biryani", "Masala", "Curry", "Dosa",
               "Punjabi", "Bombay", "Madras", "Golden", "Green", "Food", "Tasty", "Flavours of", "The"]
NAME_SUFFIX = ["Kitchen", "House", "Junction", "Point", "Corner", "Express", "Hub", "Cafe", "Dhaba",
               "Palace", "Bites", "Box", "Story", "Garden", "Treats"]
FIRST = ["Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Krishna", "Ishaan", "Rohan",
         "Ananya", "Diya", "Saanvi", "Aadhya", "Pari", "Anika", "Navya", "Myra", "Riya", "Kavya",
         "Rahul", "Priya", "Amit", "Neha", "Vikram", "Pooja", "Karan", "Sneha", "Manish", "Divya"]
LAST = ["Sharma", "Verma", "Gupta", "Iyer", "Reddy", "Nair", "Patel", "Singh", "Kumar", "Das",
        "Mehta", "Joshi", "Rao", "Banerjee", "Chopra", "Kapoor", "Agarwal", "Pillai", "Shah", "Malhotra"]
OCCUPATIONS = ["Student", "Employee", "Self Employeed", "House wife"]
INCOME = ["No Income", "Below Rs.10000", "10001 to 25000", "25001 to 50000", "More than 50000"]
EDUCATION = ["Graduate", "Post Graduate", "Ph.D", "School", "Uneducated"]
MARITAL = ["Single", "Married", "Prefer not to say"]
PAYMENTS = ["UPI", "Card", "Cash on Delivery", "Wallet", "Net Banking"]
PAYMENT_W = [52, 20, 14, 10, 4]

# Review text building blocks: (topic, sentiment) -> phrases
REVIEW_BANK = {
    ("food quality", "positive"): ["The {dish} was absolutely delicious and full of flavour.",
                                   "Loved the {dish}, tasted fresh and homely.",
                                   "Best {dish} I have had in {city} in a long time!",
                                   "Portion size was great and the {dish} was perfectly cooked."],
    ("food quality", "negative"): ["The {dish} was cold and tasted stale.",
                                   "{dish} was too oily and way too salty.",
                                   "Found the {dish} undercooked, really disappointed.",
                                   "Quality of the {dish} has gone down a lot recently."],
    ("delivery", "positive"): ["Delivered in under 25 minutes, super quick!",
                               "Delivery partner was polite and on time.",
                               "Fast delivery even during peak hours, impressed."],
    ("delivery", "negative"): ["Order arrived 50 minutes late and the food was cold.",
                               "Delivery took forever, the rider kept going the wrong way.",
                               "Very late delivery, nobody updated me on the delay.",
                               "Rider marked it delivered but I got it 30 minutes later."],
    ("pricing", "positive"): ["Great value for money, the combo offer was worth it.",
                              "Reasonably priced for the quantity you get."],
    ("pricing", "negative"): ["Way too expensive for such a small portion.",
                              "Prices have gone up a lot, not worth it anymore.",
                              "Delivery fee and taxes made it very costly."],
    ("service", "positive"): ["Customer support resolved my issue quickly.",
                              "Restaurant added extra chutney on request, nice gesture."],
    ("service", "negative"): ["Wrong item delivered and support was not helpful.",
                              "Missing items in my order and no refund yet.",
                              "Restaurant ignored my special instructions completely."],
    ("packaging", "positive"): ["Neat packaging, nothing spilled at all.",
                                "Eco-friendly packaging and well sealed containers."],
    ("packaging", "negative"): ["Packaging was poor and the gravy leaked everywhere.",
                                "The container was broken and the food spilled in the bag."],
    ("other", "neutral"): ["It was okay, nothing special.",
                           "Average experience overall.",
                           "Food was fine, ordered because of the discount.",
                           "Decent, might order again."],
}


def weighted_city(rng):
    return rng.choices(list(CITIES), weights=CITY_WEIGHTS, k=1)[0]


def write_csv(table, header, rows):
    folder = LAKE / table
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{table}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)
        w.writerows(rows)
    return path


def main():
    p = argparse.ArgumentParser(description="Generate the Zomato-style dataset into data/lake/raw/")
    p.add_argument("--restaurants", type=int, default=1500)
    p.add_argument("--users", type=int, default=8000)
    p.add_argument("--orders", type=int, default=50000)
    p.add_argument("--reviews", type=int, default=6000)
    p.add_argument("--start", default="2025-01-01", help="first order date (YYYY-MM-DD)")
    p.add_argument("--days", type=int, default=365, help="number of days of orders")
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    rng = random.Random(a.seed)

    # ---------- food (dimension) ----------
    foods, food_rows = [], []
    fid = 0
    for cuisine, dishes in CUISINES.items():
        for dish in dishes:
            fid += 1
            f_id = f"fd{fid}"
            veg = "non-veg" if any(w in dish for w in NON_VEG_WORDS) else "veg"
            foods.append((f_id, dish, cuisine))
            food_rows.append([fid - 1, f_id, dish, veg])
    write_csv("food", ["", "f_id", "item", "veg_or_non_veg"], food_rows)
    foods_by_cuisine = {}
    for f_id, dish, cuisine in foods:
        foods_by_cuisine.setdefault(cuisine, []).append((f_id, dish))

    # ---------- restaurants (messy dimension) ----------
    restaurants, rest_rows = [], []
    for i in range(a.restaurants):
        r_id = 200000 + i
        city = weighted_city(rng)
        area = rng.choice(CITIES[city])
        cuisines = rng.sample(list(CUISINES), k=rng.choice([1, 1, 2]))
        name = f"{rng.choice(NAME_PREFIX)} {rng.choice(NAME_SUFFIX)}"
        rating = "--" if rng.random() < 0.12 else f"{rng.uniform(2.8, 4.9):.1f}"
        rc = rng.choice(["Too Few Ratings", "20+ ratings", "50+ ratings", "100+ ratings", "500+ ratings",
                         "1K+ ratings"])
        cost = rng.choice([150, 200, 250, 300, 350, 400, 500, 600, 800, 1000])
        restaurants.append({"r_id": r_id, "city": city, "area": area, "cuisines": cuisines,
                            "quality": rng.uniform(-0.6, 0.6)})
        rest_rows.append([i, r_id, name, f"{area}, {city}", rating, rc, f"₹ {cost}", ",".join(cuisines),
                          f"1{rng.randint(10**12, 10**13 - 1)}",
                          f"https://www.zomato.com/{city.lower()}/r{r_id}",
                          f"{rng.randint(1, 300)}, {area}, {city}", "Menu"])
    write_csv("restaurants", ["", "id", "name", "city", "rating", "rating_count", "cost", "cuisine",
                              "lic_no", "link", "address", "menu"], rest_rows)

    # ---------- menu (dimension) ----------
    menu_rows, menu_by_rest = [], {}
    mid = 0
    for r in restaurants:
        items = []
        for c in r["cuisines"]:
            k = min(len(foods_by_cuisine[c]), rng.randint(3, 6))
            for f_id, dish in rng.sample(foods_by_cuisine[c], k=k):
                base = rng.choice([79, 99, 129, 149, 179, 199, 229, 249, 279, 299, 349, 399])
                price = int(round(base * PRICE_LEVEL[c] / 10) * 10 - 1)   # e.g. 149, 299 - menu-style prices
                items.append((f_id, dish, c, price))
                menu_rows.append([mid, f"mn{mid}", r["r_id"], f_id, c, f"{price}.0"])
                mid += 1
        menu_by_rest[r["r_id"]] = items
    # one broken menu row the staging layer must filter out
    menu_rows.append([mid, f"mn{mid}", "unknown", "fd1", "North Indian", "abc"])
    write_csv("menu", ["", "menu_id", "r_id", "f_id", "cuisine", "price"], menu_rows)

    # ---------- users (dimension) ----------
    user_rows = []
    for i in range(a.users):
        uid = i + 1
        fn, ln = rng.choice(FIRST), rng.choice(LAST)
        user_rows.append([i, uid, f"{fn} {ln}", f"{fn}.{ln}{uid}@Example.com", "********",
                          rng.choice([None] + list(range(18, 66))), rng.choice(["Male", "Female"]),
                          rng.choice(MARITAL), rng.choice(OCCUPATIONS), rng.choice(INCOME),
                          rng.choice(EDUCATION), rng.randint(1, 6)])
    write_csv("users", ["", "user_id", "name", "email", "password", "Age", "Gender", "Marital Status",
                        "Occupation", "Monthly Income", "Educational Qualifications", "Family size"], user_rows)

    # ---------- orders + order_items (facts) ----------
    start = datetime.fromisoformat(a.start)
    hour_weights = [1, 1, 1, 1, 1, 1, 2, 4, 6, 6, 5, 7, 12, 14, 10, 6, 5, 7, 10, 15, 16, 13, 8, 3]
    city_speed = {c: rng.uniform(-4, 6) for c in CITIES}
    orders, items, order_meta = [], [], []
    oi_id = 0
    for i in range(a.orders):
        order_id = 1_000_000 + i
        r = rng.choice(restaurants)
        menu = menu_by_rest[r["r_id"]]
        day = start + timedelta(days=int((i / a.orders) * a.days))   # order_id grows with time
        ts = day + timedelta(hours=rng.choices(range(24), weights=hour_weights)[0],
                             minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))
        n_items = rng.choices([1, 2, 3, 4], weights=[45, 33, 15, 7])[0]
        picked = rng.sample(menu, k=min(n_items, len(menu)))
        subtotal, qty_total = 0, 0
        for f_id, dish, c, price in picked:
            q = rng.choices([1, 2, 3], weights=[75, 20, 5])[0]
            oi_id += 1
            items.append([oi_id, order_id, r["r_id"], f_id, price, q, price * q])
            subtotal += price * q
            qty_total += q
        discount = rng.choice([0, 0, 0, 20, 40, 50, 75, 100]) if subtotal > 200 else 0
        delivery_fee = rng.choice([0, 25, 30, 40, 49])
        gst = round((subtotal - discount) * 0.05)
        sales = subtotal - discount + delivery_fee + gst
        status = rng.choices(["Delivered", "Cancelled", "Refunded"], weights=[90, 7, 3])[0]
        peak = ts.hour in (12, 13, 19, 20, 21)
        dt = max(12, round(rng.gauss(30 + city_speed[r["city"]] + (8 if peak else 0), 7)))
        rating = None
        if status == "Delivered":
            rating = max(1, min(5, round(rng.gauss(4.1 + r["quality"] - (0.8 if dt > 45 else 0), 0.8))))
        user_id = rng.randint(1, a.users)
        orders.append([order_id, ts.strftime("%Y-%m-%d %H:%M:%S"), ts.strftime("%Y-%m-%d"), user_id,
                       r["r_id"], f"{r['area']}, {r['city']}", picked[0][2], len(picked), qty_total, subtotal,
                       discount, delivery_fee, gst, sales, "INR", rng.choices(PAYMENTS, weights=PAYMENT_W)[0],
                       status, rating if rating is not None else "", dt if status == "Delivered" else ""])
        order_meta.append((order_id, user_id, r, picked[0][1], ts, status, dt, rating))
    write_csv("orders", ["order_id", "order_timestamp", "order_date", "user_id", "r_id", "restaurant_city",
                         "cuisine", "items_count", "sales_qty", "subtotal", "discount", "delivery_fee", "gst",
                         "sales_amount", "currency", "payment_method", "order_status", "customer_rating",
                         "delivery_time_min"], orders)
    write_csv("order_items", ["order_item_id", "order_id", "r_id", "f_id", "price", "quantity", "line_amount"],
              items)

    # ---------- reviews (free text for the AI layer) ----------
    delivered = [m for m in order_meta if m[5] == "Delivered"]
    sample = rng.sample(delivered, k=min(a.reviews, len(delivered)))
    review_rows = []
    for i, (order_id, user_id, r, dish, ts, status, dt, rating) in enumerate(sample):
        if rating >= 4:
            sentiment = "positive"
        elif rating <= 2:
            sentiment = "negative"
        else:
            sentiment = rng.choice(["neutral", "neutral", "negative", "positive"])
        if sentiment == "neutral":
            topic = "other"
        elif sentiment == "negative" and dt > 45:
            topic = "delivery"
        else:
            topic = rng.choices(["food quality", "delivery", "pricing", "service", "packaging"],
                                weights=[40, 20, 15, 15, 10])[0]
        text = rng.choice(REVIEW_BANK[(topic, sentiment)]).format(dish=dish, city=r["city"])
        if rng.random() < 0.35:   # a second sentence makes retrieval more interesting
            t2 = rng.choice([k for k in REVIEW_BANK if k[1] == sentiment])
            text += " " + rng.choice(REVIEW_BANK[t2]).format(dish=dish, city=r["city"])
        review_rows.append([i + 1, order_id, user_id, r["r_id"], rating, text,
                            (ts + timedelta(days=rng.randint(0, 3))).strftime("%Y-%m-%d")])
    write_csv("reviews", ["review_id", "order_id", "user_id", "restaurant_id", "rating", "comment", "review_date"],
              review_rows)

    print(f"Wrote dataset to {LAKE}")
    for t, n in [("restaurants", len(rest_rows)), ("users", len(user_rows)), ("food", len(food_rows)),
                 ("menu", len(menu_rows)), ("orders", len(orders)), ("order_items", len(items)),
                 ("reviews", len(review_rows))]:
        print(f"  {t:<12} {n:>9,} rows")


if __name__ == "__main__":
    main()

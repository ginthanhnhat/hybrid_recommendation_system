import os
import pandas as pd
import json
from tqdm import tqdm
import copy

def process_metadata(category_name, input_folder, output_folder, unique_users, unique_items, price_summary, type_of_price):
    """
    Process metadata and training data for a single category.
    """
    train_file = os.path.join(input_folder, "train", f"{category_name}.csv")
    meta_file = os.path.join(input_folder, "meta", f"meta_{category_name}.jsonl")

    output_train = os.path.join(output_folder, "train", f"{category_name}.csv")
    output_meta = os.path.join(output_folder, "meta", f"meta_{category_name}.json")
    output_filtered = os.path.join(output_folder, "filtered", f"filtered_{category_name}.json")
    output_user_file = os.path.join(output_folder, "user", f"user_{category_name}.json")
    output_item_file = os.path.join(output_folder, "item", f"item_{category_name}.json")

    os.makedirs(output_folder, exist_ok=True)
    for subfolder in ["train", "meta", "filtered", "user", "item"]:
        os.makedirs(os.path.join(output_folder, subfolder), exist_ok=True)

    train_df = pd.read_csv(train_file)
    train_asins = set(train_df["parent_asin"].unique())

    unique_users.update(set(train_df["user_id"].unique()))
    unique_items.update(train_asins)

    with open(output_user_file, "w") as f:
        json.dump(list(unique_users), f)
    with open(output_item_file, "w") as f:
        json.dump(list(unique_items), f)

    meta_data, filtered_data, prices = [], [], []

    with open(meta_file, "r") as meta_f:
        for line in tqdm(meta_f, desc=f"Processing meta {category_name}"):
            data = json.loads(line)
            parent_asin = data.get("parent_asin")

            if parent_asin in train_asins:
                meta_data.append(data)

                price = data.get("price")
                if isinstance(price, str):
                    if price.startswith("from "):
                        try:
                            data["price"] = float(price.split("from ")[1])
                        except ValueError:
                            data["price"] = None
                    else:
                        try:
                            data["price"] = float(price)
                        except ValueError:
                            data["price"] = None
                            type_of_price.add(price)

                if data["price"] is not None:
                    prices.append(data["price"])

                if data.get("title") and data.get("images", []):
                    filtered_data.append(copy.deepcopy(data))

    average_price = sum(prices) / len(prices) if prices else 0
    if average_price == 0:
        price_summary["categories_with_no_price"].append(category_name)

    price_summary["category_avg_prices"][category_name] = average_price

    for item in filtered_data:
        if item.get("price") is None:
            item["price"] = 0 if average_price == 0 else average_price

    with open(output_meta, "w") as out_meta:
        json.dump(meta_data, out_meta, indent=4)
    with open(output_filtered, "w") as out_filtered:
        json.dump(filtered_data, out_filtered, indent=4)

    train_df_filtered = train_df[train_df["parent_asin"].isin(train_asins)]
    train_df_filtered.to_csv(output_train, index=False)

    print(f"\nMetadata for category {category_name} processed successfully.\n")


if __name__ == "__main__":
    categories = ["Unknown"]
    input_folder, output_folder = "input", "output"
    unique_users, unique_items = set(), set()

    price_summary_file = os.path.join(output_folder, "price_summary.json")
    type_of_price_file = os.path.join(output_folder, "type_of_price.json")

    if os.path.exists(type_of_price_file):
        with open(type_of_price_file, "r") as f:
            type_of_price = set(json.load(f))
    else:
        type_of_price = set()

    if os.path.exists(price_summary_file):
        with open(price_summary_file, "r") as f:
            price_summary = json.load(f)
    else:
        price_summary = {"categories_with_no_price": [], "category_avg_prices": {}}

    for category in categories:
        process_metadata(category, input_folder, output_folder, unique_users, unique_items, price_summary, type_of_price)

    all_prices = [price for price in price_summary["category_avg_prices"].values() if price > 0]
    price_summary["avg_prices_all_category"] = sum(all_prices) / len(all_prices) if all_prices else 0

    with open(price_summary_file, "w") as f:
        json.dump(price_summary, f, indent=4)

    with open(type_of_price_file, "w") as f:
        json.dump(list(type_of_price), f, indent=4)
        
    print(f"Saved {len(unique_users)} unique user IDs.")
    print(f"Saved {len(unique_items)} unique item IDs.")

    print(f"\nMetadata processing completed.\n")

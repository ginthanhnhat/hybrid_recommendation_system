import os
import json
import argparse
import random
import pandas as pd
import numpy as np
from tqdm import tqdm
from collections import defaultdict

# ==== Build mapping từ user_id và item_id gốc sang số nguyên ====
def build_id_maps(df_list):
    all_users = set()
    all_items = set()
    for df in df_list:
        all_users.update(df['user_id'].unique())
        all_items.update(df['parent_asin'].unique())

    user2id = {u: i for i, u in enumerate(sorted(all_users))}
    item2id = {i: j for j, i in enumerate(sorted(all_items))}
    return user2id, item2id

# ==== Convert user_id và item_id sang ID dạng số nguyên ====
def convert_dataframe(df, user2id, item2id):
    df = df[df['user_id'].isin(user2id)]
    df = df[df['parent_asin'].isin(item2id)]
    df['user'] = df['user_id'].map(user2id)
    df['item'] = df['parent_asin'].map(item2id)
    return df[['user', 'item']]

# ==== Chia train/test theo kiểu leave-last-one-out ====
def split_train_test(df, shuffle=True):
    if shuffle:
        df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    user_group = df.groupby('user')
    train_data = []
    test_data = []

    for user, group in user_group:
        items = group['item'].tolist()
        if len(items) < 2:
            continue
        train_items = items[:-1]
        test_item = items[-1]
        for item in train_items:
            train_data.append((user, item))
        test_data.append((user, test_item))

    return train_data, test_data

# ==== Sinh negative sample cho test ====
def generate_negative_samples(train_data, test_data, all_items, num_negatives=99):
    interacted = defaultdict(set)
    for u, i in train_data:
        interacted[u].add(i)
    for u, i in test_data:
        interacted[u].add(i)

    all_items_array = np.array(list(all_items))
    test_negative = []
    
    for u, pos_i in tqdm(test_data, desc="Generating negatives"):
        user_items = interacted[u]
        mask = np.isin(all_items_array, list(user_items), invert=True)
        negatives_pool = all_items_array[mask]
        if len(negatives_pool) < num_negatives:
            negatives = negatives_pool.tolist()
        else:
            negatives = np.random.choice(negatives_pool, size=num_negatives, replace=False).tolist()
        test_negative.append((u, pos_i, negatives))
    return test_negative

# ==== Lưu kết quả ra từng file theo category ====
def save_output_per_category(cate_name, train_data, test_data, test_negative, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    with open(os.path.join(output_dir, f"{cate_name}.train.rating"), "w") as f:
        for u, i in train_data:
            f.write(f"{u}\t{i}\n")

    with open(os.path.join(output_dir, f"{cate_name}.test.rating"), "w") as f:
        for u, i in test_data:
            f.write(f"{u}\t{i}\n")

    with open(os.path.join(output_dir, f"{cate_name}.test.negative"), "w") as f:
        for u, pos_i, negatives in test_negative:
            line = f"{u}\t{pos_i}" + "".join([f"\t{neg_i}" for neg_i in negatives]) + "\n"
            f.write(line)

# ==== Lưu ánh xạ ID gốc <-> ID số nguyên ====
def save_mappings(user2id, item2id, output_dir, cate_name):
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, f"{cate_name}_user2id.json"), "w") as f:
        json.dump(user2id, f, indent=2)
    with open(os.path.join(output_dir, f"{cate_name}_item2id.json"), "w") as f:
        json.dump(item2id, f, indent=2)

# ==== Load từng file CSV trong thư mục input/category_name/category_name.csv ====
def load_csv_file(input_dir, cate):
    cate_path = os.path.join(input_dir, cate, f"{cate}.csv")
    return pd.read_csv(cate_path)

# ==== Pipeline xử lý cho từng category ====
def preprocess_category(input_dir, output_dir, cate_name, shuffle):
    print(f"\n🚀 Processing category: {cate_name}")

    df = load_csv_file(input_dir, cate_name)

    user2id, item2id = build_id_maps([df])
    converted_df = convert_dataframe(df, user2id, item2id)
    train_data, test_data = split_train_test(converted_df, shuffle=shuffle)
    all_items = set(item2id.values())
    test_negative = generate_negative_samples(train_data, test_data, all_items)

    category_output_dir = os.path.join(output_dir, cate_name)
    save_output_per_category(cate_name, train_data, test_data, test_negative, category_output_dir)
    save_mappings(user2id, item2id, category_output_dir, cate_name)

    print(f"✅ Done with category: {cate_name}")

# ==== Hàm chính xử lý toàn bộ ====
def main(input_dir, output_dir, categories, shuffle):
    for cate_name in categories:
        preprocess_category(input_dir, output_dir, cate_name, shuffle)

# ==== Gọi script từ dòng lệnh ====
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", default="../data/input", help="Thư mục chứa dữ liệu đầu vào")
    parser.add_argument("--output_dir", default="../data/output", help="Thư mục lưu dữ liệu đầu ra")
    parser.add_argument("--categories", nargs='+', required=True, help="Danh sách tên category để xử lý")
    parser.add_argument("--shuffle", action="store_true", help="Có shuffle trước khi chia train/test không")
    args = parser.parse_args()

    main(args.input_dir, args.output_dir, args.categories, args.shuffle)

# === RUN ===: python preprocess.py --input_dir data/input --output_dir data/output --categories Gift_Cards Cell_Phones_and_Accessories --shuffle
# python preprocess.py --categories Gift_Cards Cell_Phones_and_Accessories --shuffle
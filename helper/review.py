import os
import json
from tqdm import tqdm

def process_reviews(category_name, input_folder, output_folder):
    """
    Process reviews for a single category efficiently using chunk processing.
    This avoids memory overload by writing output incrementally.

    :param category_name: The category name to process.
    :param input_folder: The folder containing input data.
    :param output_folder: The folder to store processed output.
    :param chunk_size: Number of lines to process before writing to file.
    """
    train_file = os.path.join(input_folder, "train", f"{category_name}.csv")
    
    # review_file = os.path.join(input_folder, "review", f"{category_name}.jsonl")
    # output_review = os.path.join(output_folder, "review", f"{category_name}.json")
    
    # For large files - add _part1, _part2, etc... to the end of the file
    review_file = os.path.join(input_folder, "review", f"{category_name}_part3.jsonl")
    
    output_review = os.path.join(output_folder, "review", f"{category_name}_part3.json")

    os.makedirs(os.path.join(output_folder, "review"), exist_ok=True)

    # Step 1: Load train ASINs into a set for fast lookup
    train_asins = set()
    if os.path.exists(train_file):
        with open(train_file, "r") as train_f:
            next(train_f)  # Skip header
            for line in train_f:
                parts = line.strip().split(",")
                if len(parts) > 1:
                    train_asins.add(parts[1])  # parent_asin column

    # Step 2: Process reviews with chunking to avoid memory overload
    with open(review_file, "r") as review_f, open(output_review, "w") as out_review:
        out_review.write("[")  # Start JSON array
        first_item = True  # To handle commas correctly

        for line in tqdm(review_f, desc=f"Processing reviews {category_name}"):
            data = json.loads(line)
            if data.get("parent_asin") in train_asins:
                if not first_item:
                    out_review.write(",\n")  # Add comma between JSON objects
                json.dump(data, out_review, indent=4)
                first_item = False

        out_review.write("\n]")  # Close JSON array
    
    print(f"Reviews for category {category_name} processed successfully.\n")

if __name__ == "__main__":
    categories = ["Home_and_Kitchen"]  # Modify as needed
    input_folder, output_folder = "input", "output"
    

    for category in categories:
        process_reviews(category, input_folder, output_folder)
    
    print(f"Review processing completed.\n")

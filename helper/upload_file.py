from huggingface_hub import HfApi
import os

dataset_repo = "GinDev/Amazon-Reviews-2023-Recommendation"  # Thay bằng repo của bạn
review_folder = "output/review"  # Thư mục chứa các file cần upload

# Khởi tạo API
api = HfApi()

# Kiểm tra và upload từng file trong thư mục review
for file_name in os.listdir(review_folder):
    file_path = os.path.join(review_folder, file_name)

    if os.path.isfile(file_path):
        print(f"🔄 Uploading: {file_name} ...")

        api.upload_file(
            path_or_fileobj=file_path,
            path_in_repo=f"review/{file_name}",  # Chỉ lấy tên file, không lấy thư mục local
            repo_id=dataset_repo,
            repo_type="dataset"
        )

        print(f"✅ Uploaded: {file_name}")

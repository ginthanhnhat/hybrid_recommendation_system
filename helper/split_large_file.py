import os

def split_file(input_file, output_folder, num_parts=3, buffer_size=10000):
    """
    Tách file JSONL lớn thành num_parts phần bằng nhau.
    - input_file: file gốc (JSONL).
    - output_folder: thư mục chứa file tách.
    - num_parts: số phần cần tách (mặc định = 3).
    - buffer_size: số dòng ghi mỗi lần (giúp tăng tốc độ).
    """
    print(f"🔄 Đang đếm số dòng trong {input_file}...")

    # Đếm tổng số dòng trong file gốc
    with open(input_file, "r", encoding="utf-8") as f:
        total_lines = sum(1 for _ in f)

    split_size = total_lines // num_parts  # Số dòng mỗi phần
    print(f"✅ Tổng số dòng: {total_lines}, mỗi phần: ~{split_size} dòng.")

    # Mở tất cả file output trước để tránh mở/đóng nhiều lần
    output_files = [
        open(os.path.join(output_folder, f"Home_and_Kitchen_part{i+1}.jsonl"), "w", encoding="utf-8")
        for i in range(num_parts)
    ]

    print(f"✂️  Bắt đầu tách file...")
    
    with open(input_file, "r", encoding="utf-8") as infile:
        buffer = [[] for _ in range(num_parts)]  # Mảng chứa buffer cho từng file
        for i, line in enumerate(infile):
            part_index = min(i // split_size, num_parts - 1)  # Xác định file tương ứng
            buffer[part_index].append(line)

            # Ghi batch nếu đủ buffer_size
            if len(buffer[part_index]) >= buffer_size:
                output_files[part_index].writelines(buffer[part_index])
                buffer[part_index].clear()

        # Ghi nốt dữ liệu còn lại
        for j in range(num_parts):
            if buffer[j]:
                output_files[j].writelines(buffer[j])

    # Đóng tất cả file output
    for f in output_files:
        f.close()

    print(f"✅ Tách file hoàn tất. Đã tạo:")
    for i in range(num_parts):
        print(f"   📂 Home_and_Kitchen_part{i+1}.jsonl")

if __name__ == "__main__":
    category = "Home_and_Kitchen"
    input_folder = "input"
    output_folder = os.path.join(input_folder, "review")

    input_file = os.path.join(output_folder, f"{category}.jsonl")

    split_file(input_file, output_folder)

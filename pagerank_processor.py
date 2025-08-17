# pagerank_processor.py
# Tác vụ Spark để tính toán PageRank từ dữ liệu liên kết do crawler thu thập.

import sys
import json
from pyspark.sql import SparkSession


def main():
    """
    Hàm chính để chạy tác vụ Spark.
    """
    # --- Khởi tạo Spark Session ---
    spark = SparkSession.builder.appName("PageRankCalculator").getOrCreate()
    sc = spark.sparkContext

    print("\n[Spark] Spark Session đã được khởi tạo.")

    # --- Đường dẫn dữ liệu ---
    # Đường dẫn này tương ứng với volume được mount trong docker-compose
    link_data_path = "/opt/bitnami/spark/data/links/*.txt"
    output_path = "/opt/bitnami/spark/data/pagerank_scores/pagerank_scores.json"

    try:
        # --- Đọc dữ liệu và xây dựng đồ thị ---
        # Đọc tất cả các file .txt trong thư mục links
        # Mỗi dòng có dạng "source_url destination_url"
        lines = sc.textFile(link_data_path)

        # Chuyển đổi mỗi dòng thành một cặp (source, destination)
        links = lines.map(lambda url: tuple(url.split())).distinct()

        # Xây dựng cấu trúc đồ thị: (source, [list_of_destinations])
        links_grouped = links.groupByKey().mapValues(list)

        # Lấy tất cả các trang (nodes) trong đồ thị
        nodes = links.flatMap(lambda x: x).distinct()

        # Khởi tạo điểm PageRank ban đầu cho tất cả các trang là 1.0
        ranks = nodes.map(lambda url: (url, 1.0))

        num_nodes = nodes.count()
        if num_nodes == 0:
            print("[Spark !] Không tìm thấy dữ liệu liên kết để xử lý.")
            spark.stop()
            return

        print(f"\n[Spark] Đã xây dựng đồ thị với {num_nodes} trang và {links.count()} liên kết.")

        # --- Chạy thuật toán PageRank ---
        # Số lần lặp
        iterations = 15
        damping_factor = 0.85

        print(f"[Spark] Bắt đầu tính toán PageRank với {iterations} lần lặp...")

        for i in range(iterations):
            # Join ranks với cấu trúc đồ thị để có (source, (rank, [dests]))
            contribs = links_grouped.join(ranks).flatMap(
                lambda x: [(dest, x[1][0] / len(x[1][1])) for dest in x[1][1]]
            )

            # Tính lại rank mới dựa trên công thức PageRank
            # new_rank = (1 - d) + d * sum(incoming_ranks)
            # Vì ta có N node, (1-d) sẽ được phân bổ đều, nhưng để đơn giản ta cộng vào sau
            # Ở đây ta tính tổng đóng góp từ các link trỏ tới
            ranks_sum = contribs.reduceByKey(lambda x, y: x + y)

            # Áp dụng damping factor
            new_ranks = ranks_sum.mapValues(lambda rank: (1 - damping_factor) + damping_factor * rank)

            # Gán lại ranks cho lần lặp tiếp theo
            ranks = new_ranks
            print(f"  - Hoàn thành lần lặp {i + 1}/{iterations}")

        # --- Thu thập và lưu kết quả ---
        print("\n[Spark] Đang thu thập kết quả PageRank...")

        # Thu thập kết quả về driver node
        final_ranks = ranks.collect()

        # Chuyển đổi thành dạng dictionary {url: score}
        pagerank_dict = {url: rank for url, rank in final_ranks}

        print(f"[Spark] Đã tính toán xong PageRank cho {len(pagerank_dict)} trang.")

        # Lưu vào file JSON
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(pagerank_dict, f, ensure_ascii=False, indent=4)
            print(f"\n[Spark ✔] Đã lưu kết quả PageRank thành công vào '{output_path}'")
        except IOError as e:
            print(f"\n[Spark !] Lỗi khi lưu file: {e}")

    except Exception as e:
        print(f"\n[Spark !] Đã xảy ra lỗi trong quá trình xử lý: {e}")

    finally:
        # Dừng Spark Session
        spark.stop()
        print("[Spark] Spark Session đã được đóng.")


if __name__ == "__main__":
    main()

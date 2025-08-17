# indexer_worker.py (Phiên bản nâng cấp cho PageRank)
# Worker này đọc nội dung từ RabbitMQ, tra cứu điểm PageRank đã được
# tính toán bởi Spark, và đẩy tài liệu hoàn chỉnh vào Elasticsearch.

import pika
import sys
import json
import os
from elasticsearch import Elasticsearch, ApiError
import time

# --- Cấu hình ---
RABBITMQ_HOST = 'rabbitmq'
ELASTICSEARCH_HOST = 'http://elasticsearch:9200'
DATA_QUEUE = 'content_for_indexing'
INDEX_NAME = 'web_pages_ranked'  # Tên index mới
PAGERANK_SCORES_PATH = '/opt/bitnami/spark/data/pagerank_scores/pagerank_scores.json'

# --- Biến toàn cục để cache điểm PageRank ---
PAGERANK_SCORES = {}
LAST_MODIFIED_TIME = 0


def connect_to_elasticsearch():
    """Kết nối đến Elasticsearch với cơ chế thử lại."""
    es_client = None
    for i in range(5):
        try:
            es_client = Elasticsearch(ELASTICSEARCH_HOST)
            if es_client.ping():
                print("[Indexer] Đã kết nối đến Elasticsearch.")
                return es_client
        except Exception as e:
            print(f"[Indexer !] Không thể kết nối đến Elasticsearch, thử lại sau 5s... ({e})")
            time.sleep(5)
    print("[Indexer LỖI] Không thể kết nối đến Elasticsearch sau nhiều lần thử.", file=sys.stderr)
    sys.exit(1)


es_client = connect_to_elasticsearch()


def load_pagerank_scores():
    """
    Tải hoặc tải lại điểm PageRank từ file nếu file đã được cập nhật.
    """
    global PAGERANK_SCORES, LAST_MODIFIED_TIME
    try:
        if os.path.exists(PAGERANK_SCORES_PATH):
            modified_time = os.path.getmtime(PAGERANK_SCORES_PATH)
            if modified_time > LAST_MODIFIED_TIME:
                print("[Indexer] Phát hiện file PageRank mới. Đang tải lại...")
                with open(PAGERANK_SCORES_PATH, 'r', encoding='utf-8') as f:
                    PAGERANK_SCORES = json.load(f)
                LAST_MODIFIED_TIME = modified_time
                print(f"[Indexer] Đã tải {len(PAGERANK_SCORES)} điểm PageRank.")
    except (IOError, json.JSONDecodeError) as e:
        print(f"[Indexer !] Lỗi khi tải file PageRank: {e}")


def create_index_if_not_exists():
    """Tạo index trong Elasticsearch nếu nó chưa tồn tại."""
    try:
        if not es_client.indices.exists(index=INDEX_NAME):
            settings = {"analysis": {"analyzer": {"default": {"type": "standard"}}}}
            mappings = {
                "properties": {
                    "url": {"type": "keyword"},
                    "title": {"type": "text", "analyzer": "standard"},
                    "content": {"type": "text", "analyzer": "standard"},
                    "pagerank_score": {"type": "rank_feature"}  # Sử dụng rank_feature để tăng hiệu quả xếp hạng
                }
            }
            es_client.indices.create(index=INDEX_NAME, mappings=mappings, settings=settings)
            print(f"[Indexer] Đã tạo index '{INDEX_NAME}'.")
    except ApiError as e:
        print(f"[Indexer LỖI] Không thể tạo index '{INDEX_NAME}': {e}", file=sys.stderr)


def callback(ch, method, properties, body):
    """Hàm xử lý mỗi khi nhận được một trang để đánh chỉ mục."""
    load_pagerank_scores()  # Kiểm tra và tải lại điểm PageRank nếu cần

    try:
        data = json.loads(body.decode('utf-8'))
        url = data.get('url')

        # Lấy điểm PageRank, nếu không có thì dùng giá trị mặc định rất nhỏ
        pagerank_score = PAGERANK_SCORES.get(url, 0.15)  # 0.15 là giá trị (1-d)

        doc = {
            "url": url,
            "title": data.get('title'),
            "content": data.get('content'),
            "pagerank_score": pagerank_score
        }

        # Sử dụng URL làm ID để tránh trùng lặp
        es_client.index(index=INDEX_NAME, id=url, document=doc)
        print(f"[Indexer ✔] Đã đánh chỉ mục '{url[:60]}...' với PageRank: {pagerank_score:.4f}")

    except json.JSONDecodeError:
        print("[Indexer !] Lỗi: Tin nhắn nhận được không phải là JSON hợp lệ.")
    except Exception as e:
        print(f"[Indexer !] Lỗi khi xử lý trang: {e}", file=sys.stderr)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    create_index_if_not_exists()
    load_pagerank_scores()  # Tải lần đầu khi khởi động

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=DATA_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=DATA_QUEUE, on_message_callback=callback)

    print(' [*] Indexer đang chờ nội dung. Để thoát, nhấn CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(0)
    except pika.exceptions.AMQPConnectionError as e:
        print(f'[!] Không thể kết nối đến RabbitMQ: {e}. Đang thử lại...')
        time.sleep(5)
        main()

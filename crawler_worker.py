# 1. Nội dung -> RabbitMQ -> Indexer
# 2. Đồ thị liên kết -> File System -> Spark Processor

import pika
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import sys
import json
import os
import uuid

# --- Cấu hình ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
URL_QUEUE = 'url_frontier'
DATA_QUEUE = 'content_for_indexing'  # Đổi tên hàng đợi cho rõ ràng
LINK_GRAPH_DIR = '/opt/bitnami/spark/data/links'  # Thư mục lưu file liên kết (bên trong container)

# Đảm bảo thư mục tồn tại
os.makedirs(LINK_GRAPH_DIR, exist_ok=True)

# Sử dụng Redis hoặc DB trong thực tế để tránh crawl lại
SEEN_URLS = set()


def is_valid_url(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ['http', 'https']:
            return False
        if any(url.lower().endswith(ext) for ext in
               ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip', '.mp3', '.mp4', '.avi', '.css', '.js']):
            return False
        return True
    except:
        return False


def callback(ch, method, properties, body):
    url = body.decode('utf-8')
    print(f" [->] Đang xử lý URL: {url}")

    if url in SEEN_URLS:
        print(f" [i] Bỏ qua URL đã xử lý: {url}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    SEEN_URLS.add(url)

    try:
        headers = {'User-Agent': 'MySimpleCrawler/1.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        content_type = response.headers.get('content-type', '')
        if 'text/html' not in content_type:
            print(f" [i] Bỏ qua vì không phải HTML: {url}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        soup = BeautifulSoup(response.text, 'lxml')

        # 1. Xử lý nội dung để gửi cho Indexer
        title = soup.title.string if soup.title else 'Không có tiêu đề'
        for script_or_style in soup(['script', 'style']):
            script_or_style.decompose()
        content = soup.get_text(separator=' ', strip=True)

        if content:
            message_for_indexer = {
                'url': url,
                'title': title,
                'content': content
            }
            ch.basic_publish(
                exchange='',
                routing_key=DATA_QUEUE,
                body=json.dumps(message_for_indexer).encode('utf-8'),
                properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
            )
            print(f" [✔] Đã gửi nội dung của '{url[:50]}...' vào hàng đợi '{DATA_QUEUE}'")

        # 2. Xử lý liên kết để tính PageRank
        found_links = set()
        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = urljoin(url, href).split('#')[0]

            if is_valid_url(absolute_url) and urlparse(absolute_url).netloc != urlparse(url).netloc:
                # Chỉ xét các link ra ngoài domain để đồ thị đơn giản hơn
                found_links.add(absolute_url)
                if absolute_url not in SEEN_URLS:
                    ch.basic_publish(
                        exchange='',
                        routing_key=URL_QUEUE,
                        body=absolute_url.encode('utf-8'),
                        properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
                    )

        # Ghi các liên kết tìm thấy vào file
        if found_links:
            # Ghi mỗi liên kết trên một dòng: source_url dest_url
            link_data = ""
            for dest_url in found_links:
                link_data += f"{url} {dest_url}\n"

            # Sử dụng UUID để tránh ghi đè file
            file_path = os.path.join(LINK_GRAPH_DIR, f"{uuid.uuid4()}.txt")
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(link_data)
            print(f" [✔] Đã ghi {len(found_links)} liên kết vào file cho Spark.")

    except requests.RequestException as e:
        print(f" [!] Lỗi khi tải URL {url}: {e}")
    except Exception as e:
        print(f" [!] Lỗi không xác định khi xử lý {url}: {e}")
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f" [✔] Hoàn thành xử lý URL: {url}\n" + "-" * 50)
        time.sleep(1)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=URL_QUEUE, durable=True)
    channel.queue_declare(queue=DATA_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=URL_QUEUE, on_message_callback=callback)
    print(' [*] Crawler Worker đang chờ URL. Để thoát, nhấn CTRL+C')
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

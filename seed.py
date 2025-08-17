# seed.py
# Script này dùng để "gieo mầm" các URL ban đầu vào hàng đợi URL Frontier.

import pika
import sys

# Các URL ban đầu để bắt đầu quá trình crawl
SEED_URLS = [
    'https://vnexpress.net/',
    'https://dantri.com.vn/',
    'https://tuoitre.vn/',
    'https://thanhnien.vn/'
]

def main():
    """
    Kết nối đến RabbitMQ và đẩy các URL ban đầu vào hàng đợi 'url_frontier'.
    """
    try:
        # Kết nối tới server RabbitMQ
        connection_params = pika.ConnectionParameters('localhost')
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()

        # Khai báo một hàng đợi tên là 'url_frontier'
        # durable=True đảm bảo hàng đợi sẽ tồn tại ngay cả khi RabbitMQ khởi động lại
        channel.queue_declare(queue='url_frontier', durable=True)

        # Đẩy từng URL trong SEED_URLS vào hàng đợi
        for url in SEED_URLS:
            channel.basic_publish(
                exchange='',
                routing_key='url_frontier',
                body=url.encode('utf-8'),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE # Đảm bảo tin nhắn không bị mất
                )
            )
            print(f" [x] Đã gửi URL hạt giống: '{url}'")

        # Đóng kết nối
        connection.close()
        print("\n[SUCCESS] Đã gửi thành công tất cả các URL hạt giống.")

    except pika.exceptions.AMQPConnectionError:
        print('[ERROR] Không thể kết nối đến RabbitMQ.')
        print('Hãy chắc chắn rằng bạn đã chạy lệnh "docker-compose up -d" trước.')
        sys.exit(1)


if __name__ == '__main__':
    main()

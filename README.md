I. Thông tin Đồ án
Môn học: Cơ sở Dữ liệu Phân tán

Đề tài: Xây dựng Hệ thống Tìm kiếm Phân tán với Crawler Song song và So sánh Thuật toán Xếp hạng PageRank và HITS (Hiện thực hóa PageRank).

II. Thông tin Thành viên
Họ và Tên: Nguyễn Văn Vũ
MSSV: N21DCCN095
Email: n21dccn095@student.ptithcm.edu.vn

III. Tổng quan Đồ án
Dự án này xây dựng một mô hình thu nhỏ (proof-of-concept) của một cỗ máy tìm kiếm phân tán. Hệ thống có khả năng thu thập dữ liệu web, tính toán điểm uy tín PageRank, và trả về kết quả tìm kiếm cho người dùng.

Hệ thống được thiết kế dựa trên kiến trúc microservices, giúp tăng khả năng mở rộng và chịu lỗi.

Luồng hoạt động chính:

Crawling: Các Crawler Worker thu thập dữ liệu web.

Tạo Đồ thị Liên kết: Cấu trúc liên kết giữa các trang được ghi lại.

Xử lý PageRank: Apache Spark được sử dụng để tính toán điểm PageRank.

Indexing: Indexer Worker lưu trữ nội dung và điểm PageRank vào Elasticsearch.

Querying: Người dùng tìm kiếm thông qua giao diện web, yêu cầu được xử lý bởi một API Server.

IV. Công nghệ sử dụng
Nền tảng: Docker, Docker Compose

Ngôn ngữ: Python 3.9+

Hàng đợi Tin nhắn: RabbitMQ

Xử lý Dữ liệu lớn: Apache Spark

Lưu trữ & Tìm kiếm: Elasticsearch

Giao diện API: FastAPI

Giao diện Người dùng: HTML, Tailwind CSS, JS

Phân tích HTML: BeautifulSoup4

V. Kiến trúc Hệ thống
Hệ thống bao gồm các thành phần chính được container hóa và giao tiếp với nhau qua RabbitMQ, bao gồm: Crawler Worker, cụm Apache Spark, Indexer Worker, Elasticsearch và Query Engine.



VII. Hướng dẫn Cài đặt và Vận hành
Yêu cầu
Docker và Docker Compose.

Python 3.9+ và pip.

Các bước cài đặt
Clone kho chứa:

git clone <URL_KHO_CHỨA_CỦA_BẠN>
cd search_engine

Cài đặt thư viện Python:

pip install pika requests beautifulsoup4 "uvicorn[standard]" fastapi python-multipart

Vận hành hệ thống
Quá trình vận hành gồm nhiều bước chạy song song trên các cửa sổ terminal khác nhau.

Bước 1: Khởi động các dịch vụ nền

docker-compose up -d

Kiểm tra dịch vụ:

RabbitMQ: http://localhost:15672

Spark UI: http://localhost:8080

Elasticsearch: http://localhost:9200

Bước 2: Bắt đầu thu thập dữ liệu

Gieo mầm URL:

python seed.py

Khởi động Crawler:

python crawler_worker.py

Bước 3: Tính toán PageRank

Sau khi crawler đã chạy một lúc, chạy tác vụ Spark:

docker-compose exec spark-master /opt/bitnami/spark/run_pagerank.sh

Bước 4: Đánh chỉ mục dữ liệu

Khi đã có điểm PageRank, khởi động Indexer:

python indexer_worker.py

Bước 5: Khởi động API và Tìm kiếm

Khởi động API Server:

uvicorn query_engine:app --reload

Mở giao diện: Mở file index.html trong trình duyệt để bắt đầu tìm kiếm.


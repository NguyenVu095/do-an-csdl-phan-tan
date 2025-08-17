# Sử dụng phiên bản Elasticsearch mà chúng ta đã thống nhất
FROM docker.elastic.co/elasticsearch/elasticsearch:8.13.0

# Chạy lệnh để cài đặt plugin analysis-icu
# Plugin này rất cần thiết để xử lý tiếng Việt
RUN bin/elasticsearch-plugin install analysis-icu

# Sử dụng image Spark chính thức từ Bitnami
FROM bitnami/spark:3.5


# Sao chép script wrapper vào container
COPY run_pagerank.sh /opt/bitnami/spark/run_pagerank.sh

# Cấp quyền thực thi cho script
RUN chmod +x /opt/bitnami/spark/run_pagerank.sh

# Chuyển về user mặc định
USER 1001

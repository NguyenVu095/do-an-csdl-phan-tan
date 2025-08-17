#!/bin/bash

# run_pagerank.sh
# Script này thiết lập môi trường và chạy tác vụ Spark PageRank.

# Bắt buộc thiết lập biến môi trường HADOOP_USER_NAME để tránh lỗi xác thực
export HADOOP_USER_NAME=spark

echo "Đã thiết lập HADOOP_USER_NAME thành: $HADOOP_USER_NAME"
echo "Bắt đầu chạy spark-submit..."

# Chạy lệnh spark-submit với các tham số
spark-submit \
    --master spark://spark-master:7077 \
    /opt/bitnami/spark/pagerank_processor.py

echo "Tác vụ Spark đã hoàn thành."

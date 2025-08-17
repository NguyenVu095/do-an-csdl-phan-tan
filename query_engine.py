# query_engine.py (phiên bản API với FastAPI - Đã sửa lỗi CORS)
# Cung cấp một API để tìm kiếm dữ liệu đã được đánh chỉ mục trong Elasticsearch.

import sys
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware  # Import thêm middleware CORS
from elasticsearch import Elasticsearch

# --- Cấu hình ---
ELASTICSEARCH_HOST = 'http://localhost:9200'
INDEX_NAME = 'web_pages'

# --- Khởi tạo ứng dụng FastAPI và kết nối Elasticsearch ---
app = FastAPI(
    title="Search Engine API",
    description="Một API đơn giản để truy vấn hệ thống tìm kiếm.",
    version="1.0.0"
)

# --- Cấu hình CORS ---
# Đây là phần quan trọng để sửa lỗi "Failed to fetch"
origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://127.0.0.1",
    "http://127.0.0.1:8000",
    "null"  # Cho phép các yêu cầu từ file:// (khi mở file HTML trực tiếp)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # Cho phép tất cả các phương thức (GET, POST, etc.)
    allow_headers=["*"],  # Cho phép tất cả các header
)
# --- Kết thúc cấu hình CORS ---


try:
    es_client = Elasticsearch(ELASTICSEARCH_HOST)
    print("[Query Engine] Đã kết nối đến Elasticsearch.")
except Exception as e:
    print(f"[Query Engine LỖI] Không thể kết nối đến Elasticsearch: {e}", file=sys.stderr)
    sys.exit(1)


@app.get("/")
def read_root():
    return {"message": "Chào mừng đến với API của Hệ thống Tìm kiếm!"}


@app.get("/search")
def search(q: str = Query(..., min_length=2, description="Truy vấn tìm kiếm của bạn")):
    """
    Thực hiện tìm kiếm trong Elasticsearch.
    - q: Chuỗi truy vấn.
    """
    if not es_client.indices.exists(index=INDEX_NAME):
        raise HTTPException(status_code=404, detail=f"Index '{INDEX_NAME}' không tồn tại. Vui lòng chạy Indexer trước.")

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": q,
                            "fields": ["title^2", "content"],
                            "fuzziness": "AUTO"
                        }
                    }
                ],
                "should": [
                    {
                        "rank_feature": {
                            "field": "pagerank_score",
                            "boost": 0.5
                        }
                    }
                ]
            }
        },
        "size": 10,
        "highlight": {
            "fields": {
                "content": {}
            }
        }
    }

    try:
        response = es_client.search(index=INDEX_NAME, body=query)

        results = []
        for hit in response['hits']['hits']:
            result = {
                "score": hit['_score'],
                "title": hit['_source']['title'],
                "snippet": " ".join(hit['highlight']['content']) if 'highlight' in hit else hit['_source']['content'][
                                                                                            :150] + '...'
            }
            results.append(result)

        return {
            "query": q,
            "took_ms": response['took'],
            "total_hits": response['hits']['total']['value'],
            "results": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Đã xảy ra lỗi khi truy vấn: {e}")

# Để chạy server, sử dụng lệnh:
# uvicorn query_engine:app --reload

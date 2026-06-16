# Dự án Ecommerce Lakehouse (Ecommerce Lakehouse Project)

Dự án này triển khai một nền tảng dữ liệu Ecommerce Lakehouse hoàn chỉnh từ đầu đến cuối. Hệ thống thực hiện thu thập (crawl) thông tin sản phẩm và đánh giá từ các sàn thương mại điện tử, lưu trữ dữ liệu thô vào MinIO (Bronze Layer dưới dạng JSON/Delta), chuẩn hóa bằng PySpark thành các bảng hoạt động (Silver Delta), và xây dựng các bộ dữ liệu phân tích hợp nhất (Gold Delta).

---

## 1. Kiến trúc Dự án (Project Architecture)

```text
  [Ingestion / Crawlers] ──> [MinIO Bronze] ──> [Spark/Pandas] ──> [MinIO Silver] ──> [MinIO Gold]
    (Tiki, Shopee, Sendo)      (Raw JSON)          (CDC/Clean)       (Delta Lake)          (Delta Lake)
                                                                          │                      │
  [Data Simulator] ───> [Postgres Source] ───> [Debezium] ───> [Kafka] ───┘                      │
                                                                                                 ▼
  [Metabase BI] <─── [ClickHouse Serving DB] <─── [Silver to ClickHouse Sync Engine] ────────────┘
```

Hệ thống bao gồm 3 lớp dữ liệu chính:
1. **Bronze Layer (Lớp Đồng)**: Dữ liệu thô thu thập từ crawler hoặc dữ liệu sự kiện Kafka CDC được lưu trữ dưới dạng JSON/Delta trên MinIO.
2. **Silver Layer (Lớp Bạc)**: Các bảng hoạt động đã được làm sạch, khử trùng lặp, định nghĩa kiểu dữ liệu và phân vùng theo ngày sự kiện (`event_date`) dưới định dạng Delta.
3. **Gold Layer (Lớp Vàng)**: Các bảng chiều (dimension tables) được tối ưu hóa cho truy vấn phân tích (Delta format).

---

## 2. Chuẩn bị & Cài đặt (Prerequisites & Setup)

### A. Cấu hình Môi trường (Environment Configuration)
Tạo tệp `.env` tại thư mục gốc của dự án (Create `.env` file at the project root):
```bash
# Cấu hình MinIO cục bộ (MinIO Local Configuration)
MINIO_ENDPOINT_URL=http://localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123
MINIO_BUCKET_NAME=bronze-lakehouse

# Cấu hình BROWSERLESS (Yêu cầu cho việc crawl Tiki)
BROWSERLESS_URL=http://localhost:3000/webdriver
```

Đồng thời, kiểm tra hoặc tạo tệp `processing/.env` phục vụ cấu hình kết nối Spark:
```bash
# Cấu hình MinIO trong mạng Docker (MinIO Docker Configuration)
MINIO_ENDPOINT_URL=http://minio:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123
MINIO_BUCKET_NAME=bronze-lakehouse
```

---

## 3. Khởi chạy Hạ tầng cục bộ (Running Local Infrastructure)

Khởi động tất cả các dịch vụ cốt lõi bao gồm MinIO, Kafka, Postgres Metastore, Spark và cơ sở dữ liệu nguồn PostgreSQL:
```bash
docker compose up -d
```

### Các dịch vụ sẵn có (Available Services):
* **MinIO Console**: [http://localhost:9001](http://localhost:9001) (User: `admin` / Password: `password123`)
* **Spark Web UI**: [http://localhost:4040](http://localhost:4040)
* **PostgreSQL Hive Metastore**: `localhost:5432`
* **ClickHouse HTTP**: `http://localhost:8123` (Native TCP: `localhost:9009`, User: `admin` / Password: `password123`)
* **Metabase Console**: [http://localhost:3001](http://localhost:3001)

---

## 4. Chạy luồng Thu thập Dữ liệu (Run Ingestion - Bronze Layer)

Cài đặt các thư viện thu thập dữ liệu trên môi trường máy ảo Python cục bộ:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Chạy Crawler Tiki (Tiki Crawler)

Bộ cào dữ liệu Tiki tự động quản lý trạng thái cào (số trang tiếp theo) bằng file text cục bộ (`crawler_state.txt`) và tải dữ liệu thô cào được lên MinIO (Bronze Layer).

* **Chạy mặc định (Cào tuần tự tất cả danh mục trong `crawler_state.txt`)**:
  ```bash
  python ingestion/batch/main.py --limit_pages 1
  ```
  *Luồng hoạt động*: 
  - Script sẽ tự động quét danh sách danh mục trong `crawler_state.txt` (nếu file trống sẽ tự động khởi tạo bằng 6 danh mục mặc định).
  - Tìm danh mục đầu tiên chưa hoàn thành cào đủ **40 trang** (tức là `next_page <= 40`) để tiến hành cào.
  - Sau khi danh mục đó cào đủ 40 trang, script sẽ tự động chuyển sang danh mục tiếp theo trong lượt chạy kế tiếp (hoặc trong cùng lượt chạy nếu `--limit_pages` lớn hơn số trang còn lại của danh mục hiện tại).

* **Chạy cào một danh mục duy nhất (Single Category Mode)**:
  ```bash
  python ingestion/batch/main.py --category 1846 --limit_pages 1
  ```

* **Tham số tùy chỉnh (Custom parameters)**:
  * `--category`: ID danh mục cần cào (nếu bỏ trống sẽ chạy chế độ cào tuần tự tất cả danh mục).
  * `--category_name`: Tên danh mục tự chọn để ghi nhận vào file trạng thái (chỉ dùng khi truyền `--category`).
  * `--limit_pages`: Tổng số lượng trang muốn cào trong lượt chạy này (mặc định: `1`).
  * `--start_page`: Trang bắt đầu cào (ghi đè file trạng thái nếu truyền, chỉ dùng khi truyền `--category`).

---

## 5. Khởi chạy Trình Giả lập Dữ liệu (Run Data Simulator)

Trình giả lập sẽ tự động khởi tạo dữ liệu nền (Platforms, Sellers, Categories, Brands, Customers, Addresses) và liên tục tạo ra các giao dịch mua sắm, thanh toán, cập nhật kho, đánh giá sản phẩm ngẫu nhiên để đẩy vào Postgres (`postgres-data-source`), từ đó kích hoạt luồng sự kiện CDC của Debezium và Kafka.

### Cách khởi chạy cục bộ (Run Simulator locally):
```bash
# Đảm bảo môi trường ảo venv đã được kích hoạt
pip install -r simulator/requirements.txt

# Khởi chạy trình giả lập
python simulator/simulate.py
```
*(Simulator sẽ sinh dữ liệu liên tục sau mỗi 0.5 - 1.5 giây. Nhấn `Ctrl + C` để dừng giả lập)*.

---

## 6. Khởi chạy luồng xử lý Spark (Run Spark Processing Pipelines)

Các kịch bản xử lý dữ liệu Spark đã được tái cấu trúc (refactor) dưới dạng các gói module con nằm trong thư mục `processing/streaming/` (bao gồm `bronze_to_silver/` và `silver_to_gold/`) nhằm tách biệt rõ ràng giữa schema, cấu hình và logic biến đổi lõi.

Các lệnh khởi chạy dưới đây sử dụng lớp kịch bản bao bọc (wrapper) tương thích ngược, chạy trực tiếp bên trong container Spark.

### A. Ingestion dữ liệu Kafka CDC (Kafka to Bronze)
Đọc các topic CDC từ Kafka và ghi các bản ghi thô xuống Bronze Delta:
```bash
docker exec -it spark_processor python /app/streaming/kafka_to_bronze.py
```

### B. Chuẩn hóa dữ liệu CDC từ Bronze sang Silver (Bronze to Silver)
Làm sạch, khử trùng lặp dữ liệu CDC thô và chuyển đổi thành các bảng hoạt động định dạng Delta:
```bash
docker exec -it spark_processor python /app/jobs/bronze_to_silver.py
```

### C. Xây dựng mô hình phân tích Silver sang Gold (Silver to Gold)
Hợp nhất, tổng hợp dữ liệu từ các bảng hoạt động Silver Delta thành các bảng chiều Gold Delta phục vụ BI/Analytics:
```bash
docker exec -it spark_processor python /app/jobs/silver_to_gold.py
```

---

## 6B. Khởi chạy luồng xử lý Spark-free (Pandas & Rust-core)

Nhánh `feat/pandas-delta-streaming` hỗ trợ luồng xử lý dữ liệu thay thế sử dụng **Pandas** và thư viện **deltalake** (nhân Rust) thay cho Spark, giúp tiết kiệm tối đa RAM và CPU (0% CPU khi rảnh, RAM chỉ ~100MB). Các tiến trình Pandas cũng được tích hợp đầy đủ khả năng **đồng bộ Hive Metastore không dùng Spark** qua PostgreSQL backend.

Hãy cài đặt thư viện bổ sung trên môi trường venv cục bộ trước khi chạy:
```bash
pip install -r requirements.txt
```

### A. Ingestion dữ liệu Kafka CDC (Kafka to Bronze)
Đọc dữ liệu CDC từ Kafka và ghi thô vào Bronze Delta Table bằng Pandas:
```bash
python processing/streaming/pandas_kafka_to_bronze.py --bootstrap-servers localhost:9092
```
* **Các tham số tùy chọn:**
  * `--topics`: Danh sách các topic Kafka (cách nhau bằng dấu phẩy).
  * `--topic-pattern`: Regex chọn topic (mặc định: `^cdc.ecommerce.public.*`).
  * `--bootstrap-servers`: Địa chỉ Kafka Broker (mặc định: `localhost:9092` hoặc `kafka:9092`).
  * `--starting-offsets`: Offset bắt đầu khi chạy lần đầu (`earliest` hoặc `latest`).

### B. Chuẩn hóa dữ liệu CDC từ Bronze sang Silver (Bronze to Silver)
Xử lý làm sạch, ép kiểu dữ liệu, thực hiện lệnh Merge (ACID) vào các bảng Silver Delta và tự động cập nhật Hive Metastore:
```bash
python processing/streaming/pandas_bronze_to_silver.py --hive-db silver --interval 5.0
```
* **Các tham số tùy chọn:**
  * `--bronze-path`: Đường dẫn Delta chứa dữ liệu thô (mặc định: `s3a://bronze-lakehouse/kafka_cdc`).
  * `--silver-base`: Đường dẫn lưu trữ lớp Silver (mặc định: `s3a://silver-lakehouse`).
  * `--tables`: Danh sách bảng muốn xử lý (ví dụ: `--tables products,orders`).
  * `--hive-db`: Database đăng ký trong Hive Metastore (mặc định: `silver`).
  * `--skip-hive-sync`: Flag bỏ qua đồng bộ metadata vào Hive Metastore.
  * `--interval`: Khoảng thời gian quét dữ liệu mới (giây, mặc định: `5.0`).

### C. Xây dựng mô hình phân tích Silver sang Gold (Silver to Gold)
Kết hợp (Join) các bảng Silver bằng Pandas, ghi/merge vào các bảng Gold Delta và tự động cập nhật Hive Metastore:
```bash
python processing/streaming/pandas_silver_to_gold.py --hive-db gold --interval 10.0
```
* **Các tham số tùy chọn:**
  * `--silver-base`: Đường dẫn đọc dữ liệu lớp Silver (mặc định: `s3a://silver-lakehouse`).
  * `--gold-base`: Đường dẫn lưu trữ lớp Gold (mặc định: `s3a://gold-lakehouse`).
  * `--tables`: Danh sách bảng muốn xử lý (ví dụ: `--tables dim_products,fct_orders`).
  * `--hive-db`: Database đăng ký trong Hive Metastore (mặc định: `gold`).
  * `--skip-hive-sync`: Flag bỏ qua đồng bộ metadata vào Hive Metastore.
  * `--interval`: Khoảng thời gian quét dữ liệu mới (giây, mặc định: `10.0`).

### D. Đồng bộ dữ liệu lớp Gold trực tiếp vào ClickHouse (Silver to ClickHouse)
Đồng bộ các bảng chiều và bảng sự kiện lớp Gold trực tiếp từ Silver Delta Lake sang ClickHouse phục vụ Metabase truy vấn realtime với độ trễ cực thấp:
```bash
python processing/streaming/pandas_silver_to_clickhouse.py --interval 15.0
```
* **Các tham số tùy chọn:**
  * `--silver-base`: Đường dẫn đọc dữ liệu lớp Silver (mặc định: `s3a://silver-lakehouse`).
  * `--tables`: Danh sách bảng muốn đồng bộ (ví dụ: `--tables dim_products,fct_orders`).
  * `--interval`: Khoảng thời gian cập nhật micro-batch (giây, mặc định: `15.0`).
  * `--once`: Chỉ chạy đồng bộ một lần duy nhất rồi thoát (phục vụ test).

---

## 7. Hướng dẫn Tham số dòng lệnh (Command Line Arguments Guide)

Cả hai kịch bản xử lý `bronze_to_silver.py` và `silver_to_gold.py` hỗ trợ đầy đủ các tham số dòng lệnh để tùy chỉnh hành vi chạy batch hoặc stream thời gian thực.

### A. Tham số cho luồng Bronze ➔ Silver (`bronze_to_silver.py`)

| Tham số (Argument) | Giá trị mặc định (Default) | Ý nghĩa & Khi nào nên sử dụng (When to use) |
| :--- | :--- | :--- |
| `--bronze-path` | `s3a://bronze-lakehouse/kafka_cdc` | **Khi nguồn dữ liệu Bronze thay đổi**: Đường dẫn Delta chứa dữ liệu thô được ghi bởi `kafka_to_bronze.py`. Chỉ định khi muốn đọc từ bucket hoặc môi trường kiểm thử khác. |
| `--silver-base` | `s3a://silver-lakehouse` | **Khi thay đổi đích ghi lớp Silver**: Thư mục gốc chứa các bảng Delta của lớp Silver. Sử dụng khi muốn ghi sang bucket khác. |
| `--checkpoint-path` | `s3a://silver-lakehouse/_checkpoints/...` | **Thay đổi nơi lưu vết streaming**: Chỉ định đường dẫn checkpoint của Structured Streaming để Spark lưu vết offset của nguồn dữ liệu. |
| `--hive-db` | `silver` | **Đăng ký cơ sở dữ liệu Hive khác**: Tên cơ sở dữ liệu trong PostgreSQL Hive Metastore để quản lý bảng. Dùng khi chạy trên các schema dữ liệu khác nhau. |
| `--processing-time` | `30 seconds` | **Điều chỉnh tần suất quét dữ liệu**: Khoảng thời gian kích hoạt (trigger interval) của Spark Streaming. Tăng lên khi muốn tiết kiệm tài nguyên (ví dụ: `1 minute`), giảm đi khi muốn dữ liệu realtime hơn (ví dụ: `10 seconds`). |
| `--available-now` | *Không có* (Action Flag) | **Chạy quét dữ liệu hiện tại rồi tự dừng**: Tiện lợi khi chạy kiểm tra tích hợp hoặc triển khai dưới dạng **Cron Job hàng ngày** thay vì chạy dịch vụ streaming 24/7. |
| `--once` | *Không có* (Action Flag) | **Chạy dưới dạng Batch thuần túy**: Đọc dữ liệu Bronze như một tập dữ liệu tĩnh rồi thoát. **Dùng khi debug câu lệnh** hoặc khi làm dữ liệu cũ lịch sử (Backfill) mà không muốn tạo checkpoint streaming. |
| `--skip-hive-sync` | Mặc định theo biến môi trường | **Bỏ qua đăng ký Hive Metastore**: Sử dụng trong môi trường **chạy luồng sản xuất (Production Streaming) 24/7** để giảm thời gian ghi của mỗi micro-batch (Delta table tự quản lý phân vùng thông qua `_delta_log` mà không cần Hive cập nhật liên tục). |
| `--tables` | *Không có* (Đọc tất cả) | **Chỉ xử lý một số bảng nhất định**: Nhập danh sách bảng phân tách bằng dấu phẩy (ví dụ: `--tables products,orders`). Dùng khi **thử nghiệm lỗi trên một bảng cụ thể** hoặc khi muốn ưu tiên cập nhật một số bảng quan trọng trước. |

---

### B. Tham số cho luồng Silver ➔ Gold (`silver_to_gold.py`)

| Tham số (Argument) | Giá trị mặc định (Default) | Ý nghĩa & Khi nào nên sử dụng (When to use) |
| :--- | :--- | :--- |
| `--silver-base` | `s3a://silver-lakehouse` | **Khi thay đổi nguồn dữ liệu Silver**: Chỉ định thư mục chứa các Delta table của lớp Silver để lấy dữ liệu đầu vào. |
| `--gold-base` | `s3a://gold-lakehouse` | **Khi thay đổi đích ghi lớp Gold**: Chỉ định thư mục lưu trữ các Delta table phân tích lớp Gold. |
| `--checkpoint-base` | `s3a://gold-lakehouse/_checkpoints/...` | **Thay đổi nơi lưu vết checkpoint**: Chỉ định thư mục lưu trữ trạng thái streaming cho các bảng chiều Gold. |
| `--hive-db` | `gold` | **Đăng ký cơ sở dữ liệu Hive Gold khác**: Tên database đăng ký trong Hive Metastore. |
| `--processing-time` | `10 seconds` | **Điều chỉnh độ trễ bảng chiều**: Độ trễ cập nhật từ Silver sang Gold. Bảng Gold thường cần phản hồi nhanh nên mặc định ngắn hơn (10 giây). |
| `--available-now` | *Không có* (Action Flag) | **Xử lý toàn bộ dữ liệu Silver hiện có rồi dừng**: Dùng cho việc chạy cập nhật bảng phân tích định kỳ bằng Cron Job. |
| `--once` | *Không có* (Action Flag) | **Chạy Batch ETL lớp Gold**: Tính toán toàn bộ các bảng chiều từ Silver tĩnh rồi lưu xuống Gold. Dùng cho việc **Backfill phân tích**. |
| `--skip-hive-sync` | Mặc định theo biến môi trường | **Tăng tốc độ ghi bảng chiều**: Bỏ qua đăng ký Hive trong mỗi batch ghi để tối ưu hiệu năng. |
| `--tables` | *Không có* (Đọc tất cả) | **Chỉ tính toán bảng chiều chỉ định**: Chỉ định danh sách cụ thể (ví dụ: `--tables dim_products,dim_customers`). Cực kỳ hữu ích khi bạn **chỉ thay đổi logic biến đổi của riêng một bảng chiều** và chỉ muốn chạy lại bảng đó mà không ảnh hưởng tới các bảng khác. |

---

## 8. Kiểm tra & Khắc phục Sự cố (Verification & Troubleshooting)

Kiểm tra kết nối tổng quan và quyền đọc/ghi trên Lakehouse:
```bash
python processing/test_lakehouse.py
```

Kiểm tra kết nối trực tiếp đến bucket của MinIO:
```bash
python storage/check_minio_connection.py
```

Theo dõi các truy vấn streaming đang chạy bên trong môi trường Spark:
```bash
docker exec -it spark_processor spark-shell
```

Kiểm tra danh sách bảng đã tạo và số lượng bản ghi trong ClickHouse:
```bash
# Xem các bảng trong database gold_serving
docker exec clickhouse_server clickhouse-client -q "SHOW TABLES IN gold_serving"

# Xem số dòng thực tế (đã loại bỏ trùng lặp bằng ReplacingMergeTree)
docker exec clickhouse_server clickhouse-client -q "SELECT count() FROM gold_serving.fct_orders FINAL"
```

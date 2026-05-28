# Kiến Trúc Kết Nối Giữa Trino, Hive Metastore Và Delta Lake

Tài liệu này giải thích chi tiết mối quan hệ và cách thức phối hợp giữa **Trino (Catalog `delta`)**, **Hive Metastore**, và **Apache Spark** trong hệ thống Data Lakehouse của dự án.

---

## 1. Cấu Hình Catalog `delta` Của Trino

Mặc dù sử dụng cổng kết nối chuyên biệt cho Delta Lake (`connector.name=delta_lake`), **Trino vẫn bắt buộc phải kết nối tới Hive Metastore** để hỏi thông tin về sơ đồ dữ liệu (schema) và vị trí thư mục của bảng.

Cấu hình thực tế tại `trino/catalog/delta.properties`:
```properties
connector.name=delta_lake
hive.metastore=thrift
hive.metastore.uri=thrift://hive-metastore:9083   # Kết nối tới Hive Metastore chung
fs.s3.enabled=true
s3.endpoint=http://minio:9000
...
```

---

## 2. Quy Trình Phối Hợp 3 Bên (Spark - Hive Metastore - Trino)

Quy trình dữ liệu được đồng bộ hóa và truy vấn diễn ra nhịp nhàng đằng sau hậu trường như sau:

```
  [1] Spark ghi dữ liệu Delta
       │
       ├───► Ghi file Parquet & _delta_log lên [MinIO]
       │
       └───► Đăng ký: "Tôi vừa tạo bảng `ecom_products` ở `s3a://silver/...`" vào [Hive Metastore]
             │
             ▼
  [2] Bạn mở Metabase ───► Gửi câu lệnh truy vấn tới [Trino] (Catalog `delta`)
                             │
                             ├───► Hỏi [Hive Metastore]: "Bảng `ecom_products` nằm ở đâu và có cột gì?"
                             │     ◄─── Trả lời: "Nó nằm ở `s3a://silver/...` trên MinIO"
                             │
                             └───► Đọc trực tiếp thư mục `_delta_log` trên [MinIO] bằng công nghệ Delta Lake
                                   để lấy dữ liệu chính xác và trả về cho Metabase.
```

---

## 3. Tầm Quan Trọng Của Hive Metastore

Nếu không có Hive Metastore làm cầu nối ở giữa:
* **Spark** ghi dữ liệu xuống MinIO xong thì thôi, không có nơi nào lưu trữ tập trung thông tin "Bảng này tên là gì, gồm những cột nào".
* **Trino (Catalog `delta`)** sẽ hoàn toàn "mù tịt", không hề biết trong MinIO có những bảng nào để hiển thị cho bạn chọn trên Metabase, trừ khi bạn phải tự gõ lệnh thủ công định nghĩa lại từng bảng một trên Trino (rất mất công và dễ sai lệch).

### Kết luận:
**Hive Metastore đóng vai trò là "Danh bạ điện thoại" chung.** 
* **Spark** là người đăng ký số điện thoại (tên bảng & vị trí file) vào danh bạ.
* **Trino (Catalog `delta`)** là người tra danh bạ để biết địa chỉ, sau đó sử dụng đường dây kết nối siêu tốc (công nghệ Delta) để đến đúng địa chỉ đó trên MinIO lấy dữ liệu.

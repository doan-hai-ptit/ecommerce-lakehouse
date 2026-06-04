# Hướng Dẫn Truy Vấn Dữ Liệu Lớp Silver và Gold trên Metabase (Trino)

Tài liệu này tổng hợp chi tiết các câu lệnh SQL phục vụ cho các bài toán phân tích tài chính, sức khỏe người bán, vận hành và quản lý rủi ro trên sàn thương mại điện tử. 

Dữ liệu được truy vấn qua công cụ **Trino (Catalog `delta`)** kết nối tới Hive Metastore, trỏ tới các bảng Delta Lake trên MinIO:
* **Lớp Silver:** Truy vấn qua schema `delta.silver.<tên_bảng>`
* **Lớp Gold:** Truy vấn qua schema `delta.gold.<tên_bảng>` (Được khuyến khích dùng vì cấu trúc đã được chuẩn hóa và tối ưu hóa sẵn).

---

## BÀI TOÁN 1: Phân tích Sức khỏe Tài chính & Tăng trưởng (Finance & Growth)

### 1. Tính tổng giá trị giao dịch (GMV), Doanh thu sàn (Platform Revenue) & Take Rate
* **GMV (Gross Merchandise Value):** Tổng giá trị các đơn hàng có trạng thái thành công (`completed`).
* **Platform Revenue (Doanh thu sàn):** Giả định sàn thu phí hoa hồng cố định là **5%** trên tổng giá trị đơn hàng cộng thêm **2.000đ** phí cố định cho mỗi đơn hàng hoàn thành.
* **Take Rate:** Tỷ lệ phần trăm doanh thu thực tế giữ lại trên tổng dòng tiền (Platform Revenue / GMV).

#### 👉 Truy vấn trên lớp Gold (`fct_orders`):
```sql
SELECT 
    sum(total_amount) AS gmv,
    -- Giả sử 5% hoa hồng + 2000đ phí cố định mỗi đơn thành công
    sum(total_amount * 0.05 + 2000) AS platform_revenue,
    -- Tỷ lệ Take Rate (%)
    (sum(total_amount * 0.05 + 2000) / sum(total_amount)) * 100 AS take_rate
FROM delta.gold.fct_orders
WHERE order_status = 'completed';
```

#### 👉 Truy vấn trên lớp Silver (`orders`):
```sql
SELECT 
    sum(total_amount) AS gmv,
    sum(total_amount * 0.05 + 2000) AS platform_revenue,
    (sum(total_amount * 0.05 + 2000) / sum(total_amount)) * 100 AS take_rate
FROM delta.silver.orders
WHERE order_status = 'completed';
```

---

### 2. Tốc độ tăng trưởng dòng tiền và số lượng đơn hàng (Month-over-Month - MoM)
Sử dụng hàm cửa sổ `LAG()` để so sánh với số liệu tháng liền kề trước đó.

#### 👉 Truy vấn trên lớp Gold (`fct_orders`):
```sql
WITH monthly_metrics AS (
    SELECT 
        date_trunc('month', created_at) AS order_month,
        sum(total_amount) AS gmv,
        count(order_id) AS successful_orders
    FROM delta.gold.fct_orders
    WHERE order_status = 'completed'
    GROUP BY 1
)
SELECT 
    order_month,
    gmv,
    LAG(gmv) OVER (ORDER BY order_month) AS prev_month_gmv,
    ((gmv - LAG(gmv) OVER (ORDER BY order_month)) / gmv) * 100 AS gmv_mom_growth_pct,
    
    successful_orders,
    LAG(successful_orders) OVER (ORDER BY order_month) AS prev_month_orders,
    ((successful_orders - LAG(successful_orders) OVER (ORDER BY order_month)) / CAST(LAG(successful_orders) OVER (ORDER BY order_month) AS DOUBLE)) * 100 AS orders_mom_growth_pct
FROM monthly_metrics
ORDER BY order_month;
```

#### 👉 Truy vấn trên lớp Silver (`orders`):
```sql
WITH monthly_metrics AS (
    SELECT 
        date_trunc('month', ordered_at) AS order_month,
        sum(total_amount) AS gmv,
        count(order_id) AS successful_orders
    FROM delta.silver.orders
    WHERE order_status = 'completed'
    GROUP BY 1
)
SELECT 
    order_month,
    gmv,
    LAG(gmv) OVER (ORDER BY order_month) AS prev_month_gmv,
    ((gmv - LAG(gmv) OVER (ORDER BY order_month)) / LAG(gmv) OVER (ORDER BY order_month)) * 100 AS gmv_mom_growth_pct,
    
    successful_orders,
    LAG(successful_orders) OVER (ORDER BY order_month) AS prev_month_orders,
    ((successful_orders - LAG(successful_orders) OVER (ORDER BY order_month)) / CAST(LAG(successful_orders) OVER (ORDER BY order_month) AS DOUBLE)) * 100 AS orders_mom_growth_pct
FROM monthly_metrics
ORDER BY order_month;
```

---

## BÀI TOÁN 2: Phân tích Sức khỏe Hệ sinh thái Người bán (Merchant Ecosystem Health)

### 1. Phân nhóm Shop (Shop Tiering) dựa trên Doanh thu
Chia các Shop thành các hạng: **Diamond** (từ 100 triệu trở lên), **Gold** (từ 50 triệu), **Silver** (từ 10 triệu), và **Regular** cho các shop còn lại.

#### 👉 Truy vấn trên lớp Gold (`dim_sellers` & `fct_orders`):
```sql
WITH seller_revenue AS (
    SELECT 
        s.seller_id,
        s.seller_name,
        sum(o.total_amount) AS total_revenue
    FROM delta.gold.dim_sellers s
    LEFT JOIN delta.gold.fct_orders o ON s.seller_id = o.seller_id AND o.order_status = 'completed'
    GROUP BY s.seller_id, s.seller_name
)
SELECT 
    seller_id,
    seller_name,
    total_revenue,
    CASE 
        WHEN total_revenue >= 100000000 THEN 'Diamond'
        WHEN total_revenue >= 50000000  THEN 'Gold'
        WHEN total_revenue >= 10000000  THEN 'Silver'
        ELSE 'Regular'
    END AS shop_tier
FROM seller_revenue
ORDER BY total_revenue DESC;
```

#### 👉 Truy vấn trên lớp Silver (`sellers` & `orders`):
```sql
WITH seller_revenue AS (
    SELECT 
        s.seller_id,
        s.seller_name,
        sum(o.total_amount) AS total_revenue
    FROM delta.silver.sellers s
    LEFT JOIN delta.silver.orders o ON s.seller_id = o.seller_id AND o.order_status = 'completed'
    GROUP BY s.seller_id, s.seller_name
)
SELECT 
    seller_id,
    seller_name,
    total_revenue,
    CASE 
        WHEN total_revenue >= 100000000 THEN 'Diamond'
        WHEN total_revenue >= 50000000  THEN 'Gold'
        WHEN total_revenue >= 10000000  THEN 'Silver'
        ELSE 'Regular'
    END AS shop_tier
FROM seller_revenue
ORDER BY total_revenue DESC;
```

---

### 2. Tỷ lệ đóng góp doanh thu (Phân tích Pareto 80/20)
Xác định tỷ lệ lũy kế đóng góp doanh thu của từng shop để kiểm chứng xem 80% doanh thu toàn sàn có tập trung vào 20% shop hay không.

#### 👉 Truy vấn trên lớp Gold (`fct_orders`):
```sql
WITH seller_revenue AS (
    SELECT 
        seller_id,
        sum(total_amount) AS revenue
    FROM delta.gold.fct_orders
    WHERE order_status = 'completed'
    GROUP BY seller_id
),
ranked_sellers AS (
    SELECT 
        seller_id,
        revenue,
        -- Doanh thu lũy kế cộng dồn từ cao xuống thấp
        sum(revenue) OVER (ORDER BY revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue,
        sum(revenue) OVER () AS total_revenue,
        -- Thứ hạng của shop và tổng số lượng shop
        row_number() OVER (ORDER BY revenue DESC) AS shop_rank,
        count(*) OVER () AS total_sellers
    FROM seller_revenue
)
SELECT 
    seller_id,
    revenue,
    (cumulative_revenue / total_revenue) * 100 AS cumulative_revenue_pct,  -- Tỷ lệ lũy kế đóng góp doanh thu (%)
    (CAST(shop_rank AS DOUBLE) / total_sellers) * 100 AS cumulative_seller_pct -- Tỷ lệ phần trăm shop (%)
FROM ranked_sellers
ORDER BY revenue DESC;
```

---

### 3. Tốc độ "sống sót" của Shop (Survival Rate)
Đo lường tỷ lệ các Shop mới đăng ký (`created_at`) có đơn hàng thành công đầu tiên trong vòng **30 ngày**.

#### 👉 Truy vấn trên lớp Gold (`dim_sellers` & `fct_orders`):
```sql
WITH first_order AS (
    SELECT 
        seller_id,
        min(created_at) AS first_order_at
    FROM delta.gold.fct_orders
    WHERE order_status = 'completed'
    GROUP BY seller_id
),
seller_survival AS (
    SELECT 
        s.seller_id,
        s.seller_name,
        s.created_at AS shop_created_at,
        fo.first_order_at,
        -- Tính số ngày từ lúc mở shop đến lúc có đơn hàng đầu tiên
        date_diff('day', s.created_at, fo.first_order_at) AS days_to_first_order
    FROM delta.gold.dim_sellers s
    LEFT JOIN first_order fo ON s.seller_id = fo.seller_id
)
SELECT 
    count(seller_id) AS total_new_shops,
    count(CASE WHEN days_to_first_order <= 30 THEN 1 END) AS active_shops_within_first_month,
    (CAST(count(CASE WHEN days_to_first_order <= 30 THEN 1 END) AS DOUBLE) / count(seller_id)) * 100 AS survival_rate_pct
FROM seller_survival;
```

---

## BÀI TOÁN 3: Phân tích Hiệu suất Vận hành & Rủi ro (Operations & Risk Analytics)

### 1. Tỷ lệ hủy đơn (Cancellation Rate) & Hoàn hàng (Return Rate)
Đánh giá tỷ lệ rủi ro của toàn sàn dựa trên trạng thái đơn hàng.

#### 👉 Truy vấn trên lớp Gold (`fct_orders`):
```sql
SELECT 
    count(order_id) AS total_orders,
    count(CASE WHEN order_status = 'cancelled' THEN 1 END) AS cancelled_orders,
    (CAST(count(CASE WHEN order_status = 'cancelled' THEN 1 END) AS DOUBLE) / count(order_id)) * 100 AS cancellation_rate_pct,
    count(CASE WHEN order_status = 'returned' THEN 1 END) AS returned_orders,
    (CAST(count(CASE WHEN order_status = 'returned' THEN 1 END) AS DOUBLE) / count(order_id)) * 100 AS return_rate_pct
FROM delta.gold.fct_orders;
```

---

### 2. Hiệu suất của các Đơn vị vận chuyển (Logistics Partner Performance)

> [!TIP]
> Bảng `delta.gold.fct_shipments` đã được lập trình tính toán trước cột `delivery_duration_hours` (thời gian giao hàng thực tế tính bằng giờ) và chỉ số `is_delayed` (bị trễ giờ giao dự kiến), giúp giảm thiểu thao tác JOIN và tối ưu hóa hiệu năng câu truy vấn trên Metabase.

#### 👉 Truy vấn trên lớp Gold (`fct_shipments`):
```sql
SELECT 
    carrier_name,
    count(shipment_id) AS total_shipments,
    -- Thời gian giao hàng trung bình từ lúc đặt hàng (giờ)
    avg(delivery_duration_hours) AS avg_delivery_duration_hours,
    -- Tỷ lệ đơn giao bị trễ hẹn (%)
    avg(is_delayed) * 100 AS delay_rate_pct,
    -- Tỷ lệ giao hàng thất bại hoặc hoàn lại (%)
    (CAST(count(CASE WHEN status IN ('failed', 'returned') THEN 1 END) AS DOUBLE) / count(shipment_id)) * 100 AS failed_or_returned_rate_pct
FROM delta.gold.fct_shipments
GROUP BY carrier_name
ORDER BY avg_delivery_duration_hours;
```

#### 👉 Truy vấn trên lớp Silver (`shipments` & `orders`):
```sql
SELECT 
    s.carrier_name,
    count(s.shipment_id) AS total_shipments,
    -- Thời gian giao hàng trung bình từ lúc đặt hàng đến lúc giao thành công (ngày)
    avg(date_diff('day', o.ordered_at, s.delivered_at)) AS avg_delivery_days,
    -- Tỷ lệ trễ hẹn giao hàng (%)
    (CAST(count(CASE WHEN s.delivered_at > s.estimated_delivery_at THEN 1 END) AS DOUBLE) / count(CASE WHEN s.delivered_at IS NOT NULL THEN 1 END)) * 100 AS delay_rate_pct,
    -- Tỷ lệ giao thất bại hoặc hoàn lại (%)
    (CAST(count(CASE WHEN s.status IN ('failed', 'returned') THEN 1 END) AS DOUBLE) / count(s.shipment_id)) * 100 AS failed_or_returned_rate_pct
FROM delta.silver.shipments s
LEFT JOIN delta.silver.orders o ON s.order_id = o.order_id
GROUP BY s.carrier_name
ORDER BY avg_delivery_days;
```

---

### 3. Chỉ số uy tín của Sàn (Điểm đánh giá trung bình theo thời gian)
Phân tích điểm số đánh giá trung bình của sản phẩm, đối tác giao nhận và shop qua từng tháng.

#### 👉 Truy vấn trên lớp Gold (`fct_product_reviews`):
```sql
SELECT 
    date_trunc('month', created_at) AS review_month,
    avg(rating) AS avg_product_rating,             -- Đánh giá sản phẩm trung bình (1-5 sao)
    avg(delivery_rating) AS avg_delivery_rating,   -- Đánh giá dịch vụ vận chuyển
    avg(seller_rating) AS avg_seller_rating,       -- Đánh giá thái độ shop
    count(review_id) AS total_reviews
FROM delta.gold.fct_product_reviews
GROUP BY 1
ORDER BY review_month;
```

#### 👉 Truy vấn trên lớp Silver (`product_reviews`):
```sql
SELECT 
    date_trunc('month', reviewed_at) AS review_month,
    avg(rating) AS avg_product_rating,
    avg(delivery_rating) AS avg_delivery_rating,
    avg(seller_rating) AS avg_seller_rating,
    count(review_id) AS total_reviews
FROM delta.silver.product_reviews
GROUP BY 1
ORDER BY review_month;
```

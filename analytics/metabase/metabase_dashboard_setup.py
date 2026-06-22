#!/usr/bin/env python3
"""
metabase_dashboard_setup.py
============================
Tự động tạo Dashboard "Ecommerce Lakehouse Analytics" hoàn chỉnh trên Metabase
thông qua REST API.  Script tạo ~30 câu truy vấn SQL (questions/cards), phân bố
vào 5 tab trên một dashboard duy nhất với bố cục grid tự động.

Cách sử dụng:
    # Chạy thật
    python metabase_dashboard_setup.py

    # Dry-run (chỉ hiển thị, không tạo)
    python metabase_dashboard_setup.py --dry-run

    # Tùy chỉnh endpoint
    python metabase_dashboard_setup.py --metabase-url http://localhost:3001 \\
        --metabase-user admin@example.com --metabase-password yourpassword

Yêu cầu: pip install requests
"""

import argparse
import json
import os
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Cấu hình mặc định
# ---------------------------------------------------------------------------
DEFAULT_METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3001")
DEFAULT_METABASE_USER = os.getenv("METABASE_USER", "admin@example.com")
DEFAULT_METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "password123")

COLLECTION_NAME = "Ecommerce Lakehouse Analytics"
DASHBOARD_NAME = "🏪 Ecommerce Lakehouse — Executive Dashboard"

TAB_NAMES = [
    "Tổng Quan Kinh Doanh",
    "Sản Phẩm & Đánh Giá",
    "Phân Tích Người Bán",
    "Vận Hành & Logistics",
    "Phân Tích Khách Hàng",
]

# ---------------------------------------------------------------------------
# Định nghĩa toàn bộ cards (questions)
# ---------------------------------------------------------------------------
# Mỗi card: name, sql, display, tab, size_x, size_y
# display hợp lệ: scalar, line, bar, pie, row, table, scatter, area, combo
CARDS = [
    # ===== Tab 1: Tổng Quan Kinh Doanh =====
    {
        "name": "💰 Tổng GMV",
        "sql": (
            "SELECT sum(total_amount) AS \"Tổng GMV (VNĐ)\"\n"
            "FROM delta.gold.fct_orders\n"
            "WHERE order_status = 'completed'"
        ),
        "display": "scalar",
        "tab": "Tổng Quan Kinh Doanh",
        "size_x": 4,
        "size_y": 3,
    },
    {
        "name": "📦 Tổng Đơn Hàng",
        "sql": (
            "SELECT count(order_id) AS \"Tổng đơn hoàn thành\"\n"
            "FROM delta.gold.fct_orders\n"
            "WHERE order_status = 'completed'"
        ),
        "display": "scalar",
        "tab": "Tổng Quan Kinh Doanh",
        "size_x": 4,
        "size_y": 3,
    },
    {
        "name": "🛒 AOV (Giá trị đơn TB)",
        "sql": (
            "SELECT sum(total_amount) / count(order_id) AS \"Giá trị đơn TB (VNĐ)\"\n"
            "FROM delta.gold.fct_orders\n"
            "WHERE order_status = 'completed'"
        ),
        "display": "scalar",
        "tab": "Tổng Quan Kinh Doanh",
        "size_x": 4,
        "size_y": 3,
    },
    {
        "name": "❌ Tỷ lệ hủy đơn (%)",
        "sql": (
            "SELECT\n"
            "    ROUND(\n"
            "        CAST(count(CASE WHEN order_status = 'cancelled' THEN 1 END) AS DOUBLE)\n"
            "        / count(order_id) * 100, 2\n"
            "    ) AS \"Tỷ lệ hủy (%)\"\n"
            "FROM delta.gold.fct_orders"
        ),
        "display": "scalar",
        "tab": "Tổng Quan Kinh Doanh",
        "size_x": 4,
        "size_y": 3,
    },
    {
        "name": "📈 Doanh thu theo tháng",
        "sql": (
            "SELECT\n"
            "    date_trunc('month', created_at) AS \"Tháng\",\n"
            "    sum(total_amount) AS \"Doanh thu (VNĐ)\",\n"
            "    count(order_id) AS \"Số đơn hàng\"\n"
            "FROM delta.gold.fct_orders\n"
            "WHERE order_status = 'completed'\n"
            "GROUP BY 1\n"
            "ORDER BY 1"
        ),
        "display": "line",
        "tab": "Tổng Quan Kinh Doanh",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "🍩 Phân bổ trạng thái đơn hàng",
        "sql": (
            "SELECT\n"
            "    order_status AS \"Trạng thái\",\n"
            "    count(order_id) AS \"Số đơn\"\n"
            "FROM delta.gold.fct_orders\n"
            "GROUP BY order_status\n"
            "ORDER BY \"Số đơn\" DESC"
        ),
        "display": "pie",
        "tab": "Tổng Quan Kinh Doanh",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "📊 Tăng trưởng MoM",
        "sql": (
            "WITH monthly AS (\n"
            "    SELECT\n"
            "        date_trunc('month', created_at) AS thang,\n"
            "        sum(total_amount) AS doanh_thu,\n"
            "        count(order_id) AS so_don\n"
            "    FROM delta.gold.fct_orders\n"
            "    WHERE order_status = 'completed'\n"
            "    GROUP BY 1\n"
            ")\n"
            "SELECT\n"
            "    thang AS \"Tháng\",\n"
            "    doanh_thu AS \"Doanh thu (VNĐ)\",\n"
            "    so_don AS \"Số đơn\",\n"
            "    LAG(doanh_thu) OVER (ORDER BY thang) AS \"DT tháng trước\",\n"
            "    ROUND(\n"
            "        (doanh_thu - LAG(doanh_thu) OVER (ORDER BY thang))\n"
            "        / NULLIF(LAG(doanh_thu) OVER (ORDER BY thang), 0) * 100,\n"
            "    2) AS \"Tăng trưởng MoM (%)\"\n"
            "FROM monthly\n"
            "ORDER BY 1"
        ),
        "display": "bar",
        "tab": "Tổng Quan Kinh Doanh",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "📊 Doanh thu theo nền tảng",
        "sql": (
            "SELECT\n"
            "    date_trunc('month', o.created_at) AS \"Tháng\",\n"
            "    p.platform_name AS \"Nền tảng\",\n"
            "    sum(o.total_amount) AS \"Doanh thu (VNĐ)\"\n"
            "FROM delta.gold.fct_orders o\n"
            "LEFT JOIN delta.gold.dim_platforms p ON o.platform_id = p.platform_id\n"
            "WHERE o.order_status = 'completed'\n"
            "GROUP BY 1, 2\n"
            "ORDER BY 1, 2"
        ),
        "display": "bar",
        "tab": "Tổng Quan Kinh Doanh",
        "size_x": 9,
        "size_y": 6,
    },
    # ===== Tab 2: Sản Phẩm & Đánh Giá =====
    {
        "name": "🏆 Top 10 SP bán chạy nhất",
        "sql": (
            "SELECT\n"
            "    dp.product_name AS \"Sản phẩm\",\n"
            "    sum(oi.quantity) AS \"Đã bán\",\n"
            "    sum(oi.quantity * oi.unit_price) AS \"Doanh thu (VNĐ)\"\n"
            "FROM delta.gold.fct_order_items oi\n"
            "JOIN delta.gold.dim_products dp ON oi.product_id = dp.product_id\n"
            "GROUP BY dp.product_name\n"
            "ORDER BY \"Đã bán\" DESC\n"
            "LIMIT 10"
        ),
        "display": "row",
        "tab": "Sản Phẩm & Đánh Giá",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "🏷️ Top 10 Thương hiệu theo doanh thu",
        "sql": (
            "SELECT\n"
            "    db.brand_name AS \"Thương hiệu\",\n"
            "    sum(oi.net_amount) AS \"Doanh thu (VNĐ)\"\n"
            "FROM delta.gold.fct_order_items oi\n"
            "JOIN delta.gold.dim_products dp ON oi.product_id = dp.product_id\n"
            "JOIN delta.gold.dim_brands db ON dp.brand_id = db.brand_id\n"
            "GROUP BY db.brand_name\n"
            "ORDER BY \"Doanh thu (VNĐ)\" DESC\n"
            "LIMIT 10"
        ),
        "display": "bar",
        "tab": "Sản Phẩm & Đánh Giá",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "⭐ Phân bổ đánh giá sản phẩm (1–5 sao)",
        "sql": (
            "SELECT\n"
            "    rating AS \"Số sao\",\n"
            "    count(review_id) AS \"Số lượt đánh giá\"\n"
            "FROM delta.gold.fct_product_reviews\n"
            "GROUP BY rating\n"
            "ORDER BY rating"
        ),
        "display": "bar",
        "tab": "Sản Phẩm & Đánh Giá",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "📈 Điểm đánh giá trung bình theo tháng",
        "sql": (
            "SELECT\n"
            "    date_trunc('month', created_at) AS \"Tháng\",\n"
            "    ROUND(avg(rating), 2) AS \"TB sản phẩm\",\n"
            "    ROUND(avg(delivery_rating), 2) AS \"TB vận chuyển\",\n"
            "    ROUND(avg(seller_rating), 2) AS \"TB người bán\",\n"
            "    count(review_id) AS \"Số đánh giá\"\n"
            "FROM delta.gold.fct_product_reviews\n"
            "GROUP BY 1\n"
            "ORDER BY 1"
        ),
        "display": "line",
        "tab": "Sản Phẩm & Đánh Giá",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "⚠️ Top 10 SP đánh giá thấp nhất",
        "sql": (
            "SELECT\n"
            "    dp.product_name AS \"Sản phẩm\",\n"
            "    dp.seller_id AS \"Mã người bán\",\n"
            "    ROUND(avg(r.rating), 2) AS \"Điểm TB\",\n"
            "    count(r.review_id) AS \"Số đánh giá\",\n"
            "    ROUND(avg(r.delivery_rating), 2) AS \"Điểm vận chuyển\"\n"
            "FROM delta.gold.fct_product_reviews r\n"
            "JOIN delta.gold.dim_products dp ON r.product_id = dp.product_id\n"
            "GROUP BY dp.product_name, dp.seller_id\n"
            "HAVING count(r.review_id) >= 5\n"
            "ORDER BY \"Điểm TB\" ASC\n"
            "LIMIT 10"
        ),
        "display": "table",
        "tab": "Sản Phẩm & Đánh Giá",
        "size_x": 18,
        "size_y": 6,
    },
    # ===== Tab 3: Phân Tích Người Bán =====
    {
        "name": "💎 Phân nhóm Shop theo Doanh thu",
        "sql": (
            "WITH seller_revenue AS (\n"
            "    SELECT seller_id, sum(total_amount) AS revenue\n"
            "    FROM delta.gold.fct_orders\n"
            "    WHERE order_status = 'completed'\n"
            "    GROUP BY seller_id\n"
            ")\n"
            "SELECT\n"
            "    CASE\n"
            "        WHEN revenue >= 100000000 THEN 'Kim Cương (≥100M)'\n"
            "        WHEN revenue >= 50000000 THEN 'Vàng (≥50M)'\n"
            "        WHEN revenue >= 10000000 THEN 'Bạc (≥10M)'\n"
            "        ELSE 'Phổ thông (<10M)'\n"
            "    END AS \"Hạng Shop\",\n"
            "    count(*) AS \"Số lượng\"\n"
            "FROM seller_revenue\n"
            "GROUP BY 1\n"
            "ORDER BY \"Số lượng\" DESC"
        ),
        "display": "pie",
        "tab": "Phân Tích Người Bán",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "🏅 Top 10 Người bán doanh thu cao nhất",
        "sql": (
            "SELECT\n"
            "    ds.seller_name AS \"Người bán\",\n"
            "    ds.platform_name AS \"Nền tảng\",\n"
            "    sum(o.total_amount) AS \"Doanh thu (VNĐ)\",\n"
            "    count(o.order_id) AS \"Số đơn\"\n"
            "FROM delta.gold.fct_orders o\n"
            "JOIN delta.gold.dim_sellers ds ON o.seller_id = ds.seller_id\n"
            "WHERE o.order_status = 'completed'\n"
            "GROUP BY ds.seller_name, ds.platform_name\n"
            "ORDER BY \"Doanh thu (VNĐ)\" DESC\n"
            "LIMIT 10"
        ),
        "display": "row",
        "tab": "Phân Tích Người Bán",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "📈 Phân tích Pareto 80/20",
        "sql": (
            "WITH seller_revenue AS (\n"
            "    SELECT seller_id, sum(total_amount) AS revenue\n"
            "    FROM delta.gold.fct_orders\n"
            "    WHERE order_status = 'completed'\n"
            "    GROUP BY seller_id\n"
            "),\n"
            "ranked AS (\n"
            "    SELECT\n"
            "        seller_id,\n"
            "        revenue,\n"
            "        sum(revenue) OVER (ORDER BY revenue DESC\n"
            "            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_rev,\n"
            "        sum(revenue) OVER () AS total_rev,\n"
            "        row_number() OVER (ORDER BY revenue DESC) AS rk,\n"
            "        count(*) OVER () AS total_sellers\n"
            "    FROM seller_revenue\n"
            ")\n"
            "SELECT\n"
            "    ROUND(CAST(rk AS DOUBLE) / total_sellers * 100, 2) AS \"% Lũy kế Seller\",\n"
            "    ROUND(cum_rev / total_rev * 100, 2) AS \"% Lũy kế Doanh thu\"\n"
            "FROM ranked\n"
            "ORDER BY rk"
        ),
        "display": "line",
        "tab": "Phân Tích Người Bán",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "⏱️ Survival Rate (Shop có đơn trong 30 ngày)",
        "sql": (
            "WITH first_order AS (\n"
            "    SELECT seller_id, min(created_at) AS first_order_at\n"
            "    FROM delta.gold.fct_orders\n"
            "    WHERE order_status = 'completed'\n"
            "    GROUP BY seller_id\n"
            "),\n"
            "survival AS (\n"
            "    SELECT\n"
            "        s.seller_id,\n"
            "        date_diff('day', s.created_at, fo.first_order_at) AS days_to_first_order\n"
            "    FROM delta.gold.dim_sellers s\n"
            "    LEFT JOIN first_order fo ON s.seller_id = fo.seller_id\n"
            ")\n"
            "SELECT\n"
            "    count(*) AS \"Tổng Shop\",\n"
            "    count(CASE WHEN days_to_first_order <= 30 THEN 1 END) AS \"Có đơn trong 30 ngày\",\n"
            "    ROUND(\n"
            "        CAST(count(CASE WHEN days_to_first_order <= 30 THEN 1 END) AS DOUBLE)\n"
            "        / count(*) * 100, 2\n"
            "    ) AS \"Tỷ lệ sống sót (%)\"\n"
            "FROM survival"
        ),
        "display": "scalar",
        "tab": "Phân Tích Người Bán",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "📍 Số lượng Seller theo tỉnh/thành",
        "sql": (
            "SELECT\n"
            "    province AS \"Tỉnh/Thành phố\",\n"
            "    count(seller_id) AS \"Số người bán\"\n"
            "FROM delta.gold.dim_sellers\n"
            "WHERE province IS NOT NULL\n"
            "GROUP BY province\n"
            "ORDER BY \"Số người bán\" DESC\n"
            "LIMIT 15"
        ),
        "display": "bar",
        "tab": "Phân Tích Người Bán",
        "size_x": 18,
        "size_y": 6,
    },
    # ===== Tab 4: Vận Hành & Logistics =====
    {
        "name": "🕐 Thời gian giao hàng TB (giờ)",
        "sql": (
            "SELECT ROUND(avg(delivery_duration_hours), 1) AS \"Thời gian giao TB (giờ)\"\n"
            "FROM delta.gold.fct_shipments\n"
            "WHERE delivered_at IS NOT NULL"
        ),
        "display": "scalar",
        "tab": "Vận Hành & Logistics",
        "size_x": 6,
        "size_y": 3,
    },
    {
        "name": "⏰ Tỷ lệ giao trễ (%)",
        "sql": (
            "SELECT ROUND(avg(is_delayed) * 100, 2) AS \"Tỷ lệ trễ (%)\"\n"
            "FROM delta.gold.fct_shipments\n"
            "WHERE delivered_at IS NOT NULL"
        ),
        "display": "scalar",
        "tab": "Vận Hành & Logistics",
        "size_x": 6,
        "size_y": 3,
    },
    {
        "name": "🚫 Tỷ lệ giao thất bại (%)",
        "sql": (
            "SELECT ROUND(\n"
            "    CAST(count(CASE WHEN status IN ('failed', 'returned') THEN 1 END) AS DOUBLE)\n"
            "    / count(shipment_id) * 100, 2\n"
            ") AS \"Tỷ lệ thất bại (%)\"\n"
            "FROM delta.gold.fct_shipments"
        ),
        "display": "scalar",
        "tab": "Vận Hành & Logistics",
        "size_x": 6,
        "size_y": 3,
    },
    {
        "name": "🚚 Hiệu suất đơn vị vận chuyển",
        "sql": (
            "SELECT\n"
            "    carrier_name AS \"Đơn vị vận chuyển\",\n"
            "    count(shipment_id) AS \"Tổng đơn giao\",\n"
            "    ROUND(avg(delivery_duration_hours), 1) AS \"Giao TB (giờ)\",\n"
            "    ROUND(avg(is_delayed) * 100, 2) AS \"Trễ hẹn (%)\",\n"
            "    ROUND(\n"
            "        CAST(count(CASE WHEN status IN ('failed', 'returned') THEN 1 END) AS DOUBLE)\n"
            "        / count(shipment_id) * 100, 2\n"
            "    ) AS \"Thất bại (%)\"\n"
            "FROM delta.gold.fct_shipments\n"
            "GROUP BY carrier_name\n"
            "ORDER BY \"Giao TB (giờ)\""
        ),
        "display": "bar",
        "tab": "Vận Hành & Logistics",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "📊 Trạng thái vận chuyển theo tháng",
        "sql": (
            "SELECT\n"
            "    date_trunc('month', created_at) AS \"Tháng\",\n"
            "    status AS \"Trạng thái\",\n"
            "    count(shipment_id) AS \"Số đơn giao\"\n"
            "FROM delta.gold.fct_shipments\n"
            "GROUP BY 1, 2\n"
            "ORDER BY 1, 2"
        ),
        "display": "bar",
        "tab": "Vận Hành & Logistics",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "⏳ Phân bổ thời gian giao hàng",
        "sql": (
            "SELECT\n"
            "    CASE\n"
            "        WHEN delivery_duration_hours <= 24 THEN '0-24h'\n"
            "        WHEN delivery_duration_hours <= 48 THEN '24-48h'\n"
            "        WHEN delivery_duration_hours <= 72 THEN '48-72h'\n"
            "        WHEN delivery_duration_hours <= 120 THEN '72-120h'\n"
            "        ELSE '>120h'\n"
            "    END AS \"Khoảng thời gian\",\n"
            "    count(shipment_id) AS \"Số đơn giao\"\n"
            "FROM delta.gold.fct_shipments\n"
            "WHERE delivered_at IS NOT NULL\n"
            "GROUP BY 1\n"
            "ORDER BY\n"
            "    CASE\n"
            "        WHEN delivery_duration_hours <= 24 THEN 1\n"
            "        WHEN delivery_duration_hours <= 48 THEN 2\n"
            "        WHEN delivery_duration_hours <= 72 THEN 3\n"
            "        WHEN delivery_duration_hours <= 120 THEN 4\n"
            "        ELSE 5\n"
            "    END"
        ),
        "display": "bar",
        "tab": "Vận Hành & Logistics",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "📉 Tỷ lệ hủy/hoàn đơn theo tháng",
        "sql": (
            "SELECT\n"
            "    date_trunc('month', created_at) AS \"Tháng\",\n"
            "    ROUND(\n"
            "        CAST(count(CASE WHEN order_status = 'cancelled' THEN 1 END) AS DOUBLE)\n"
            "        / count(order_id) * 100, 2\n"
            "    ) AS \"Tỷ lệ hủy (%)\",\n"
            "    ROUND(\n"
            "        CAST(count(CASE WHEN order_status = 'returned' THEN 1 END) AS DOUBLE)\n"
            "        / count(order_id) * 100, 2\n"
            "    ) AS \"Tỷ lệ hoàn (%)\"\n"
            "FROM delta.gold.fct_orders\n"
            "GROUP BY 1\n"
            "ORDER BY 1"
        ),
        "display": "area",
        "tab": "Vận Hành & Logistics",
        "size_x": 9,
        "size_y": 6,
    },
    # ===== Tab 5: Phân Tích Khách Hàng =====
    {
        "name": "👫 Phân bổ khách hàng theo giới tính",
        "sql": (
            "SELECT\n"
            "    COALESCE(gender, 'Không xác định') AS \"Giới tính\",\n"
            "    count(customer_id) AS \"Số khách hàng\"\n"
            "FROM delta.gold.dim_customers\n"
            "GROUP BY 1\n"
            "ORDER BY \"Số khách hàng\" DESC"
        ),
        "display": "pie",
        "tab": "Phân Tích Khách Hàng",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "📍 Khách hàng theo tỉnh/thành",
        "sql": (
            "SELECT\n"
            "    COALESCE(primary_province, 'N/A') AS \"Tỉnh/Thành phố\",\n"
            "    count(customer_id) AS \"Số khách hàng\"\n"
            "FROM delta.gold.dim_customers\n"
            "WHERE primary_province IS NOT NULL\n"
            "GROUP BY 1\n"
            "ORDER BY \"Số khách hàng\" DESC\n"
            "LIMIT 15"
        ),
        "display": "bar",
        "tab": "Phân Tích Khách Hàng",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "⊙ Phân tích RFM khách hàng",
        "sql": (
            "SELECT\n"
            "    customer_id AS \"Mã KH\",\n"
            "    date_diff('day', max(created_at), current_timestamp) AS \"Gần đây (ngày)\",\n"
            "    count(order_id) AS \"Tần suất mua\",\n"
            "    sum(total_amount) AS \"Tổng chi tiêu (VNĐ)\"\n"
            "FROM delta.gold.fct_orders\n"
            "WHERE order_status = 'completed'\n"
            "GROUP BY customer_id"
        ),
        "display": "scatter",
        "tab": "Phân Tích Khách Hàng",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "💳 Phương thức thanh toán phổ biến",
        "sql": (
            "SELECT\n"
            "    payment_method AS \"Phương thức\",\n"
            "    count(payment_id) AS \"Số giao dịch\",\n"
            "    sum(amount) AS \"Tổng tiền (VNĐ)\"\n"
            "FROM delta.silver.payments\n"
            "WHERE status = 'completed'\n"
            "GROUP BY payment_method\n"
            "ORDER BY \"Số giao dịch\" DESC"
        ),
        "display": "pie",
        "tab": "Phân Tích Khách Hàng",
        "size_x": 9,
        "size_y": 6,
    },
    {
        "name": "👑 Top 10 Khách hàng VIP (chi tiêu cao nhất)",
        "sql": (
            "SELECT\n"
            "    c.full_name AS \"Họ tên\",\n"
            "    c.primary_city AS \"Thành phố\",\n"
            "    count(o.order_id) AS \"Số đơn\",\n"
            "    sum(o.total_amount) AS \"Tổng chi tiêu (VNĐ)\",\n"
            "    ROUND(avg(o.total_amount), 0) AS \"Giá trị đơn TB (VNĐ)\"\n"
            "FROM delta.gold.fct_orders o\n"
            "JOIN delta.gold.dim_customers c ON o.customer_id = c.customer_id\n"
            "WHERE o.order_status = 'completed'\n"
            "GROUP BY c.full_name, c.primary_city\n"
            "ORDER BY \"Tổng chi tiêu (VNĐ)\" DESC\n"
            "LIMIT 10"
        ),
        "display": "table",
        "tab": "Phân Tích Khách Hàng",
        "size_x": 18,
        "size_y": 6,
    },
]


# ---------------------------------------------------------------------------
# MetabaseClient — giao tiếp với Metabase REST API
# ---------------------------------------------------------------------------
class MetabaseClient:
    """Thin wrapper quanh Metabase REST API."""

    def __init__(self, base_url: str, user: str, password: str, dry_run: bool = False):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.dry_run = dry_run
        self.session = requests.Session()
        self.token = None

    # -- Auth ---------------------------------------------------------------
    def login(self):
        """Đăng nhập và lấy session token."""
        print(f"🔐 Đang đăng nhập Metabase tại {self.base_url} ...")
        if self.dry_run:
            print("   [DRY-RUN] Bỏ qua đăng nhập.")
            self.token = "dry-run-token"
            return
        resp = self.session.post(
            f"{self.base_url}/api/session",
            json={"username": self.user, "password": self.password},
        )
        resp.raise_for_status()
        self.token = resp.json()["id"]
        self.session.headers["X-Metabase-Session"] = self.token
        print("   ✅ Đăng nhập thành công!")

    # -- Helpers ------------------------------------------------------------
    def _get(self, path: str):
        resp = self.session.get(f"{self.base_url}{path}")
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: dict):
        resp = self.session.post(f"{self.base_url}{path}", json=payload)
        resp.raise_for_status()
        return resp.json()

    def _put(self, path: str, payload: dict):
        resp = self.session.put(f"{self.base_url}{path}", json=payload)
        resp.raise_for_status()
        return resp.json()

    # -- Database -----------------------------------------------------------
    def find_trino_database_id(self) -> int:
        """Tìm database ID của Trino/Starburst trong Metabase."""
        print("🔍 Đang tìm database Trino ...")
        if self.dry_run:
            print("   [DRY-RUN] Giả lập database_id = 1")
            return 1
        databases = self._get("/api/database")
        db_list = databases.get("data", databases) if isinstance(databases, dict) else databases
        for db in db_list:
            engine = db.get("engine", "").lower()
            name = db.get("name", "").lower()
            # Trino hiển thị là "starburst" hoặc "presto" trên Metabase,
            # hoặc tên chứa "trino"
            if engine in ("starburst", "presto") or "trino" in name:
                print(f"   ✅ Tìm thấy database: '{db['name']}' (id={db['id']}, engine={db['engine']})")
                return db["id"]
        # Fallback: trả về database đầu tiên nếu không match
        if db_list:
            first = db_list[0]
            print(
                f"   ⚠️  Không tìm thấy Trino. Dùng database đầu tiên: "
                f"'{first['name']}' (id={first['id']}, engine={first.get('engine')})"
            )
            return first["id"]
        raise RuntimeError("Không tìm thấy database nào trên Metabase!")

    # -- Collection ---------------------------------------------------------
    def find_or_create_collection(self, name: str) -> int:
        """Tạo collection nếu chưa tồn tại, trả về collection_id."""
        print(f"📂 Đang kiểm tra collection '{name}' ...")
        if self.dry_run:
            print(f"   [DRY-RUN] Giả lập collection_id = 100")
            return 100
        collections = self._get("/api/collection")
        for c in collections:
            if c.get("name") == name and not c.get("archived"):
                print(f"   ✅ Collection đã tồn tại (id={c['id']})")
                return c["id"]
        result = self._post("/api/collection", {"name": name, "color": "#509EE3"})
        cid = result["id"]
        print(f"   ✅ Đã tạo collection mới (id={cid})")
        return cid

    # -- Card (Question) ----------------------------------------------------
    def create_card(self, name: str, sql: str, display: str, db_id: int, collection_id: int) -> int:
        """Tạo một SQL question (card)."""
        print(f"   📝 Tạo card: {name} ({display}) ...", end=" ")
        if self.dry_run:
            print("[DRY-RUN]")
            return -1

        # Visualization settings cơ bản theo loại biểu đồ
        viz_settings = {}
        if display == "scalar":
            viz_settings = {"scalar.field": "gmv"}
        elif display == "pie":
            viz_settings = {"pie.show_legend": True}

        payload = {
            "name": name,
            "dataset_query": {
                "type": "native",
                "native": {"query": sql},
                "database": db_id,
            },
            "display": display,
            "visualization_settings": viz_settings,
            "collection_id": collection_id,
        }
        try:
            result = self._post("/api/card", payload)
            card_id = result["id"]
            print(f"✅ (id={card_id})")
            return card_id
        except requests.HTTPError as e:
            print(f"❌ Lỗi: {e}")
            if e.response is not None:
                print(f"      Chi tiết: {e.response.text[:300]}")
            return -1

    # -- Dashboard ----------------------------------------------------------
    def find_or_create_dashboard(self, name: str, collection_id: int) -> int:
        """Tạo dashboard nếu chưa tồn tại."""
        print(f"📋 Đang kiểm tra dashboard '{name}' ...")
        if self.dry_run:
            print(f"   [DRY-RUN] Giả lập dashboard_id = 200")
            return 200
        # Tìm dashboard hiện có
        dashboards = self._get("/api/dashboard")
        for d in dashboards:
            if d.get("name") == name and not d.get("archived"):
                print(f"   ✅ Dashboard đã tồn tại (id={d['id']})")
                return d["id"]
        result = self._post(
            "/api/dashboard",
            {"name": name, "collection_id": collection_id},
        )
        did = result["id"]
        print(f"   ✅ Đã tạo dashboard mới (id={did})")
        return did

    def bulk_update_dashboard(
        self,
        dashboard_id: int,
        tab_names: list[str],
        cards_layout: list[dict],
        card_ids: dict,
    ):
        """
        Cập nhật dashboard bằng một lệnh PUT duy nhất (tương thích Metabase v0.61+).

        Metabase v0.61 yêu cầu gửi tabs và dashcards cùng nhau trong cùng 1 request.
        Sử dụng ID tạm âm (negative temporary IDs) cho cả tabs và dashcards.
        """
        print(f"📑 Đang tạo {len(tab_names)} tab + gắn {len(cards_layout)} cards ...")
        if self.dry_run:
            for i, name in enumerate(tab_names):
                print(f"   [DRY-RUN] Tab: {name} → temp_id={-(i+1)}")
            for item in cards_layout:
                print(
                    f"   [DRY-RUN] {item['name']:<45} "
                    f"→ tab={item['tab_name']:<25} "
                    f"col={item['col']:>2}, row={item['row']:>2}, "
                    f"size=({item['size_x']}×{item['size_y']})"
                )
            return

        # Tạo mapping tab_name → negative temp ID
        tab_temp_ids = {}
        tabs_payload = []
        for i, name in enumerate(tab_names):
            temp_id = -(i + 1)
            tab_temp_ids[name] = temp_id
            tabs_payload.append({"id": temp_id, "name": name})

        # Tạo dashcards payload với negative temp IDs
        dashcards_payload = []
        for idx, item in enumerate(cards_layout):
            cid = card_ids.get(item["name"], -1)
            if cid <= 0:
                continue  # Bỏ qua card bị lỗi khi tạo
            tab_temp_id = tab_temp_ids.get(item["tab_name"])
            dashcards_payload.append({
                "id": -(idx + 1),
                "card_id": cid,
                "size_x": item["size_x"],
                "size_y": item["size_y"],
                "col": item["col"],
                "row": item["row"],
                "dashboard_tab_id": tab_temp_id,
            })

        # Gửi 1 request duy nhất
        try:
            result = self._put(f"/api/dashboard/{dashboard_id}", {
                "tabs": tabs_payload,
                "dashcards": dashcards_payload,
            })
            actual_tabs = result.get("tabs", [])
            actual_dashcards = result.get("dashcards", [])
            print(f"   ✅ Tabs tạo thành công: {len(actual_tabs)}")
            for t in actual_tabs:
                print(f"      📑 {t['name']} (id={t['id']})")
            print(f"   ✅ Dashcards gắn thành công: {len(actual_dashcards)}")
        except requests.HTTPError as e:
            print(f"   ❌ Lỗi cập nhật dashboard: {e}")
            if e.response is not None:
                print(f"      Chi tiết: {e.response.text[:500]}")


# ---------------------------------------------------------------------------
# Layout calculator — tính toán vị trí grid tự động
# ---------------------------------------------------------------------------
def compute_layout(cards: list[dict], tab_names: list[str]) -> list[dict]:
    """
    Tính toán col/row cho từng card trên grid 18 cột của Metabase.
    Cards cùng tab được xếp liên tiếp, tự động xuống hàng khi hết chỗ.
    Mỗi tab bắt đầu row=0 riêng biệt.
    """
    # Nhóm cards theo tab
    tab_cards = {tab: [] for tab in tab_names}
    for card in cards:
        tab_name = card["tab"]
        if tab_name in tab_cards:
            tab_cards[tab_name].append(card)

    result = []
    for tab_name, tab_card_list in tab_cards.items():
        current_col = 0
        current_row = 0
        max_row_height = 0

        for card in tab_card_list:
            sx = card["size_x"]
            sy = card["size_y"]

            # Nếu card không vừa hàng hiện tại, xuống hàng mới
            if current_col + sx > 18:
                current_row += max_row_height
                current_col = 0
                max_row_height = 0

            result.append({
                **card,
                "col": current_col,
                "row": current_row,
                "tab_name": tab_name,
            })

            current_col += sx
            max_row_height = max(max_row_height, sy)

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Tự động tạo Dashboard Ecommerce Lakehouse trên Metabase."
    )
    parser.add_argument(
        "--metabase-url",
        default=DEFAULT_METABASE_URL,
        help=f"URL Metabase (mặc định: {DEFAULT_METABASE_URL})",
    )
    parser.add_argument(
        "--metabase-user",
        default=DEFAULT_METABASE_USER,
        help="Email đăng nhập Metabase.",
    )
    parser.add_argument(
        "--metabase-password",
        default=DEFAULT_METABASE_PASSWORD,
        help="Mật khẩu đăng nhập Metabase.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chỉ hiển thị kế hoạch, không tạo thật trên Metabase.",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("🏪 METABASE DASHBOARD SETUP — Ecommerce Lakehouse Analytics")
    print("=" * 70)
    print(f"   URL:      {args.metabase_url}")
    print(f"   User:     {args.metabase_user}")
    print(f"   Dry-run:  {args.dry_run}")
    print(f"   Cards:    {len(CARDS)}")
    print(f"   Tabs:     {len(TAB_NAMES)}")
    print("=" * 70)

    client = MetabaseClient(
        base_url=args.metabase_url,
        user=args.metabase_user,
        password=args.metabase_password,
        dry_run=args.dry_run,
    )

    # Bước 1: Đăng nhập
    client.login()

    # Bước 2: Tìm database Trino
    db_id = client.find_trino_database_id()

    # Bước 3: Tạo collection
    collection_id = client.find_or_create_collection(COLLECTION_NAME)

    # Bước 4: Tạo tất cả cards
    print(f"\n{'='*70}")
    print(f"📝 ĐANG TẠO {len(CARDS)} CARDS (SQL Questions)")
    print(f"{'='*70}")
    card_ids = {}
    for i, card_def in enumerate(CARDS, 1):
        card_id = client.create_card(
            name=card_def["name"],
            sql=card_def["sql"],
            display=card_def["display"],
            db_id=db_id,
            collection_id=collection_id,
        )
        card_ids[card_def["name"]] = card_id
        # Tránh rate limiting
        if not args.dry_run:
            time.sleep(0.3)

    # Bước 5: Tạo dashboard
    print(f"\n{'='*70}")
    print("📋 ĐANG TẠO DASHBOARD")
    print(f"{'='*70}")
    dashboard_id = client.find_or_create_dashboard(DASHBOARD_NAME, collection_id)

    # Bước 6: Tính toán layout
    print(f"\n{'='*70}")
    print("📐 ĐANG SẮP XẾP BỐ CỤC VÀ GẮN CARDS")
    print(f"{'='*70}")
    layout = compute_layout(CARDS, TAB_NAMES)

    # Bước 7: Gửi 1 bulk PUT để tạo tabs + gắn cards cùng lúc
    client.bulk_update_dashboard(
        dashboard_id=dashboard_id,
        tab_names=TAB_NAMES,
        cards_layout=layout,
        card_ids=card_ids,
    )

    # Hoàn tất
    print(f"\n{'='*70}")
    print("🎉 HOÀN TẤT!")
    print(f"{'='*70}")
    success_count = sum(1 for v in card_ids.values() if v > 0)
    fail_count = sum(1 for v in card_ids.values() if v <= 0)
    if args.dry_run:
        print(f"   [DRY-RUN] Sẽ tạo {len(CARDS)} cards trên {len(TAB_NAMES)} tabs.")
        print(f"   Chạy lại KHÔNG có --dry-run để tạo thật.")
    else:
        print(f"   ✅ Tạo thành công: {success_count} cards")
        if fail_count:
            print(f"   ❌ Thất bại:       {fail_count} cards")
        print(f"   📋 Dashboard ID:  {dashboard_id}")
        print(f"   🔗 Truy cập:      {client.base_url}/dashboard/{dashboard_id}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()

import psycopg2
from faker import Faker
import time
import random
from datetime import datetime

# Khởi tạo thư viện Faker với ngôn ngữ Việt Nam
fake = Faker('vi_VN')

# Thông tin kết nối tới postgres-data-source (cổng host 5433)
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "data-source"
DB_USER = "postgres"
DB_PASS = "postgres"

def get_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        print(f"❌ Lỗi kết nối database: {e}")
        return None

def seed_base_data(cursor):
    """Khởi tạo dữ liệu cơ bản (Catalog, Sellers, Customers) để đảm bảo không bị lỗi khóa ngoại."""
    print("------------------------------------------------------------")
    print("🚀 BẮT ĐẦU KIỂM TRA & SEED DỮ LIỆU BAN ĐẦU (seeding base data)...")
    print("------------------------------------------------------------")

    # 1. Seed Platforms
    cursor.execute("SELECT COUNT(*) FROM platforms")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu platforms...")
        platforms = [
            ('tiki', 'Tiki', 'https://tiki.vn'),
            ('shopee', 'Shopee', 'https://shopee.vn'),
            ('sendo', 'Sendo', 'https://sendo.vn')
        ]
        cursor.executemany(
            "INSERT INTO platforms (platform_code, platform_name, base_url) VALUES (%s, %s, %s)",
            platforms
        )

    # Lấy IDs platforms
    cursor.execute("SELECT platform_id, platform_code FROM platforms WHERE is_active = TRUE")
    platform_map = {code: pid for pid, code in cursor.fetchall()}

    # 2. Seed Sellers
    cursor.execute("SELECT COUNT(*) FROM sellers")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu sellers...")
        sellers = []
        for code, pid in platform_map.items():
            for i in range(5): # 5 sellers mỗi sàn
                sellers.append((
                    pid,
                    f"seller_{code}_{i}",
                    fake.company(),
                    fake.city(),
                    fake.state(),
                    random.randint(100, 50000),
                    random.choice([True, False])
                ))
        cursor.executemany(
            """INSERT INTO sellers (platform_id, platform_seller_id, seller_name, city, province, follower_count, is_official_store) 
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            sellers
        )

    # 3. Seed Categories
    cursor.execute("SELECT COUNT(*) FROM categories")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu categories...")
        categories = []
        cat_names = ["Điện Thoại - Máy Tính Bảng", "Thời Trang Nam", "Thời Trang Nữ", "Nhà Cửa - Đời Sống", "Đồ Chơi - Mẹ & Bé"]
        for code, pid in platform_map.items():
            for idx, cat_name in enumerate(cat_names):
                categories.append((
                    pid,
                    f"cat_{code}_{idx}",
                    cat_name
                ))
        cursor.executemany(
            "INSERT INTO categories (platform_id, platform_category_id, category_name) VALUES (%s, %s, %s)",
            categories
        )

    # 4. Seed Brands
    cursor.execute("SELECT COUNT(*) FROM brands")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu brands...")
        brands = []
        brand_names = ["Samsung", "Apple", "Xiaomi", "Coolmate", "Lock&Lock", "LEGO"]
        for code, pid in platform_map.items():
            for b_name in brand_names:
                brands.append((
                    pid,
                    f"brand_{code}_{b_name.lower()}",
                    b_name,
                    random.choice(["South Korea", "USA", "China", "Vietnam", "Denmark"]),
                    random.choice([True, False])
                ))
        cursor.executemany(
            """INSERT INTO brands (platform_id, platform_brand_id, brand_name, country, is_official)
               VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
            brands
        )

    # 5. Seed Customers & Addresses
    cursor.execute("SELECT COUNT(*) FROM customers")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu customers & addresses...")
        for code, pid in platform_map.items():
            for i in range(15): # 15 khách hàng mỗi sàn
                gender = random.choice(['male', 'female', 'other', 'unknown'])
                cursor.execute(
                    """INSERT INTO customers (platform_id, platform_customer_id, full_name, email, phone_number, gender, date_of_birth)
                       VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING customer_id""",
                    (
                        pid,
                        f"cust_{code}_{i}",
                        fake.name(),
                        fake.free_email(),
                        fake.phone_number(),
                        gender,
                        fake.date_of_birth(minimum_age=18, maximum_age=60)
                    )
                )
                cust_id = cursor.fetchone()[0]
                
                # Thêm 1-2 địa chỉ nhận hàng
                for j in range(random.randint(1, 2)):
                    cursor.execute(
                        """INSERT INTO customer_addresses (customer_id, recipient_name, phone_number, address_line, ward, district, city, province, is_default)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            cust_id,
                            fake.name(),
                            fake.phone_number(),
                            fake.street_address(),
                            "Phường " + str(random.randint(1, 15)),
                            "Quận " + str(random.randint(1, 12)),
                            fake.city(),
                            fake.state(),
                            j == 0
                        )
                    )

    # 6. Seed Products & Variants & Inventory
    cursor.execute("SELECT COUNT(*) FROM products")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu products & variants & inventory...")
        cursor.execute("SELECT seller_id, platform_id FROM sellers")
        seller_list = cursor.fetchall()
        cursor.execute("SELECT category_id, platform_id FROM categories")
        category_list = cursor.fetchall()
        cursor.execute("SELECT brand_id, platform_id FROM brands")
        brand_list = cursor.fetchall()

        prod_names = {
            "Điện Thoại - Máy Tính Bảng": ["iPhone 15 Pro Max", "Samsung Galaxy S24 Ultra", "Xiaomi Redmi Note 13"],
            "Thời Trang Nam": ["Áo thun thể thao nam", "Quần short kaki nam", "Áo khoác gió bomber"],
            "Thời Trang Nữ": ["Váy hoa dáng xòe", "Đầm dự tiệc trễ vai", "Áo sơ mi lụa nữ"],
            "Nhà Cửa - Đời Sống": ["Bình giữ nhiệt Lock&Lock", "Bộ lau nhà tự vắt", "Đèn học chống cận"],
            "Đồ Chơi - Mẹ & Bé": ["Bộ đồ chơi LEGO City", "Tã quần Bobby size XL", "Sữa bột Frisolac"]
        }

        for idx, (seller_id, platform_id) in enumerate(seller_list):
            sub_cats = [c for c in category_list if c[1] == platform_id]
            sub_brands = [b for b in brand_list if b[1] == platform_id]
            if not sub_cats or not sub_brands: continue

            # Tạo 3 sản phẩm cho mỗi seller
            for p_idx in range(3):
                cat_id, _ = random.choice(sub_cats)
                brand_id, _ = random.choice(sub_brands)

                # Lấy tên danh mục để đặt tên sản phẩm cho phù hợp
                cursor.execute("SELECT category_name FROM categories WHERE category_id = %s", (cat_id,))
                cat_name = cursor.fetchone()[0]
                p_name = random.choice(prod_names.get(cat_name, ["Sản phẩm ecom mẫu " + str(p_idx)])) + f" (Cửa hàng {seller_id})"

                cursor.execute(
                    """INSERT INTO products (platform_product_id, seller_id, category_id, brand_id, product_name, description, status, is_authentic)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING product_id""",
                    (
                        f"prod_{seller_id}_{p_idx}",
                        seller_id,
                        cat_id,
                        brand_id,
                        p_name,
                        fake.text(max_nb_chars=200),
                        'active',
                        random.choice([True, False])
                    )
                )
                prod_id = cursor.fetchone()[0]

                # Tạo 1-2 variant cho mỗi sản phẩm
                for v_idx in range(random.randint(1, 2)):
                    original_price = random.randint(50, 2000) * 10000
                    sale_price = original_price * random.choice([0.8, 0.9, 1.0])
                    cursor.execute(
                        """INSERT INTO product_variants (product_id, platform_variant_id, sku, variant_name, original_price, sale_price, weight_gram, status)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING variant_id""",
                        (
                            prod_id,
                            f"var_{prod_id}_{v_idx}",
                            f"SKU-{seller_id}-{prod_id}-{v_idx}",
                            f"Màu sắc {v_idx} / Size {random.choice(['M', 'L', 'XL', '256GB', '512GB'])}",
                            original_price,
                            sale_price,
                            random.randint(100, 2000),
                            'active'
                        )
                    )
                    variant_id = cursor.fetchone()[0]

                    # Khởi tạo kho hàng
                    qty = random.randint(100, 500)
                    cursor.execute(
                        """INSERT INTO product_inventory (variant_id, quantity_on_hand, quantity_reserved, low_stock_threshold)
                           VALUES (%s, %s, %s, %s)""",
                        (
                            variant_id,
                            qty,
                            0,
                            15
                        )
                    )
    print("✅ HOÀN TẤT SEED DỮ LIỆU CƠ BẢN!")
    print("------------------------------------------------------------\n")

def simulate_streaming_data(conn):
    """Mô phỏng luồng dữ liệu e-commerce liên tục (views, carts, orders, reviews)."""
    cursor = conn.cursor()

    # Lấy danh sách ID cơ bản để giả lập
    cursor.execute("SELECT customer_id FROM customers")
    customer_ids = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT seller_id FROM sellers")
    seller_ids = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT product_id FROM products")
    product_ids = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT variant_id, sale_price FROM product_variants WHERE status='active'")
    variants = [(r[0], float(r[1])) for r in cursor.fetchall()]

    if not customer_ids or not product_ids or not variants:
        print("❌ Dữ liệu ban đầu không đầy đủ để mô phỏng. Vui lòng kiểm tra lại quá trình seed.")
        return

    print("⚡ Bắt đầu tạo luồng dữ liệu giả lập. Nhấn Ctrl+C để dừng...")
    try:
        while True:
            # Chọn loại hành vi ngẫu nhiên
            # Tần suất: View (50%), Add to Cart (25%), Purchase (15%), Review (10%)
            event_choice = random.choices(
                ['view', 'add_to_cart', 'purchase', 'review'],
                weights=[50, 25, 15, 10],
                k=1
            )[0]

            cust_id = random.choice(customer_ids)
            prod_id = random.choice(product_ids)
            var_id, unit_price = random.choice(variants)

            if event_choice == 'view':
                # Insert vào bảng events
                cursor.execute(
                    """INSERT INTO events (customer_id, product_id, variant_id, event_type)
                       VALUES (%s, %s, %s, 'view')""",
                    (cust_id, prod_id, var_id)
                )
                conn.commit()
                print(f"👁️ [VIEW]: Khách hàng {cust_id} đang xem sản phẩm {prod_id} (Phiên bản {var_id})")

            elif event_choice == 'add_to_cart':
                # Tạo hoặc lấy giỏ hàng của khách hàng
                cursor.execute(
                    """INSERT INTO carts (customer_id, status) VALUES (%s, 'active') 
                       ON CONFLICT (customer_id) DO UPDATE SET status='active' RETURNING cart_id""", 
                    (cust_id,)
                )
                cart_id = cursor.fetchone()[0]

                # Thêm vào bảng cart_items
                qty = random.randint(1, 3)
                cursor.execute(
                    """INSERT INTO cart_items (cart_id, variant_id, quantity, unit_price)
                       VALUES (%s, %s, %s, %s) ON CONFLICT (cart_id, variant_id) 
                       DO UPDATE SET quantity = cart_items.quantity + EXCLUDED.quantity""",
                    (cart_id, var_id, qty, unit_price)
                )
                conn.commit()
                print(f"🛒 [CART]: Khách hàng {cust_id} thêm {qty}x sản phẩm {var_id} vào giỏ hàng #{cart_id}")

            elif event_choice == 'purchase':
                # Lấy địa chỉ giao hàng mặc định của khách hàng
                cursor.execute("SELECT address_id FROM customer_addresses WHERE customer_id = %s AND is_default = TRUE LIMIT 1", (cust_id,))
                addr_row = cursor.fetchone()
                addr_id = addr_row[0] if addr_row else None

                # Lấy seller_id của sản phẩm
                cursor.execute("SELECT seller_id FROM products WHERE product_id = %s", (prod_id,))
                sell_id = cursor.fetchone()[0]

                qty = random.randint(1, 2)
                subtotal = unit_price * qty
                shipping_fee = 30000.0
                total = subtotal + shipping_fee

                # 1. Tạo đơn hàng (Order)
                cursor.execute(
                    """INSERT INTO orders (platform_order_id, customer_id, seller_id, shipping_address_id, order_status, subtotal_amount, shipping_fee, total_amount)
                       VALUES (%s, %s, %s, %s, 'pending', %s, %s, %s) RETURNING order_id""",
                    (
                        f"ord_{int(time.time())}_{random.randint(100, 999)}",
                        cust_id,
                        sell_id,
                        addr_id,
                        subtotal,
                        shipping_fee,
                        total
                    )
                )
                order_id = cursor.fetchone()[0]

                # 2. Tạo chi tiết đơn hàng (Order Item)
                cursor.execute(
                    """INSERT INTO order_items (order_id, variant_id, quantity, unit_price)
                       VALUES (%s, %s, %s, %s) RETURNING order_item_id""",
                    (order_id, var_id, qty, unit_price)
                )
                order_item_id = cursor.fetchone()[0]

                # 3. Trừ kho hàng (Product Inventory) & Tạo log biến động (Inventory Movement)
                cursor.execute("SELECT inventory_id, quantity_on_hand FROM product_inventory WHERE variant_id = %s LIMIT 1", (var_id,))
                inv_row = cursor.fetchone()
                if inv_row:
                    inv_id, q_on_hand = inv_row
                    if q_on_hand >= qty:
                        cursor.execute(
                            "UPDATE product_inventory SET quantity_on_hand = quantity_on_hand - %s WHERE inventory_id = %s",
                            (qty, inv_id)
                        )
                        cursor.execute(
                            """INSERT INTO inventory_movements (inventory_id, movement_type, quantity_delta, reference_type, reference_id, reason)
                               VALUES (%s, 'sale', %s, 'orders', %s, 'Trừ kho do đặt hàng')""",
                            (inv_id, -qty, str(order_id))
                        )

                # 4. Giả lập thanh toán (Payment) ngay lập tức
                pmeth = random.choice(['cod', 'card', 'bank_transfer', 'momo', 'zalopay'])
                pstatus = 'paid' if pmeth != 'cod' else 'pending'
                cursor.execute(
                    """INSERT INTO payments (order_id, payment_method, provider, amount, status, paid_at)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        order_id,
                        pmeth,
                        pmeth.upper(),
                        total,
                        pstatus,
                        datetime.now() if pstatus == 'paid' else None
                    )
                )

                # 5. Cập nhật trạng thái đơn hàng sau khi có thông tin thanh toán
                ostatus = 'confirmed' if pstatus == 'paid' else 'pending'
                cursor.execute("UPDATE orders SET order_status = %s WHERE order_id = %s", (ostatus, order_id))

                # 6. Tạo thông tin vận chuyển (Shipment)
                cursor.execute(
                    """INSERT INTO shipments (order_id, carrier_name, tracking_number, status)
                       VALUES (%s, %s, %s, 'preparing')""",
                    (
                        order_id,
                        random.choice(["Giao Hàng Nhanh", "Giao Hàng Tiết Kiệm", "Viettel Post", "J&T Express"]),
                        f"TRACK_{order_id}_{random.randint(10000, 99999)}",
                    )
                )

                conn.commit()
                print(f"🛍️ [ORDER]: Đơn hàng #{order_id} được tạo thành công! Khách {cust_id} mua {qty}x sản phẩm {var_id} | Tổng thanh toán: {int(total):,} VNĐ (Hình thức: {pmeth.upper()})")

            elif event_choice == 'review':
                # Tìm một mặt hàng bất kỳ khách hàng này từng mua để đánh giá
                cursor.execute(
                    """SELECT oi.order_item_id, o.order_id, p.product_id
                       FROM order_items oi
                       JOIN orders o ON oi.order_id = o.order_id
                       JOIN product_variants pv ON oi.variant_id = pv.variant_id
                       JOIN products p ON pv.product_id = p.product_id
                       WHERE o.customer_id = %s LIMIT 1""",
                    (cust_id,)
                )
                oi_row = cursor.fetchone()
                if oi_row:
                    oi_id, o_id, p_id = oi_row
                    rating = random.choices([5, 4, 3, 2, 1], weights=[70, 15, 10, 3, 2], k=1)[0]
                    cursor.execute(
                        """INSERT INTO product_reviews (platform_review_id, product_id, order_item_id, customer_id, rating, title, content)
                           VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                        (
                            f"rev_{oi_id}_{random.randint(10,99)}",
                            p_id,
                            oi_id,
                            cust_id,
                            rating,
                            fake.sentence(),
                            fake.text(max_nb_chars=120)
                        )
                    )
                    conn.commit()
                    print(f"⭐ [REVIEW]: Khách {cust_id} đánh giá {rating} sao cho sản phẩm {p_id} (Thuộc đơn hàng {o_id})")

            # Giả lập khoảng nghỉ ngẫu nhiên từ 0.5 giây đến 1.5 giây
            time.sleep(random.uniform(0.5, 1.5))

    except KeyboardInterrupt:
        print("\nĐã nhận lệnh dừng (Ctrl+C). Kết thúc luồng dữ liệu giả lập.")
    except Exception as e:
        print(f"Lỗi trong quá trình tạo luồng dữ liệu: {e}")

if __name__ == "__main__":
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        try:
            # Seed dữ liệu Catalog cơ sở
            seed_base_data(cursor)
            conn.commit()
            
            # Khởi chạy vòng lặp tạo luồng dữ liệu biến động
            simulate_streaming_data(conn)
        finally:
            cursor.close()
            conn.close()
import psycopg2
from faker import Faker
import time
import random
from datetime import datetime, timedelta
import os

# Khởi tạo thư viện Faker với ngôn ngữ Việt Nam
fake = Faker('vi_VN')

# Thông tin kết nối tới postgres-data-source (mặc định cổng host 5433)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "data-source")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")

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

def time_delta_days(days):
    return timedelta(days=days)

def seed_base_data(cursor):
    """Khởi tạo dữ liệu cơ bản (Catalog, Sellers, Customers, Vouchers) để đảm bảo không bị lỗi khóa ngoại và đầy đủ các cột."""
    print("------------------------------------------------------------")
    print("🚀 BẮT ĐẦU KIỂM TRA & SEED DỮ LIỆU BAN ĐẦU (seeding base data)...")
    print("------------------------------------------------------------")

    # 1. Seed Platforms
    cursor.execute("SELECT COUNT(*) FROM platforms")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu platforms...")
        platforms = [
            ('tiki', 'Tiki', 'https://tiki.vn', True, datetime.now() - time_delta_days(300), datetime.now() - time_delta_days(300)),
            ('shopee', 'Shopee', 'https://shopee.vn', True, datetime.now() - time_delta_days(300), datetime.now() - time_delta_days(300)),
            ('sendo', 'Sendo', 'https://sendo.vn', True, datetime.now() - time_delta_days(300), datetime.now() - time_delta_days(300)),
            ('chotot', 'ChoTot', 'https://chotot.com', True, datetime.now() - time_delta_days(300), datetime.now() - time_delta_days(300))
        ]
        cursor.executemany(
            """INSERT INTO platforms (platform_code, platform_name, base_url, is_active, created_at, updated_at) 
               VALUES (%s, %s, %s, %s, %s, %s)""",
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
                status = random.choices(['active', 'inactive', 'suspended'], weights=[90, 5, 5], k=1)[0]
                created_date = datetime.now() - time_delta_days(random.randint(100, 250))
                sellers.append((
                    pid,
                    f"seller_{code}_{i}",
                    fake.company(),
                    fake.city(),
                    fake.state(),
                    random.randint(100, 50000),
                    random.choice([True, False]),
                    status,
                    created_date,
                    created_date + time_delta_days(random.randint(1, 50))
                ))
        cursor.executemany(
            """INSERT INTO sellers (platform_id, platform_seller_id, seller_name, city, province, follower_count, is_official_store, status, created_at, updated_at) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            sellers
        )

    # Lấy IDs sellers
    cursor.execute("SELECT seller_id FROM sellers")
    seller_ids = [r[0] for r in cursor.fetchall()]

    # 3. Seed Categories (Parent-Child)
    cursor.execute("SELECT COUNT(*) FROM categories")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu categories với quan hệ cha-con...")
        parent_cats = [
            ("Điện Tử", "electronics"),
            ("Thời Trang", "fashion"),
            ("Tiêu Dùng", "consumer_goods")
        ]
        child_cats_map = {
            "electronics": ["Điện Thoại - Máy Tính Bảng", "Laptop & Phụ Kiện", "Thiết Bị Âm Thanh"],
            "fashion": ["Thời Trang Nam", "Thời Trang Nữ", "Phụ Kiện Thời Trang"],
            "consumer_goods": ["Nhà Cửa - Đời Sống", "Đồ Chơi - Mẹ & Bé", "Bách Hóa Online"]
        }
        for code, pid in platform_map.items():
            for cat_name, cat_code in parent_cats:
                created_date = datetime.now() - time_delta_days(random.randint(150, 250))
                cursor.execute(
                    """INSERT INTO categories (platform_id, platform_category_id, parent_category_id, category_name, is_active, created_at, updated_at) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING category_id""",
                    (pid, f"cat_{code}_{cat_code}", None, cat_name, True, created_date, created_date)
                )
                parent_id = cursor.fetchone()[0]
                
                for idx, child_name in enumerate(child_cats_map[cat_code]):
                    cursor.execute(
                        """INSERT INTO categories (platform_id, platform_category_id, parent_category_id, category_name, is_active, created_at, updated_at) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (pid, f"cat_{code}_{cat_code}_{idx}", parent_id, child_name, True, created_date, created_date)
                    )

    # 4. Seed Brands
    cursor.execute("SELECT COUNT(*) FROM brands")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu brands...")
        brands = []
        brand_names = ["Samsung", "Apple", "Xiaomi", "Coolmate", "Lock&Lock", "LEGO", "Nike"]
        for code, pid in platform_map.items():
            for b_name in brand_names:
                created_date = datetime.now() - time_delta_days(random.randint(150, 250))
                brands.append((
                    pid,
                    f"brand_{code}_{b_name.lower()}",
                    b_name,
                    random.choice(["South Korea", "USA", "China", "Vietnam", "Denmark", "Germany"]),
                    random.choice([True, False]),
                    created_date,
                    created_date
                ))
        cursor.executemany(
            """INSERT INTO brands (platform_id, platform_brand_id, brand_name, country, is_official, created_at, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
            brands
        )

    # 5. Seed Customers & Addresses
    cursor.execute("SELECT COUNT(*) FROM customers")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu customers & addresses...")
        for code, pid in platform_map.items():
            for i in range(15): # 15 khách hàng mỗi sàn
                gender = random.choice(['male', 'female', 'other', 'unknown'])
                status = random.choices(['active', 'inactive', 'blocked'], weights=[90, 8, 2], k=1)[0]
                created_date = datetime.now() - time_delta_days(random.randint(100, 200))
                cursor.execute(
                    """INSERT INTO customers (platform_id, platform_customer_id, full_name, email, phone_number, gender, date_of_birth, status, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING customer_id""",
                    (
                        pid,
                        f"cust_{code}_{i}",
                        fake.name(),
                        fake.free_email(),
                        fake.phone_number(),
                        gender,
                        fake.date_of_birth(minimum_age=18, maximum_age=60),
                        status,
                        created_date,
                        created_date + time_delta_days(random.randint(1, 30))
                    )
                )
                cust_id = cursor.fetchone()[0]
                
                # Thêm 1-2 địa chỉ nhận hàng
                for j in range(random.randint(1, 2)):
                    addr_created = created_date + time_delta_days(random.randint(0, 5))
                    cursor.execute(
                        """INSERT INTO customer_addresses (customer_id, recipient_name, phone_number, address_line, ward, district, city, province, country, postal_code, is_default, created_at, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            cust_id,
                            fake.name(),
                            fake.phone_number(),
                            fake.street_address(),
                            "Phường " + str(random.randint(1, 15)),
                            "Quận " + str(random.randint(1, 12)),
                            fake.city(),
                            fake.state(),
                            "Vietnam",
                            fake.postcode(),
                            j == 0,
                            addr_created,
                            addr_created
                        )
                    )

    # 6. Seed Vouchers
    cursor.execute("SELECT COUNT(*) FROM vouchers")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu vouchers...")
        vouchers = []
        for code, pid in platform_map.items():
            starts = datetime.now() - time_delta_days(30)
            ends = datetime.now() + time_delta_days(90)
            # Platform Voucher 1: percent discount
            vouchers.append((
                pid, None, f"PLATFORM_{code.upper()}_10", f"Giảm giá toàn sàn {code.upper()} 10%", 
                "percent", 10.0, 50000.0, 100000.0, 1000, starts, ends, "active", starts, starts
            ))
            # Platform Voucher 2: free shipping
            vouchers.append((
                pid, None, f"PLATFORM_{code.upper()}_FREESHIP", f"Miễn phí vận chuyển {code.upper()}", 
                "free_shipping", 30000.0, 30000.0, 50000.0, 2000, starts, ends, "active", starts, starts
            ))
            
        # Seed shop specific vouchers for some sellers
        for s_id in seller_ids[:10]:
            cursor.execute("SELECT platform_id FROM sellers WHERE seller_id = %s", (s_id,))
            pid = cursor.fetchone()[0]
            starts = datetime.now() - time_delta_days(10)
            ends = datetime.now() + time_delta_days(40)
            # Shop Voucher: fixed amount
            vouchers.append((
                pid, s_id, f"SHOP_{s_id}_20K", f"Giảm giá shop #{s_id} 20K", 
                "fixed_amount", 20000.0, 20000.0, 150000.0, 500, starts, ends, "active", starts, starts
            ))
            
        cursor.executemany(
            """INSERT INTO vouchers (platform_id, seller_id, voucher_code, voucher_name, discount_type, discount_value, max_discount_amount, min_order_amount, usage_limit, starts_at, ends_at, status, created_at, updated_at) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            vouchers
        )

    # 7. Seed Products & Variants & Inventory
    cursor.execute("SELECT COUNT(*) FROM products")
    if cursor.fetchone()[0] == 0:
        print("💡 Đang khởi tạo dữ liệu products & variants & inventory...")
        cursor.execute("SELECT seller_id, platform_id FROM sellers WHERE status = 'active'")
        seller_list = cursor.fetchall()
        cursor.execute("SELECT category_id, platform_id FROM categories WHERE parent_category_id IS NOT NULL")
        category_list = cursor.fetchall()
        cursor.execute("SELECT brand_id, platform_id FROM brands")
        brand_list = cursor.fetchall()

        prod_names_map = {
            "Điện Thoại - Máy Tính Bảng": ["iPhone 15 Pro Max", "Samsung Galaxy S24 Ultra", "Xiaomi Redmi Note 13", "iPad Air M2"],
            "Laptop & Phụ Kiện": ["MacBook Air M3", "Dell XPS 13", "Bàn phím cơ Keychron", "Chuột Logitech MX Master 3S"],
            "Thiết Bị Âm Thanh": ["Tai nghe Sony WH-1000XM5", "Loa Bluetooth JBL Charge 5", "Tai nghe AirPods Pro 2"],
            "Thời Trang Nam": ["Áo thun thể thao nam", "Quần short kaki nam", "Áo khoác gió bomber Coolmate"],
            "Thời Trang Nữ": ["Váy hoa dáng xòe", "Đầm dự tiệc trễ vai", "Áo sơ mi lụa nữ"],
            "Phụ Kiện Thời Trang": ["Kính mát nam chống UV", "Ví da cầm tay nữ", "Thắt lưng da cao cấp"],
            "Nhà Cửa - Đời Sống": ["Bình giữ nhiệt Lock&Lock", "Bộ lau nhà tự vắt", "Đèn học chống cận Xiaomi"],
            "Đồ Chơi - Mẹ & Bé": ["Bộ đồ chơi LEGO City", "Tã quần Bobby size XL", "Sữa bột Frisolac Gold"],
            "Bách Hóa Online": ["Hộp cà phê Trung Nguyên G7", "Thùng mì Hảo Hảo tôm chua cay", "Nước rửa chén Sunlight"]
        }

        for idx, (seller_id, platform_id) in enumerate(seller_list):
            sub_cats = [c for c in category_list if c[1] == platform_id]
            sub_brands = [b for b in brand_list if b[1] == platform_id]
            if not sub_cats or not sub_brands: continue

            for p_idx in range(4):
                cat_id, _ = random.choice(sub_cats)
                brand_id, _ = random.choice(sub_brands)

                cursor.execute("SELECT category_name FROM categories WHERE category_id = %s", (cat_id,))
                cat_name = cursor.fetchone()[0]
                p_name = random.choice(prod_names_map.get(cat_name, ["Sản phẩm mẫu " + str(p_idx)])) + f" (Seller #{seller_id})"
                
                prod_status = random.choices(['active', 'inactive', 'out_of_stock'], weights=[90, 5, 5], k=1)[0]
                created_date = datetime.now() - time_delta_days(random.randint(50, 100))
                
                cursor.execute(
                    """INSERT INTO products (platform_product_id, seller_id, category_id, brand_id, product_name, description, status, is_authentic, published_at, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING product_id""",
                    (
                        f"prod_{seller_id}_{p_idx}",
                        seller_id,
                        cat_id,
                        brand_id,
                        p_name,
                        fake.text(max_nb_chars=200),
                        prod_status,
                        random.choice([True, False]),
                        created_date,
                        created_date,
                        created_date
                    )
                )
                prod_id = cursor.fetchone()[0]

                # Tạo 1-2 variant cho mỗi sản phẩm
                for v_idx in range(random.randint(1, 2)):
                    original_price = random.randint(30, 1500) * 10000
                    sale_price = original_price * random.choice([0.8, 0.85, 0.9, 0.95, 1.0])
                    v_status = 'active' if prod_status == 'active' else prod_status
                    v_created = created_date + time_delta_days(random.randint(0, 2))
                    
                    cursor.execute(
                        """INSERT INTO product_variants (product_id, platform_variant_id, sku, variant_name, original_price, sale_price, weight_gram, status, created_at, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING variant_id""",
                        (
                            prod_id,
                            f"var_{prod_id}_{v_idx}",
                            f"SKU-{seller_id}-{prod_id}-{v_idx}",
                            f"Phiên bản {v_idx} / Size {random.choice(['M', 'L', 'XL', '256GB', '128GB', '1.2L'])}",
                            original_price,
                            sale_price,
                            random.randint(50, 3000),
                            v_status,
                            v_created,
                            v_created
                        )
                    )
                    variant_id = cursor.fetchone()[0]

                    # Khởi tạo kho hàng
                    qty = random.randint(80, 400)
                    cursor.execute(
                        """INSERT INTO product_inventory (variant_id, warehouse_code, quantity_on_hand, quantity_reserved, low_stock_threshold, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (
                            variant_id,
                            random.choice(['WH-NORTH', 'WH-SOUTH', 'WH-CENTRAL']),
                            qty,
                            0,
                            15,
                            v_created
                        )
                    )
    print("✅ HOÀN TẤT SEED DỮ LIỆU CƠ BẢN!")
    print("------------------------------------------------------------\n")

def release_order_inventory(cursor, order_id, is_cancellation=True, is_return=False):
    """
    Cập nhật lượng tồn kho thực tế và lượng đặt trước khi hủy/giao/hoàn trả đơn hàng.
    Bảo đảm: quantity_reserved <= quantity_on_hand.
    """
    cursor.execute("SELECT variant_id, quantity FROM order_items WHERE order_id = %s", (order_id,))
    items = cursor.fetchall()
    for var_id, qty in items:
        cursor.execute(
            """SELECT inventory_id, quantity_on_hand, quantity_reserved 
               FROM product_inventory WHERE variant_id = %s FOR UPDATE""",
            (var_id,)
        )
        inv_row = cursor.fetchone()
        if inv_row:
            inv_id, on_hand, reserved = inv_row
            if is_cancellation:
                new_reserved = max(0, reserved - qty)
                if is_return:
                    new_on_hand = on_hand + qty
                    m_type = 'return'
                    reason = f'Khách trả hàng cho đơn #{order_id} (Nhập lại kho)'
                    qty_delta = qty
                else:
                    new_on_hand = on_hand # Hủy trước giao: on_hand giữ nguyên, chỉ giải phóng phần reserved
                    m_type = 'release'
                    reason = f'Hủy đơn hàng #{order_id} (Giải phóng lượng đặt trước)'
                    qty_delta = 0
                
                cursor.execute(
                    """UPDATE product_inventory 
                       SET quantity_on_hand = %s, quantity_reserved = %s, updated_at = %s 
                       WHERE inventory_id = %s""",
                    (new_on_hand, new_reserved, datetime.now(), inv_id)
                )
                cursor.execute(
                    """INSERT INTO inventory_movements (inventory_id, movement_type, quantity_delta, reference_type, reference_id, reason, occurred_at)
                       VALUES (%s, %s, %s, 'orders', %s, %s, %s)""",
                    (inv_id, m_type, qty_delta, str(order_id), reason, datetime.now())
                )
            else:
                # Giao thành công: Giải phóng reserved, trừ thực tế ở on_hand
                new_reserved = max(0, reserved - qty)
                new_on_hand = max(0, on_hand - qty)
                
                if new_reserved > new_on_hand:
                    new_reserved = new_on_hand
                
                cursor.execute(
                    """UPDATE product_inventory 
                       SET quantity_on_hand = %s, quantity_reserved = %s, updated_at = %s 
                       WHERE inventory_id = %s""",
                    (new_on_hand, new_reserved, datetime.now(), inv_id)
                )
                cursor.execute(
                    """INSERT INTO inventory_movements (inventory_id, movement_type, quantity_delta, reference_type, reference_id, reason, occurred_at)
                       VALUES (%s, 'sale', %s, 'orders', %s, 'Xuất kho hoàn tất giao đơn hàng', %s)""",
                    (inv_id, -qty, str(order_id), datetime.now())
                )

def simulate_streaming_data(conn):
    """Mô phỏng luồng dữ liệu e-commerce liên tục (hành vi khách hàng, vòng đời đơn hàng, cập nhật thực thể)."""
    cursor = conn.cursor()

    # Lấy danh sách ID để giả lập
    cursor.execute("SELECT customer_id FROM customers WHERE status='active'")
    customer_ids = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT seller_id FROM sellers WHERE status='active'")
    seller_ids = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT product_id FROM products WHERE status='active'")
    product_ids = [r[0] for r in cursor.fetchall()]

    if not customer_ids or not seller_ids or not product_ids:
        print("❌ Dữ liệu ban đầu không đầy đủ để mô phỏng. Vui lòng kiểm tra lại quá trình seed.")
        return

    print("⚡ Bắt đầu tạo luồng dữ liệu giả lập...")
    try:
        while True:
            # Chọn loại hành vi ngẫu nhiên
            event_choice = random.choices(
                ['view', 'add_to_cart', 'purchase', 'update_order', 'review', 'update_entities'],
                weights=[35, 25, 15, 15, 6, 4],
                k=1
            )[0]

            if event_choice == 'view':
                cust_id = random.choice(customer_ids)
                prod_id = random.choice(product_ids)
                cursor.execute("SELECT variant_id FROM product_variants WHERE product_id = %s AND status='active'", (prod_id,))
                v_rows = cursor.fetchall()
                var_id = random.choice(v_rows)[0] if v_rows else None
                
                cursor.execute(
                    """INSERT INTO events (platform_event_id, customer_id, product_id, variant_id, cart_item_id, order_item_id, event_type, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        f"evt_{int(time.time()*1000)}_{random.randint(100, 999)}",
                        cust_id,
                        prod_id,
                        var_id,
                        None,
                        None,
                        'view',
                        datetime.now()
                    )
                )
                conn.commit()
                print(f"👁️ [VIEW]: Khách hàng #{cust_id} xem sản phẩm #{prod_id} (Variant: {var_id})")

            elif event_choice == 'add_to_cart':
                cust_id = random.choice(customer_ids)
                cursor.execute(
                    """INSERT INTO carts (customer_id, status, created_at, updated_at) 
                       VALUES (%s, 'active', %s, %s) 
                       ON CONFLICT (customer_id) DO UPDATE SET status='active', updated_at = %s 
                       RETURNING cart_id""", 
                    (cust_id, datetime.now(), datetime.now(), datetime.now())
                )
                cart_id = cursor.fetchone()[0]

                prod_id = random.choice(product_ids)
                cursor.execute("SELECT variant_id, sale_price FROM product_variants WHERE product_id = %s AND status='active'", (prod_id,))
                v_rows = cursor.fetchall()
                if v_rows:
                    var_id, unit_price = random.choice(v_rows)
                    qty = random.randint(1, 3)
                    cursor.execute(
                        """INSERT INTO cart_items (cart_id, variant_id, quantity, unit_price, added_at, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s) 
                           ON CONFLICT (cart_id, variant_id) 
                           DO UPDATE SET quantity = cart_items.quantity + EXCLUDED.quantity, unit_price = EXCLUDED.unit_price, updated_at = EXCLUDED.updated_at
                           RETURNING cart_item_id""",
                        (cart_id, var_id, qty, unit_price, datetime.now(), datetime.now())
                    )
                    cart_item_id = cursor.fetchone()[0]
                    
                    cursor.execute(
                        """INSERT INTO events (platform_event_id, customer_id, product_id, variant_id, cart_item_id, order_item_id, event_type, created_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            f"evt_{int(time.time()*1000)}_{random.randint(100, 999)}",
                            cust_id,
                            prod_id,
                            var_id,
                            cart_item_id,
                            None,
                            'add_to_cart',
                            datetime.now()
                        )
                    )
                    conn.commit()
                    print(f"🛒 [CART]: Khách hàng #{cust_id} thêm {qty}x variant #{var_id} vào giỏ hàng #{cart_id}")

            elif event_choice == 'purchase':
                cust_id = random.choice(customer_ids)
                
                # Tìm xem giỏ hàng có hàng không, nếu không tạo nhanh 1-2 món
                cursor.execute(
                    """SELECT c.cart_id, ci.cart_item_id, ci.variant_id, ci.quantity, ci.unit_price, pv.product_id, p.seller_id
                       FROM carts c
                       JOIN cart_items ci ON c.cart_id = ci.cart_id
                       JOIN product_variants pv ON ci.variant_id = pv.variant_id
                       JOIN products p ON pv.product_id = p.product_id
                       WHERE c.customer_id = %s AND c.status = 'active'""",
                    (cust_id,)
                )
                cart_rows = cursor.fetchall()
                
                if not cart_rows:
                    prod_id = random.choice(product_ids)
                    cursor.execute("SELECT variant_id, sale_price FROM product_variants WHERE product_id = %s AND status='active'", (prod_id,))
                    v_rows = cursor.fetchall()
                    if not v_rows: continue
                    var_id, unit_price = random.choice(v_rows)
                    qty = random.randint(1, 2)
                    
                    cursor.execute(
                        """INSERT INTO carts (customer_id, status, created_at, updated_at) 
                           VALUES (%s, 'active', %s, %s) 
                           ON CONFLICT (customer_id) DO UPDATE SET status='active', updated_at = %s RETURNING cart_id""", 
                        (cust_id, datetime.now(), datetime.now(), datetime.now())
                    )
                    cart_id = cursor.fetchone()[0]
                    cursor.execute(
                        """INSERT INTO cart_items (cart_id, variant_id, quantity, unit_price, added_at, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (cart_id, variant_id) DO UPDATE SET quantity = EXCLUDED.quantity""",
                        (cart_id, var_id, qty, unit_price, datetime.now(), datetime.now())
                    )
                    
                    cursor.execute(
                        """SELECT c.cart_id, ci.cart_item_id, ci.variant_id, ci.quantity, ci.unit_price, pv.product_id, p.seller_id
                           FROM carts c
                           JOIN cart_items ci ON c.cart_id = ci.cart_id
                           JOIN product_variants pv ON ci.variant_id = pv.variant_id
                           JOIN products p ON pv.product_id = p.product_id
                           WHERE c.customer_id = %s AND c.status = 'active'""",
                        (cust_id,)
                    )
                    cart_rows = cursor.fetchall()
                
                if cart_rows:
                    cart_id = cart_rows[0][0]
                    seller_id = cart_rows[0][6]
                    
                    cursor.execute("SELECT address_id FROM customer_addresses WHERE customer_id = %s AND is_default = TRUE LIMIT 1", (cust_id,))
                    addr_row = cursor.fetchone()
                    addr_id = addr_row[0] if addr_row else None
                    
                    cursor.execute("SELECT platform_id FROM customers WHERE customer_id = %s", (cust_id,))
                    p_id = cursor.fetchone()[0]
                    
                    # Tìm voucher áp dụng
                    cursor.execute(
                        """SELECT voucher_id, discount_type, discount_value, min_order_amount, max_discount_amount 
                           FROM vouchers 
                           WHERE platform_id = %s AND (seller_id IS NULL OR seller_id = %s) AND status = 'active' 
                           AND starts_at <= NOW() AND ends_at >= NOW() LIMIT 1""",
                        (p_id, seller_id)
                    )
                    v_row = cursor.fetchone()
                    
                    subtotal_amount = sum(row[3] * row[4] for row in cart_rows)
                    voucher_id = None
                    discount_amount = 0.0
                    
                    if v_row and subtotal_amount >= float(v_row[3]):
                        voucher_id, dtype, dval, _, max_d = v_row
                        dval = float(dval)
                        max_d = float(max_d) if max_d else None
                        if dtype == 'percent':
                            discount_amount = subtotal_amount * (dval / 100.0)
                            if max_d and discount_amount > max_d:
                                discount_amount = max_d
                        elif dtype == 'fixed_amount':
                            discount_amount = dval
                        elif dtype == 'free_shipping':
                            discount_amount = min(30000.0, dval)
                    
                    shipping_fee = 30000.0
                    total_amount = max(0.0, subtotal_amount + shipping_fee - discount_amount)
                    
                    # Tạo Order
                    cursor.execute(
                        """INSERT INTO orders (platform_order_id, customer_id, seller_id, shipping_address_id, voucher_id, order_status, subtotal_amount, shipping_fee, discount_amount, total_amount, ordered_at, updated_at)
                           VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s, %s, %s, %s, %s) RETURNING order_id""",
                        (
                            f"ord_{int(time.time())}_{random.randint(100, 999)}",
                            cust_id,
                            seller_id,
                            addr_id,
                            voucher_id,
                            subtotal_amount,
                            shipping_fee,
                            discount_amount,
                            total_amount,
                            datetime.now(),
                            datetime.now()
                        )
                    )
                    order_id = cursor.fetchone()[0]
                    
                    for row in cart_rows:
                        _, cart_item_id, var_id, qty, unit_price, prod_id, _ = row
                        cursor.execute(
                            """INSERT INTO order_items (order_id, variant_id, quantity, unit_price, discount_amount)
                               VALUES (%s, %s, %s, %s, %s) RETURNING order_item_id""",
                            (order_id, var_id, qty, unit_price, 0.0)
                        )
                        order_item_id = cursor.fetchone()[0]
                        
                        # Reserve inventory
                        cursor.execute(
                            """SELECT quantity_on_hand, quantity_reserved, inventory_id 
                               FROM product_inventory WHERE variant_id = %s FOR UPDATE""", 
                            (var_id,)
                        )
                        inv = cursor.fetchone()
                        if inv:
                            on_hand, reserved, inv_id = inv
                            new_reserved = min(on_hand, reserved + qty)
                            cursor.execute(
                                "UPDATE product_inventory SET quantity_reserved = %s, updated_at = %s WHERE inventory_id = %s",
                                (new_reserved, datetime.now(), inv_id)
                            )
                            cursor.execute(
                                """INSERT INTO inventory_movements (inventory_id, movement_type, quantity_delta, reference_type, reference_id, reason, occurred_at)
                                   VALUES (%s, 'reserve', %s, 'orders', %s, 'Giữ kho chờ thanh toán đơn hàng', %s)""",
                                (inv_id, qty, str(order_id), datetime.now())
                            )
                            
                        # Purchase event
                        cursor.execute(
                            """INSERT INTO events (platform_event_id, customer_id, product_id, variant_id, cart_item_id, order_item_id, event_type, created_at)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (
                                f"evt_{int(time.time()*1000)}_{random.randint(100, 999)}",
                                cust_id,
                                prod_id,
                                var_id,
                                None,
                                order_item_id,
                                'purchase',
                                datetime.now()
                            )
                        )
                    
                    # Update cart & clear items
                    cursor.execute("UPDATE carts SET status = 'ordered', updated_at = %s WHERE cart_id = %s", (datetime.now(), cart_id))
                    cursor.execute("DELETE FROM cart_items WHERE cart_id = %s", (cart_id,))
                    
                    # Payment
                    pmeth = random.choice(['cod', 'card', 'bank_transfer', 'momo', 'zalopay', 'shopeepay', 'wallet'])
                    pstatus = 'paid' if pmeth != 'cod' else 'pending'
                    paid_at = datetime.now() if pstatus == 'paid' else None
                    cursor.execute(
                        """INSERT INTO payments (order_id, payment_method, provider, amount, status, paid_at, created_at, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (order_id, pmeth, pmeth.upper(), total_amount, pstatus, paid_at, datetime.now(), datetime.now())
                    )
                    
                    if pstatus == 'paid':
                        cursor.execute("UPDATE orders SET order_status = 'confirmed', updated_at = %s WHERE order_id = %s", (datetime.now(), order_id))
                    
                    # Shipment
                    cursor.execute(
                        """INSERT INTO shipments (order_id, carrier_name, tracking_number, shipping_method, status, shipped_at, estimated_delivery_at, delivered_at, created_at, updated_at)
                           VALUES (%s, %s, %s, %s, 'preparing', %s, %s, %s, %s, %s)""",
                        (
                            order_id,
                            random.choice(["Giao Hàng Nhanh", "Giao Hàng Tiết Kiệm", "Viettel Post", "J&T Express", "Ninja Van"]),
                            f"TRACK_{order_id}_{random.randint(100000, 999999)}",
                            random.choice(["standard", "express", "saving"]),
                            None, None, None, datetime.now(), datetime.now()
                        )
                    )
                    
                    conn.commit()
                    print(f"🛍️ [ORDER]: Đơn hàng #{order_id} được tạo thành công! Khách #{cust_id} thanh toán: {pmeth.upper()}")

            elif event_choice == 'update_order':
                cursor.execute(
                    """SELECT order_id, order_status, total_amount 
                       FROM orders 
                       WHERE order_status IN ('pending', 'confirmed', 'packed', 'shipping') 
                       ORDER BY RANDOM() LIMIT 5"""
                )
                orders_to_update = cursor.fetchall()
                
                for o_id, o_status, t_amount in orders_to_update:
                    next_status = None
                    
                    if o_status == 'pending':
                        if random.random() < 0.85:
                            next_status = 'confirmed'
                            cursor.execute("UPDATE payments SET status = 'paid', paid_at = %s, updated_at = %s WHERE order_id = %s AND status = 'pending' AND payment_method != 'cod'", (datetime.now(), datetime.now(), o_id))
                        else:
                            next_status = 'cancelled'
                            cursor.execute("UPDATE payments SET status = 'cancelled', updated_at = %s WHERE order_id = %s", (datetime.now(), o_id))
                            release_order_inventory(cursor, o_id, is_cancellation=True)
                            
                    elif o_status == 'confirmed':
                        next_status = 'packed'
                        
                    elif o_status == 'packed':
                        next_status = 'shipping'
                        cursor.execute(
                            """UPDATE shipments 
                               SET status = 'in_transit', shipped_at = %s, estimated_delivery_at = %s, updated_at = %s 
                               WHERE order_id = %s""",
                            (datetime.now(), datetime.now() + time_delta_days(random.randint(1, 3)), datetime.now(), o_id)
                        )
                        
                    elif o_status == 'shipping':
                        rand_val = random.random()
                        if rand_val < 0.80:
                            next_status = 'completed'
                            is_late = random.random() < 0.2
                            delivered_at = datetime.now()
                            if is_late:
                                cursor.execute("SELECT estimated_delivery_at FROM shipments WHERE order_id = %s", (o_id,))
                                est_row = cursor.fetchone()
                                if est_row and est_row[0]:
                                    delivered_at = est_row[0] + time_delta_days(random.randint(1, 2))
                            
                            cursor.execute(
                                """UPDATE shipments 
                                   SET status = 'delivered', delivered_at = %s, updated_at = %s 
                                   WHERE order_id = %s""",
                                (delivered_at, datetime.now(), o_id)
                            )
                            cursor.execute("UPDATE payments SET status = 'paid', paid_at = %s, updated_at = %s WHERE order_id = %s AND status = 'pending'", (datetime.now(), datetime.now(), o_id))
                            release_order_inventory(cursor, o_id, is_cancellation=False)
                        elif rand_val < 0.95:
                            next_status = 'cancelled'
                            cursor.execute("UPDATE shipments SET status = 'cancelled', updated_at = %s WHERE order_id = %s", (datetime.now(), o_id))
                            cursor.execute("UPDATE payments SET status = 'cancelled', updated_at = %s WHERE order_id = %s", (datetime.now(), o_id))
                            release_order_inventory(cursor, o_id, is_cancellation=True)
                        else:
                            next_status = 'returned'
                            cursor.execute("UPDATE shipments SET status = 'returned', updated_at = %s WHERE order_id = %s", (datetime.now(), o_id))
                            cursor.execute("UPDATE payments SET status = 'refunded', updated_at = %s WHERE order_id = %s", (datetime.now(), o_id))
                            release_order_inventory(cursor, o_id, is_cancellation=True, is_return=True)
                    
                    if next_status:
                        cursor.execute(
                            "UPDATE orders SET order_status = %s, updated_at = %s WHERE order_id = %s",
                            (next_status, datetime.now(), o_id)
                        )
                        conn.commit()
                        print(f"🔄 [VÒNG ĐỜI ĐƠN HÀNG]: Đơn #{o_id} chuyển từ {o_status} ➔ {next_status}")

                if random.random() < 0.05:
                    cursor.execute(
                        """SELECT order_id FROM orders 
                           WHERE order_status = 'completed' AND updated_at < NOW() - INTERVAL '15 seconds' 
                           ORDER BY RANDOM() LIMIT 1"""
                    )
                    completed_row = cursor.fetchone()
                    if completed_row:
                        o_id = completed_row[0]
                        cursor.execute("UPDATE orders SET order_status = 'returned', updated_at = %s WHERE order_id = %s", (datetime.now(), o_id))
                        cursor.execute("UPDATE shipments SET status = 'returned', updated_at = %s WHERE order_id = %s", (datetime.now(), o_id))
                        cursor.execute("UPDATE payments SET status = 'refunded', updated_at = %s WHERE order_id = %s", (datetime.now(), o_id))
                        release_order_inventory(cursor, o_id, is_cancellation=True, is_return=True)
                        conn.commit()
                        print(f"↩️ [TRẢ HÀNG]: Khách trả lại đơn đã hoàn tất #{o_id} thành công!")

            elif event_choice == 'review':
                cursor.execute(
                    """SELECT oi.order_item_id, o.order_id, pv.product_id, o.customer_id
                       FROM order_items oi
                       JOIN orders o ON oi.order_id = o.order_id
                       JOIN product_variants pv ON oi.variant_id = pv.variant_id
                       LEFT JOIN product_reviews pr ON oi.order_item_id = pr.order_item_id
                       WHERE o.order_status = 'completed' AND pr.review_id IS NULL
                       ORDER BY RANDOM() LIMIT 1"""
                )
                reviewable_row = cursor.fetchone()
                if reviewable_row:
                    oi_id, o_id, p_id, cust_id = reviewable_row
                    rating = random.choices([5, 4, 3, 2, 1], weights=[65, 20, 10, 3, 2], k=1)[0]
                    del_rating = random.choices([5, 4, 3, 2, 1, None], weights=[60, 20, 10, 3, 2, 5], k=1)[0]
                    sel_rating = random.choices([5, 4, 3, 2, 1, None], weights=[60, 20, 10, 3, 2, 5], k=1)[0]
                    rev_status = random.choice(['published', 'published', 'pending', 'hidden'])
                    
                    cursor.execute(
                        """INSERT INTO product_reviews (platform_review_id, product_id, order_item_id, customer_id, rating, title, content, delivery_rating, seller_rating, helpful_count, status, reviewed_at, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
                        (
                            f"rev_{oi_id}_{random.randint(1000, 9999)}",
                            p_id,
                            oi_id,
                            cust_id,
                            rating,
                            fake.sentence(),
                            fake.text(max_nb_chars=120),
                            del_rating,
                            sel_rating,
                            random.randint(0, 45),
                            rev_status,
                            datetime.now(),
                            datetime.now()
                        )
                    )
                    conn.commit()
                    print(f"⭐ [REVIEW]: Khách #{cust_id} đánh giá {rating} sao cho sản phẩm #{p_id}")

            elif event_choice == 'update_entities':
                s_id = random.choice(seller_ids)
                cursor.execute("UPDATE sellers SET follower_count = follower_count + %s, updated_at = %s WHERE seller_id = %s", (random.randint(-5, 20), datetime.now(), s_id))
                
                # Cập nhật giá ngẫu nhiên để kiểm tra thay đổi variant
                cursor.execute("SELECT variant_id, original_price FROM product_variants ORDER BY RANDOM() LIMIT 1")
                var_row = cursor.fetchone()
                if var_row:
                    v_id, orig_p = var_row
                    new_sale = float(orig_p) * random.choice([0.7, 0.8, 0.85, 0.9, 0.95, 1.0])
                    cursor.execute("UPDATE product_variants SET sale_price = %s, updated_at = %s WHERE variant_id = %s", (new_sale, datetime.now(), v_id))
                
                # Bổ sung hàng tồn kho
                cursor.execute(
                    """SELECT inventory_id, quantity_on_hand 
                       FROM product_inventory 
                       WHERE quantity_on_hand <= low_stock_threshold LIMIT 3"""
                )
                low_stocks = cursor.fetchall()
                for inv_id, q_hand in low_stocks:
                    replenish_qty = random.randint(150, 300)
                    cursor.execute(
                        """UPDATE product_inventory 
                           SET quantity_on_hand = quantity_on_hand + %s, updated_at = %s 
                           WHERE inventory_id = %s""",
                        (replenish_qty, datetime.now(), inv_id)
                    )
                    cursor.execute(
                        """INSERT INTO inventory_movements (inventory_id, movement_type, quantity_delta, reference_type, reference_id, reason, occurred_at)
                           VALUES (%s, 'import', %s, 'replenishment', NULL, 'Nhập bổ sung kho do cảnh báo hết hàng', %s)""",
                        (inv_id, replenish_qty, datetime.now())
                    )
                    print(f"📦 [KHO]: Bổ sung +{replenish_qty} sản phẩm vào kho #{inv_id}")
                
                conn.commit()

            time.sleep(random.uniform(0.3, 1.2))

    except KeyboardInterrupt:
        print("\nĐã tắt bộ giả lập.")
    except Exception as e:
        print(f"Lỗi simulator: {e}")

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
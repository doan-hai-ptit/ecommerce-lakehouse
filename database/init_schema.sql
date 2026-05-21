-- ==============================================================================
-- Ecommerce marketplace streaming source schema
--
-- Các bảng này dùng để giả lập dữ liệu giao dịch từ nhiều sàn thương mại điện tử
-- trước khi đẩy stream/CDC vào lakehouse. Thiết kế ưu tiên:
--   - Có source platform để phân biệt Tiki/Shopee/Sendo/ChoTot.
--   - Có dữ liệu vận hành: sản phẩm, tồn kho, giỏ hàng, đơn hàng, thanh toán,
--     vận chuyển, review, voucher.
--   - Có bảng event_outbox và stream_checkpoints để phục vụ mô phỏng streaming.
-- ==============================================================================

-- ==============================================================================
-- 0. XÓA BẢNG CŨ
-- ==============================================================================
DROP TABLE IF EXISTS event_outbox CASCADE;
DROP TABLE IF EXISTS stream_checkpoints CASCADE;
DROP TABLE IF EXISTS product_reviews CASCADE;
DROP TABLE IF EXISTS shipments CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS cart_items CASCADE;
DROP TABLE IF EXISTS carts CASCADE;
DROP TABLE IF EXISTS vouchers CASCADE;
DROP TABLE IF EXISTS inventory_movements CASCADE;
DROP TABLE IF EXISTS product_inventory CASCADE;
DROP TABLE IF EXISTS product_variants CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS brands CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS customer_addresses CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS sellers CASCADE;
DROP TABLE IF EXISTS platforms CASCADE;

-- Bảng từ các thiết kế cũ, giữ DROP để chạy lại script không lỗi.
DROP TABLE IF EXISTS marketplace_reviews CASCADE;
DROP TABLE IF EXISTS marketplace_products CASCADE;
DROP TABLE IF EXISTS marketplace_sellers CASCADE;
DROP TABLE IF EXISTS marketplace_categories CASCADE;
DROP TABLE IF EXISTS product_price_snapshots CASCADE;
DROP TABLE IF EXISTS raw_object_manifests CASCADE;
DROP TABLE IF EXISTS crawl_runs CASCADE;
DROP TABLE IF EXISTS crawler_state CASCADE;
DROP TABLE IF EXISTS data_sources CASCADE;
DROP TABLE IF EXISTS wishlists CASCADE;
DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS deliveries CASCADE;
DROP TABLE IF EXISTS product_inventory_old CASCADE;
DROP TABLE IF EXISTS user_addresses CASCADE;
DROP TABLE IF EXISTS user_profiles CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS roles CASCADE;

-- ==============================================================================
-- 1. NỀN TẢNG, SELLER, CUSTOMER
-- ==============================================================================
CREATE TABLE platforms (
    platform_id SMALLSERIAL PRIMARY KEY,
    platform_code VARCHAR(30) UNIQUE NOT NULL,
    platform_name VARCHAR(100) NOT NULL,
    base_url TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO platforms (platform_code, platform_name, base_url)
VALUES
    ('tiki', 'Tiki', 'https://tiki.vn'),
    ('shopee', 'Shopee', 'https://shopee.vn'),
    ('sendo', 'Sendo', 'https://sendo.vn'),
    ('chotot', 'Cho Tot', 'https://www.chotot.com')
ON CONFLICT (platform_code) DO NOTHING;

CREATE TABLE sellers (
    seller_id BIGSERIAL PRIMARY KEY,
    platform_id SMALLINT NOT NULL REFERENCES platforms(platform_id),
    platform_seller_id VARCHAR(100) NOT NULL,
    seller_name VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    province VARCHAR(100),
    rating_average NUMERIC(3, 2)
        CHECK (rating_average IS NULL OR rating_average BETWEEN 0 AND 5),
    follower_count INT CHECK (follower_count IS NULL OR follower_count >= 0),
    is_official_store BOOLEAN NOT NULL DEFAULT FALSE,
    status VARCHAR(30) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform_id, platform_seller_id),
    CHECK (status IN ('active', 'inactive', 'suspended'))
);

CREATE TABLE customers (
    customer_id BIGSERIAL PRIMARY KEY,
    platform_id SMALLINT NOT NULL REFERENCES platforms(platform_id),
    platform_customer_id VARCHAR(100) NOT NULL,
    full_name VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(30),
    gender VARCHAR(20),
    date_of_birth DATE,
    status VARCHAR(30) NOT NULL DEFAULT 'active',
    registered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform_id, platform_customer_id),
    CHECK (status IN ('active', 'inactive', 'blocked')),
    CHECK (gender IS NULL OR gender IN ('male', 'female', 'other', 'unknown'))
);

CREATE TABLE customer_addresses (
    address_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    recipient_name VARCHAR(255),
    phone_number VARCHAR(30),
    address_line TEXT NOT NULL,
    ward VARCHAR(100),
    district VARCHAR(100),
    city VARCHAR(100),
    province VARCHAR(100),
    country VARCHAR(100) NOT NULL DEFAULT 'Vietnam',
    postal_code VARCHAR(30),
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- ==============================================================================
-- 2. CATALOG SẢN PHẨM
-- ==============================================================================
CREATE TABLE categories (
    category_id BIGSERIAL PRIMARY KEY,
    platform_id SMALLINT NOT NULL REFERENCES platforms(platform_id),
    platform_category_id VARCHAR(100) NOT NULL,
    parent_category_id BIGINT REFERENCES categories(category_id),
    category_name VARCHAR(255) NOT NULL,
    category_path TEXT,
    level INT NOT NULL DEFAULT 0 CHECK (level >= 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform_id, platform_category_id)
);

CREATE TABLE brands (
    brand_id BIGSERIAL PRIMARY KEY,
    platform_id SMALLINT REFERENCES platforms(platform_id),
    platform_brand_id VARCHAR(100),
    brand_name VARCHAR(255) NOT NULL,
    country VARCHAR(100),
    is_official BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform_id, platform_brand_id),
    UNIQUE (platform_id, brand_name)
);

CREATE TABLE products (
    product_id BIGSERIAL PRIMARY KEY,
    platform_id SMALLINT NOT NULL REFERENCES platforms(platform_id),
    platform_product_id VARCHAR(100) NOT NULL,
    seller_id BIGINT NOT NULL REFERENCES sellers(seller_id),
    category_id BIGINT REFERENCES categories(category_id),
    brand_id BIGINT REFERENCES brands(brand_id),
    product_name TEXT NOT NULL,
    description TEXT,
    product_url TEXT,
    thumbnail_url TEXT,
    image_urls JSONB,
    status VARCHAR(30) NOT NULL DEFAULT 'active',
    rating_average NUMERIC(3, 2)
        CHECK (rating_average IS NULL OR rating_average BETWEEN 0 AND 5),
    review_count INT NOT NULL DEFAULT 0 CHECK (review_count >= 0),
    sold_count INT NOT NULL DEFAULT 0 CHECK (sold_count >= 0),
    view_count INT NOT NULL DEFAULT 0 CHECK (view_count >= 0),
    liked_count INT NOT NULL DEFAULT 0 CHECK (liked_count >= 0),
    is_authentic BOOLEAN,
    is_official_store BOOLEAN,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform_id, platform_product_id),
    CHECK (status IN ('active', 'inactive', 'deleted', 'out_of_stock'))
);

CREATE TABLE product_variants (
    variant_id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
    platform_variant_id VARCHAR(100),
    sku VARCHAR(255),
    variant_name VARCHAR(255),
    option_values JSONB,
    currency VARCHAR(10) NOT NULL DEFAULT 'VND',
    original_price NUMERIC(18, 2) NOT NULL CHECK (original_price >= 0),
    sale_price NUMERIC(18, 2) NOT NULL CHECK (sale_price >= 0),
    discount_rate NUMERIC(5, 2)
        CHECK (discount_rate IS NULL OR discount_rate BETWEEN 0 AND 100),
    barcode VARCHAR(100),
    weight_gram INT CHECK (weight_gram IS NULL OR weight_gram >= 0),
    status VARCHAR(30) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (product_id, platform_variant_id),
    CHECK (status IN ('active', 'inactive', 'deleted', 'out_of_stock'))
);
-- ==============================================================================
-- 3. TỒN KHO & KHUYẾN MÃI
-- ==============================================================================
CREATE TABLE product_inventory (
    inventory_id BIGSERIAL PRIMARY KEY,
    variant_id BIGINT NOT NULL UNIQUE REFERENCES product_variants(variant_id)
        ON DELETE CASCADE,
    warehouse_code VARCHAR(100) NOT NULL DEFAULT 'default',
    quantity_on_hand INT NOT NULL DEFAULT 0 CHECK (quantity_on_hand >= 0),
    quantity_reserved INT NOT NULL DEFAULT 0 CHECK (quantity_reserved >= 0),
    quantity_available INT GENERATED ALWAYS AS
        (quantity_on_hand - quantity_reserved) STORED,
    low_stock_threshold INT NOT NULL DEFAULT 0 CHECK (low_stock_threshold >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (quantity_reserved <= quantity_on_hand)
);

CREATE TABLE inventory_movements (
    movement_id BIGSERIAL PRIMARY KEY,
    inventory_id BIGINT NOT NULL REFERENCES product_inventory(inventory_id)
        ON DELETE CASCADE,
    movement_type VARCHAR(30) NOT NULL,
    quantity_delta INT NOT NULL,
    reference_type VARCHAR(50),
    reference_id VARCHAR(100),
    reason TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (movement_type IN ('import', 'sale', 'return', 'reserve', 'release', 'adjustment'))
);

CREATE TABLE vouchers (
    voucher_id BIGSERIAL PRIMARY KEY,
    platform_id SMALLINT NOT NULL REFERENCES platforms(platform_id),
    seller_id BIGINT REFERENCES sellers(seller_id),
    voucher_code VARCHAR(100) NOT NULL,
    voucher_name VARCHAR(255),
    discount_type VARCHAR(30) NOT NULL,
    discount_value NUMERIC(18, 2) NOT NULL CHECK (discount_value >= 0),
    max_discount_amount NUMERIC(18, 2) CHECK (max_discount_amount IS NULL OR max_discount_amount >= 0),
    min_order_amount NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (min_order_amount >= 0),
    usage_limit INT CHECK (usage_limit IS NULL OR usage_limit >= 0),
    used_count INT NOT NULL DEFAULT 0 CHECK (used_count >= 0),
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform_id, voucher_code),
    CHECK (discount_type IN ('percent', 'fixed_amount', 'free_shipping')),
    CHECK (status IN ('active', 'inactive', 'expired')),
    CHECK (ends_at > starts_at)
);

-- ==============================================================================
-- 4. GIỎ HÀNG & ĐƠN HÀNG
-- ==============================================================================
CREATE TABLE carts (
    cart_id BIGSERIAL PRIMARY KEY,
    platform_id SMALLINT NOT NULL REFERENCES platforms(platform_id),
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    status VARCHAR(30) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform_id, customer_id),
    CHECK (status IN ('active', 'ordered', 'abandoned', 'expired'))
);

CREATE TABLE cart_items (
    cart_item_id BIGSERIAL PRIMARY KEY,
    cart_id BIGINT NOT NULL REFERENCES carts(cart_id) ON DELETE CASCADE,
    variant_id BIGINT NOT NULL REFERENCES product_variants(variant_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(18, 2) NOT NULL CHECK (unit_price >= 0),
    added_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (cart_id, variant_id)
);

CREATE TABLE orders (
    order_id BIGSERIAL PRIMARY KEY,
    platform_id SMALLINT NOT NULL REFERENCES platforms(platform_id),
    platform_order_id VARCHAR(100) NOT NULL,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    seller_id BIGINT NOT NULL REFERENCES sellers(seller_id),
    shipping_address_id BIGINT REFERENCES customer_addresses(address_id),
    voucher_id BIGINT REFERENCES vouchers(voucher_id),
    order_status VARCHAR(30) NOT NULL DEFAULT 'pending',
    payment_status VARCHAR(30) NOT NULL DEFAULT 'unpaid',
    fulfillment_status VARCHAR(30) NOT NULL DEFAULT 'unfulfilled',
    currency VARCHAR(10) NOT NULL DEFAULT 'VND',
    subtotal_amount NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (subtotal_amount >= 0),
    discount_amount NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    shipping_fee NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (shipping_fee >= 0),
    tax_amount NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
    total_amount NUMERIC(18, 2) NOT NULL CHECK (total_amount >= 0),
    ordered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform_id, platform_order_id),
    CHECK (order_status IN ('pending', 'confirmed', 'packed', 'shipping', 'completed', 'cancelled', 'returned')),
    CHECK (payment_status IN ('unpaid', 'paid', 'refunded', 'failed')),
    CHECK (fulfillment_status IN ('unfulfilled', 'processing', 'fulfilled', 'returned', 'cancelled'))
);

CREATE TABLE order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    variant_id BIGINT NOT NULL REFERENCES product_variants(variant_id),
    product_name TEXT NOT NULL,
    sku VARCHAR(255),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(18, 2) NOT NULL CHECK (unit_price >= 0),
    discount_amount NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    total_amount NUMERIC(18, 2) NOT NULL CHECK (total_amount >= 0)
);
-- ==============================================================================
-- 5. THANH TOÁN & VẬN CHUYỂN
-- ==============================================================================
CREATE TABLE payments (
    payment_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    payment_method VARCHAR(50) NOT NULL,
    provider VARCHAR(100),
    amount NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    currency VARCHAR(10) NOT NULL DEFAULT 'VND',
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    paid_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (payment_method IN ('cod', 'card', 'bank_transfer', 'momo', 'zalopay', 'shopeepay', 'wallet')),
    CHECK (status IN ('pending', 'authorized', 'paid', 'failed', 'cancelled', 'refunded'))
);

CREATE TABLE shipments (
    shipment_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    carrier_name VARCHAR(100),
    tracking_number VARCHAR(100),
    shipping_method VARCHAR(100),
    status VARCHAR(30) NOT NULL DEFAULT 'preparing',
    event_history JSONB,
    shipped_at TIMESTAMPTZ,
    estimated_delivery_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tracking_number),
    CHECK (status IN ('preparing', 'picked_up', 'in_transit', 'delivered', 'failed', 'returned', 'cancelled'))
);

-- ==============================================================================
-- 6. REVIEW & MEDIA
-- ==============================================================================
CREATE TABLE product_reviews (
    review_id BIGSERIAL PRIMARY KEY,
    platform_id SMALLINT NOT NULL REFERENCES platforms(platform_id),
    platform_review_id VARCHAR(100) NOT NULL,
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    order_item_id BIGINT REFERENCES order_items(order_item_id),
    customer_id BIGINT REFERENCES customers(customer_id),
    seller_id BIGINT REFERENCES sellers(seller_id),
    rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    title TEXT,
    content TEXT,
    media_urls JSONB,
    delivery_rating INT CHECK (delivery_rating IS NULL OR delivery_rating BETWEEN 1 AND 5),
    seller_rating INT CHECK (seller_rating IS NULL OR seller_rating BETWEEN 1 AND 5),
    helpful_count INT NOT NULL DEFAULT 0 CHECK (helpful_count >= 0),
    status VARCHAR(30) NOT NULL DEFAULT 'published',
    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform_id, platform_review_id),
    CHECK (status IN ('published', 'hidden', 'deleted', 'pending'))
);
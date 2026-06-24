#!/usr/bin/env python3
"""
add_dashboard_filters.py
=========================
Thêm filter Platform (và tuỳ chọn Date Range) vào Dashboard trên Metabase.
Hỗ trợ cả silver_real_serving (Dashboard 3) và gold_serving (Dashboard 5).

Cách sử dụng:
    # Dashboard 3 (silver) — chỉ platform
    python3 add_dashboard_filters.py --dashboard 3 --schema silver_real_serving

    # Dashboard 5 (gold) — platform + date range
    python3 add_dashboard_filters.py --dashboard 5 --schema gold_serving --add-date-filter

    # Dry-run
    python3 add_dashboard_filters.py --dashboard 5 --schema gold_serving --add-date-filter --dry-run
"""

import argparse
import json
import re
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Cấu hình
# ---------------------------------------------------------------------------
METABASE_URL = "http://localhost:3001"
METABASE_USER = "admin@example.com"
METABASE_PASSWORD = "REDACTED_METABASE_PASSWORD"

# SQL keywords — không được nhận diện nhầm làm alias
SQL_KEYWORDS = {
    "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "JOIN", "ON",
    "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "SET", "AND", "OR",
    "AS", "SELECT", "FROM", "BY", "ASC", "DESC", "IN", "NOT", "IS",
    "NULL", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END",
    "INSERT", "UPDATE", "DELETE", "INTO", "VALUES", "WITH", "UNION",
    "ALL", "EXISTS", "LIKE", "TRUE", "FALSE",
}

# Bảng có platform_id
TABLES_PLATFORM = {
    "silver_real_serving": [
        "products", "product_reviews", "sellers", "customers",
    ],
    "gold_serving": [
        "fct_orders", "fct_order_items", "fct_product_reviews",
        "fct_shipments", "dim_sellers", "dim_customers", "dim_brands",
        "dim_platforms",
    ],
}

# Bảng có created_at (cho date filter)
TABLES_CREATED_AT = {
    "gold_serving": [
        "fct_orders", "fct_order_items", "fct_product_reviews",
        "fct_shipments", "dim_products", "dim_sellers", "dim_customers",
        "dim_brands", "dim_platforms",
    ],
    "silver_real_serving": [
        "product_reviews", "customers",
    ],
}

# Bảng có event_date (String) — dùng cho silver nếu cần
TABLES_EVENT_DATE = {
    "silver_real_serving": [
        "products", "product_reviews", "sellers", "customers",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _clean_alias(alias):
    """Trả về alias hợp lệ (không phải SQL keyword), hoặc None."""
    if alias and alias.upper() not in SQL_KEYWORDS:
        return alias
    return None


def detect_tables_and_aliases(sql, schema):
    """
    Tìm tất cả bảng từ schema trong SQL, cùng alias.
    Trả về list of (table_name, alias_or_none).
    """
    pattern = rf"(?:FROM|JOIN)\s+{re.escape(schema)}\.(\w+)(?:\s+(\w+))?"
    matches = re.findall(pattern, sql, re.IGNORECASE)
    result = []
    for table, raw_alias in matches:
        alias = _clean_alias(raw_alias)
        result.append((table, alias))
    return result


def find_insert_position(sql, has_where):
    """
    Tìm vị trí (index) trong SQL để chèn filter.
    Trả về (position, before_keyword) hoặc (len(sql), None).
    """
    sql_upper = sql.upper()

    if has_where:
        where_pos = sql_upper.find("WHERE")
        for keyword in ["GROUP BY", "ORDER BY", "HAVING", "LIMIT"]:
            idx = sql_upper.find(keyword)
            if idx > where_pos:
                return idx, keyword
        return len(sql), None
    else:
        for keyword in ["GROUP BY", "ORDER BY", "HAVING", "LIMIT"]:
            idx = sql_upper.find(keyword)
            if idx > 0:
                return idx, keyword
        return len(sql), None


def add_filters_to_sql(sql, schema, add_platform=True, add_date=False):
    """
    Thêm filter vào SQL. Trả về (new_sql, added_platform, added_date).
    """
    tables = detect_tables_and_aliases(sql, schema)
    if not tables:
        return sql, False, False

    platform_tables = TABLES_PLATFORM.get(schema, [])
    date_tables = TABLES_CREATED_AT.get(schema, [])

    # Tìm bảng chính cho platform filter
    platform_col = None
    if add_platform:
        for table, alias in tables:
            if table in platform_tables:
                prefix = f"{alias}." if alias else ""
                platform_col = f"{prefix}platform_id"
                break

    # Tìm bảng chính cho date filter
    date_col = None
    if add_date:
        for table, alias in tables:
            if table in date_tables:
                prefix = f"{alias}." if alias else ""
                date_col = f"{prefix}created_at"
                break

    if not platform_col and not date_col:
        return sql, False, False

    # Kiểm tra xem đã có filter chưa
    already_has_platform = "{{platform}}" in sql
    already_has_date_start = "{{date_start}}" in sql

    added_platform = False
    added_date = False

    snippets = []
    if platform_col and not already_has_platform:
        snippets.append(f"[[AND {platform_col} = {{{{platform}}}}]]")
        added_platform = True
    if date_col and not already_has_date_start:
        snippets.append(f"[[AND {date_col} >= toDateTime({{{{date_start}}}})]]")
        snippets.append(f"[[AND {date_col} <= toDateTime({{{{date_end}}}})]]")
        added_date = True

    if not snippets:
        return sql, already_has_platform and add_platform, already_has_date_start and add_date

    filter_text = "\n".join(snippets)

    sql_upper = sql.upper()
    has_where = "WHERE" in sql_upper

    pos, _ = find_insert_position(sql, has_where)

    if has_where:
        new_sql = sql[:pos].rstrip() + "\n" + filter_text + "\n" + sql[pos:]
    else:
        where_block = "WHERE 1=1\n" + filter_text
        new_sql = sql[:pos].rstrip() + "\n" + where_block + "\n" + sql[pos:]

    return new_sql, added_platform or (already_has_platform and add_platform), added_date or (already_has_date_start and add_date)


# ---------------------------------------------------------------------------
# Metabase API Client
# ---------------------------------------------------------------------------
class MetabaseClient:
    def __init__(self, base_url, user, password, dry_run=False):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.dry_run = dry_run
        self.session = requests.Session()
        self.token = None

    def login(self):
        print(f"🔐 Đang đăng nhập Metabase ...")
        resp = self.session.post(
            f"{self.base_url}/api/session",
            json={"username": self.user, "password": self.password},
        )
        resp.raise_for_status()
        self.token = resp.json()["id"]
        self.session.headers["X-Metabase-Session"] = self.token
        print("   ✅ Đăng nhập thành công!")

    def _get(self, path):
        resp = self.session.get(f"{self.base_url}{path}")
        resp.raise_for_status()
        return resp.json()

    def _put(self, path, payload):
        resp = self.session.put(f"{self.base_url}{path}", json=payload)
        resp.raise_for_status()
        return resp.json()

    def get_dashboard(self, dashboard_id):
        return self._get(f"/api/dashboard/{dashboard_id}")

    def get_card(self, card_id):
        return self._get(f"/api/card/{card_id}")

    def update_card(self, card_id, payload):
        if self.dry_run:
            return {"id": card_id}
        return self._put(f"/api/card/{card_id}", payload)

    def update_dashboard(self, dashboard_id, payload):
        if self.dry_run:
            return payload
        return self._put(f"/api/dashboard/{dashboard_id}", payload)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def process_dashboard(client, dashboard_id, schema, add_platform, add_date, dry_run):
    """Xử lý 1 dashboard: thêm filter platform (và date nếu cần)."""

    dashboard = client.get_dashboard(dashboard_id)
    print(f"\n📋 Dashboard: {dashboard.get('name')} (ID={dashboard_id})")

    dashcards = dashboard.get("dashcards", [])
    print(f"   📊 {len(dashcards)} dashcards")

    # Detect database ID từ card đầu tiên
    db_id = None
    for dc in dashcards:
        card = dc.get("card", {})
        dq = card.get("dataset_query", {})
        if dq.get("database"):
            db_id = dq["database"]
            break

    # ------ Bước 1: Cập nhật SQL cho từng card ------
    print(f"\n{'='*70}")
    print(f"📝 CẬP NHẬT SQL CHO TỪNG CARD")
    print(f"{'='*70}")

    cards_with_platform = []
    cards_with_date = []
    cards_skipped = []

    for dc in dashcards:
        card_id = dc.get("card_id")
        card = dc.get("card", {})

        if card_id is None:
            cards_skipped.append(dc["id"])
            continue

        card_name = card.get("name", "?")
        full_card = client.get_card(card_id)
        dq = full_card.get("dataset_query", {})
        stages = dq.get("stages", [])

        # Tìm SQL
        sql = None
        stage_idx = None
        for idx, s in enumerate(stages):
            if s.get("native"):
                sql = s["native"]
                stage_idx = idx
                break

        if not sql:
            print(f"   ⚠️  Card {card_id} ({card_name}): không có SQL, bỏ qua.")
            cards_skipped.append(dc["id"])
            continue

        # Thêm filter
        new_sql, has_platform, has_date = add_filters_to_sql(
            sql, schema,
            add_platform=add_platform,
            add_date=add_date,
        )

        if new_sql == sql:
            # Không thay đổi gì (đã có filter hoặc không áp dụng được)
            if has_platform:
                cards_with_platform.append((dc["id"], card_id, card_name))
            if has_date:
                cards_with_date.append((dc["id"], card_id, card_name))
            if has_platform or has_date:
                print(f"   ✅ Card {card_id} ({card_name}): đã có filter, bỏ qua.")
            else:
                print(f"   ⚠️  Card {card_id} ({card_name}): không áp dụng được filter.")
                cards_skipped.append(dc["id"])
            continue

        print(f"\n   📝 Card {card_id}: {card_name}")
        print(f"      OLD: {sql[:100]}...")
        print(f"      NEW: {new_sql[:120]}...")

        # Chuẩn bị template-tags
        template_tags = {}
        if stage_idx is not None:
            template_tags = dict(stages[stage_idx].get("template-tags", {}))

        if has_platform and "platform" not in template_tags:
            # Dùng type phù hợp với kiểu dữ liệu
            if schema == "gold_serving":
                tag_type = "text"
            else:
                tag_type = "number"
            template_tags["platform"] = {
                "id": f"platform_tag_{card_id}",
                "name": "platform",
                "display-name": "Nền tảng",
                "type": tag_type,
            }

        if has_date:
            if "date_start" not in template_tags:
                template_tags["date_start"] = {
                    "id": f"date_start_tag_{card_id}",
                    "name": "date_start",
                    "display-name": "Từ ngày",
                    "type": "date",
                }
            if "date_end" not in template_tags:
                template_tags["date_end"] = {
                    "id": f"date_end_tag_{card_id}",
                    "name": "date_end",
                    "display-name": "Đến ngày",
                    "type": "date",
                }

        # Cập nhật card
        new_stages = list(stages)
        new_stages[stage_idx] = {
            **stages[stage_idx],
            "native": new_sql,
            "template-tags": template_tags,
        }
        new_dq = {**dq, "stages": new_stages}

        if not dry_run:
            try:
                client.update_card(card_id, {"dataset_query": new_dq})
                print(f"      ✅ Đã cập nhật!")
                time.sleep(0.3)
            except requests.HTTPError as e:
                print(f"      ❌ Lỗi: {e}")
                if e.response is not None:
                    print(f"         {e.response.text[:300]}")
                continue
        else:
            print(f"      [DRY-RUN]")

        if has_platform:
            cards_with_platform.append((dc["id"], card_id, card_name))
        if has_date:
            cards_with_date.append((dc["id"], card_id, card_name))

    # ------ Bước 2: Thêm parameters vào dashboard ------
    print(f"\n{'='*70}")
    print(f"🎛️  THÊM PARAMETERS VÀO DASHBOARD")
    print(f"{'='*70}")

    existing_params = list(dashboard.get("parameters", []))
    existing_slugs = {p.get("slug") for p in existing_params}

    params = list(existing_params)

    # Platform parameter
    PLATFORM_PARAM_ID = "platform_filter"
    if add_platform and "platform" not in existing_slugs:
        if schema == "gold_serving":
            param_type = "string/="
            section = "string"
        else:
            param_type = "number/="
            section = "number"
        params.append({
            "id": PLATFORM_PARAM_ID,
            "type": param_type,
            "name": "Nền tảng",
            "slug": "platform",
            "sectionId": section,
        })
        print(f"   ➕ Thêm parameter: Nền tảng ({param_type})")
    else:
        # Tìm ID hiện tại
        for p in existing_params:
            if p.get("slug") == "platform":
                PLATFORM_PARAM_ID = p["id"]
                break
        if "platform" in existing_slugs:
            print(f"   ✅ Parameter 'platform' đã tồn tại.")

    # Date parameters
    DATE_START_PARAM_ID = "date_start_filter"
    DATE_END_PARAM_ID = "date_end_filter"
    if add_date:
        if "date_start" not in existing_slugs:
            params.append({
                "id": DATE_START_PARAM_ID,
                "type": "date/single",
                "name": "Từ ngày",
                "slug": "date_start",
                "sectionId": "date",
            })
            print(f"   ➕ Thêm parameter: Từ ngày")
        else:
            for p in existing_params:
                if p.get("slug") == "date_start":
                    DATE_START_PARAM_ID = p["id"]
            print(f"   ✅ Parameter 'date_start' đã tồn tại.")

        if "date_end" not in existing_slugs:
            params.append({
                "id": DATE_END_PARAM_ID,
                "type": "date/single",
                "name": "Đến ngày",
                "slug": "date_end",
                "sectionId": "date",
            })
            print(f"   ➕ Thêm parameter: Đến ngày")
        else:
            for p in existing_params:
                if p.get("slug") == "date_end":
                    DATE_END_PARAM_ID = p["id"]
            print(f"   ✅ Parameter 'date_end' đã tồn tại.")

    # ------ Bước 3: Map parameters → dashcards ------
    print(f"\n{'='*70}")
    print(f"🔗 MAP PARAMETERS → DASHCARDS")
    print(f"{'='*70}")

    updated_dashcards = []
    for dc in dashcards:
        dc_id = dc["id"]
        card_id = dc.get("card_id")

        dc_copy = {
            "id": dc_id,
            "card_id": card_id,
            "size_x": dc.get("size_x"),
            "size_y": dc.get("size_y"),
            "col": dc.get("col"),
            "row": dc.get("row"),
            "dashboard_tab_id": dc.get("dashboard_tab_id"),
            "parameter_mappings": list(dc.get("parameter_mappings", [])),
            "visualization_settings": dc.get("visualization_settings", {}),
        }

        if card_id is None:
            updated_dashcards.append(dc_copy)
            continue

        existing_map_ids = {m.get("parameter_id") for m in dc_copy["parameter_mappings"]}

        # Platform mapping
        is_platform_card = any(c_id == card_id for (_, c_id, _) in cards_with_platform)
        if is_platform_card and PLATFORM_PARAM_ID not in existing_map_ids:
            dc_copy["parameter_mappings"].append({
                "parameter_id": PLATFORM_PARAM_ID,
                "card_id": card_id,
                "target": ["variable", ["template-tag", "platform"]],
            })

        # Date mappings
        is_date_card = any(c_id == card_id for (_, c_id, _) in cards_with_date)
        if is_date_card:
            if DATE_START_PARAM_ID not in existing_map_ids:
                dc_copy["parameter_mappings"].append({
                    "parameter_id": DATE_START_PARAM_ID,
                    "card_id": card_id,
                    "target": ["variable", ["template-tag", "date_start"]],
                })
            if DATE_END_PARAM_ID not in existing_map_ids:
                dc_copy["parameter_mappings"].append({
                    "parameter_id": DATE_END_PARAM_ID,
                    "card_id": card_id,
                    "target": ["variable", ["template-tag", "date_end"]],
                })

        mappings_count = len(dc_copy["parameter_mappings"])
        if mappings_count > 0 and card_id:
            print(f"   🔗 dc={dc_id}, card={card_id}: {mappings_count} mappings")

        updated_dashcards.append(dc_copy)

    # ------ Bước 4: Cập nhật dashboard ------
    print(f"\n{'='*70}")
    print(f"📋 CẬP NHẬT DASHBOARD {dashboard_id}")
    print(f"{'='*70}")

    # Include tabs nếu dashboard có tabs (tránh FK constraint error)
    existing_tabs = dashboard.get("tabs", [])
    dashboard_payload = {
        "parameters": params,
        "dashcards": updated_dashcards,
    }
    if existing_tabs:
        dashboard_payload["tabs"] = existing_tabs

    if not dry_run:
        try:
            result = client.update_dashboard(dashboard_id, dashboard_payload)
            print("   ✅ Dashboard cập nhật thành công!")
            for p in result.get("parameters", []):
                print(f"      📎 {p.get('name')} (slug={p.get('slug')}, type={p.get('type')})")
        except requests.HTTPError as e:
            print(f"   ❌ Lỗi: {e}")
            if e.response is not None:
                print(f"      {e.response.text[:500]}")
    else:
        print(f"   [DRY-RUN] Parameters: {len(params)}, Dashcards: {len(updated_dashcards)}")

    # Tổng kết
    print(f"\n{'='*70}")
    print(f"🎉 HOÀN TẤT DASHBOARD {dashboard_id}!")
    print(f"{'='*70}")
    print(f"   ✅ Cards có platform filter: {len(cards_with_platform)}")
    if add_date:
        print(f"   ✅ Cards có date filter: {len(cards_with_date)}")
    print(f"   ⏭️  Cards bỏ qua: {len(cards_skipped)}")

    return len(cards_with_platform), len(cards_with_date)


def main():
    parser = argparse.ArgumentParser(
        description="Thêm filter Platform + Date vào Dashboard Metabase."
    )
    parser.add_argument(
        "--dashboard", type=int, nargs="+", required=True,
        help="Dashboard ID(s) cần xử lý (ví dụ: --dashboard 3 5)",
    )
    parser.add_argument(
        "--schema", type=str, nargs="+",
        help="Schema tương ứng với mỗi dashboard (ví dụ: --schema silver_real_serving gold_serving)",
    )
    parser.add_argument(
        "--add-date-filter", action="store_true",
        help="Thêm filter theo ngày (date_start, date_end).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Chỉ hiển thị, không thay đổi.",
    )
    args = parser.parse_args()

    # Validate schema count matches dashboard count
    schemas = args.schema or []
    dashboards = args.dashboard
    if len(schemas) == 0:
        # Auto-detect
        schemas = []
        for d in dashboards:
            if d == 3:
                schemas.append("silver_real_serving")
            elif d == 5:
                schemas.append("gold_serving")
            else:
                print(f"❌ Cần chỉ định --schema cho dashboard {d}")
                sys.exit(1)
    elif len(schemas) == 1 and len(dashboards) > 1:
        schemas = schemas * len(dashboards)
    elif len(schemas) != len(dashboards):
        print("❌ Số lượng --schema phải bằng số lượng --dashboard")
        sys.exit(1)

    print("=" * 70)
    print("🔧 THÊM FILTER VÀO METABASE DASHBOARDS")
    print("=" * 70)
    print(f"   Dashboards: {dashboards}")
    print(f"   Schemas:    {schemas}")
    print(f"   Date filter: {args.add_date_filter}")
    print(f"   Dry-run:    {args.dry_run}")
    print("=" * 70)

    client = MetabaseClient(METABASE_URL, METABASE_USER, METABASE_PASSWORD, dry_run=args.dry_run)
    client.login()

    for dashboard_id, schema in zip(dashboards, schemas):
        # Dashboard 3 chỉ có platform filter
        # Dashboard 5 có cả platform + date filter
        add_date = args.add_date_filter
        # Nếu xử lý dashboard 3 cùng lúc, không thêm date filter cho nó
        # trừ khi user chỉ rõ
        if dashboard_id == 3 and len(dashboards) > 1:
            add_date_for_this = False
        else:
            add_date_for_this = add_date

        process_dashboard(
            client, dashboard_id, schema,
            add_platform=True,
            add_date=add_date_for_this,
            dry_run=args.dry_run,
        )

    print(f"\n{'='*70}")
    print("📖 HƯỚNG DẪN SỬ DỤNG:")
    print("=" * 70)
    for d_id in dashboards:
        print(f"\n   Dashboard {d_id}: http://localhost:3001/dashboard/{d_id}")
    print(f"\n   🔹 Filter 'Nền tảng':")
    if "gold_serving" in schemas:
        print(f"      • Nhập: '1' = Tiki, '2' = Shopee, '3' = Sendo")
    if "silver_real_serving" in schemas:
        print(f"      • Nhập: 1 = Tiki, 2 = Sendo, 3 = Cho Tot")
    print(f"      • Để trống = Tất cả")
    if args.add_date_filter:
        print(f"\n   🔹 Filter 'Từ ngày' / 'Đến ngày':")
        print(f"      • Chọn ngày bắt đầu và kết thúc")
        print(f"      • Để trống = Không giới hạn")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()

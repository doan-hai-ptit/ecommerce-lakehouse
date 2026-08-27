#!/usr/bin/env python3
"""
add_platform_filter.py
=======================
Thêm filter "Nền tảng" (Platform) vào Dashboard 3 trên Metabase.
Filter cho phép chọn Tiki, Sendo, Cho Tot hoặc Tất cả.

Cách hoạt động:
  1. Cập nhật SQL mỗi card: thêm điều kiện lọc platform_id (optional snippet)
  2. Thêm parameter "Platform" lên dashboard
  3. Map parameter tới mỗi dashcard

Cách sử dụng:
    python add_platform_filter.py
    python add_platform_filter.py --dry-run
"""

import argparse
import json
import os
import re
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Cấu hình
# ---------------------------------------------------------------------------
METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3001")
METABASE_USER = os.getenv("METABASE_USER", "admin@example.com")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "change_me")
DASHBOARD_ID = 3
DB_ID = 8  # ClickHouse database ID trong Metabase

# Platform mapping: platform_id -> platform_name
PLATFORMS = {
    1: "Tiki",
    2: "Sendo",
    3: "Cho Tot",
}

# Bảng nào có cột platform_id
TABLES_WITH_PLATFORM_ID = ["products", "product_reviews", "sellers", "customers"]

# Parameter ID dùng trên dashboard
PARAM_SLUG = "platform"
PARAM_ID = "platform_filter"


# ---------------------------------------------------------------------------
# Helper: Xác định bảng chính (bảng đầu tiên trong FROM) và alias
# ---------------------------------------------------------------------------
SQL_KEYWORDS = {
    "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "JOIN", "ON",
    "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "SET", "AND", "OR",
    "AS", "SELECT", "FROM", "BY", "ASC", "DESC", "IN", "NOT", "IS",
    "NULL", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END",
    "INSERT", "UPDATE", "DELETE", "INTO", "VALUES",
}


def _clean_alias(alias):
    """Trả về alias nếu hợp lệ (không phải SQL keyword), None nếu không."""
    if alias and alias.upper() not in SQL_KEYWORDS:
        return alias
    return None


def detect_main_table_and_alias(sql):
    """
    Phân tích SQL để tìm bảng chính có platform_id và alias (nếu có).
    Trả về (table_name, alias_or_none, has_platform_id).

    Ưu tiên:
      - Bảng trong FROM (trước JOIN)
      - Nếu FROM bảng không có platform_id, thử bảng trong JOIN
    """
    # Tìm tất cả bảng được reference
    # Pattern: silver_real_serving.<table> [alias]
    from_match = re.search(
        r"FROM\s+silver_real_serving\.(\w+)(?:\s+(\w+))?",
        sql,
        re.IGNORECASE,
    )
    join_matches = re.findall(
        r"JOIN\s+silver_real_serving\.(\w+)(?:\s+(\w+))?",
        sql,
        re.IGNORECASE,
    )

    if from_match:
        table = from_match.group(1)
        alias = _clean_alias(from_match.group(2))
        if table in TABLES_WITH_PLATFORM_ID:
            return table, alias, True

    # Nếu bảng FROM không có platform_id, kiểm tra JOIN
    for table, raw_alias in join_matches:
        alias = _clean_alias(raw_alias)
        if table in TABLES_WITH_PLATFORM_ID:
            return table, alias, True

    # Fallback: bảng FROM nhưng không có platform_id
    if from_match:
        return from_match.group(1), _clean_alias(from_match.group(2)), False

    return None, None, False


def add_platform_filter_to_sql(sql):
    """
    Thêm điều kiện lọc platform_id vào SQL.
    Sử dụng Metabase optional clause [[...]] để filter chỉ áp dụng khi
    người dùng chọn giá trị.

    Trả về (new_sql, has_filter).
    """
    table, alias, has_platform = detect_main_table_and_alias(sql)

    if not has_platform:
        return sql, False

    # Xây dựng prefix cho cột (alias.platform_id hoặc platform_id)
    col_prefix = f"{alias}.platform_id" if alias else "platform_id"

    # Snippet filter
    filter_snippet = f"[[AND {col_prefix} = {{{{platform}}}}]]"

    # Xác định vị trí chèn filter
    sql_upper = sql.upper()

    # Trường hợp 1: Đã có WHERE -> chèn trước GROUP BY / ORDER BY / LIMIT / cuối
    if "WHERE" in sql_upper:
        # Tìm vị trí cuối cùng của WHERE clause (trước GROUP BY / ORDER BY / HAVING / LIMIT)
        for keyword in ["GROUP BY", "ORDER BY", "HAVING", "LIMIT"]:
            idx = sql_upper.find(keyword)
            if idx > sql_upper.find("WHERE"):
                # Chèn trước keyword
                new_sql = sql[:idx].rstrip() + "\n" + filter_snippet + "\n" + sql[idx:]
                return new_sql, True

        # Không có GROUP BY/ORDER BY/LIMIT -> chèn cuối
        new_sql = sql.rstrip() + "\n" + filter_snippet
        return new_sql, True

    # Trường hợp 2: Không có WHERE -> thêm WHERE 1=1 rồi filter
    filter_with_where = f"WHERE 1=1\n{filter_snippet}"

    for keyword in ["GROUP BY", "ORDER BY", "HAVING", "LIMIT"]:
        idx = sql_upper.find(keyword)
        if idx > 0:
            new_sql = sql[:idx].rstrip() + "\n" + filter_with_where + "\n" + sql[idx:]
            return new_sql, True

    # Không có gì -> chèn cuối
    new_sql = sql.rstrip() + "\n" + filter_with_where
    return new_sql, True


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
        print(f"🔐 Đang đăng nhập Metabase tại {self.base_url} ...")
        if self.dry_run:
            print("   [DRY-RUN] Bỏ qua đăng nhập.")
            return
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
# Main logic
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Thêm filter Platform vào Dashboard 3 trên Metabase."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chỉ hiển thị kế hoạch, không thay đổi thật.",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("🔧 THÊM FILTER PLATFORM VÀO DASHBOARD 3")
    print("=" * 70)

    client = MetabaseClient(
        METABASE_URL, METABASE_USER, METABASE_PASSWORD, dry_run=args.dry_run
    )
    client.login()

    # ----- Bước 1: Lấy dashboard hiện tại -----
    print("\n📋 Đang lấy thông tin Dashboard 3 ...")
    if args.dry_run:
        print("   [DRY-RUN] Sẽ giả lập dữ liệu.")
        # In dry-run mode we need real data to plan, so still fetch
        # But we won't modify anything
        temp_client = MetabaseClient(METABASE_URL, METABASE_USER, METABASE_PASSWORD)
        temp_client.login()
        dashboard = temp_client.get_dashboard(DASHBOARD_ID)
    else:
        dashboard = client.get_dashboard(DASHBOARD_ID)

    dashcards = dashboard.get("dashcards", [])
    print(f"   📊 Tìm thấy {len(dashcards)} dashcards")

    # ----- Bước 2: Cập nhật SQL từng card -----
    print(f"\n{'=' * 70}")
    print("📝 CẬP NHẬT SQL CHO TỪNG CARD (thêm filter platform_id)")
    print(f"{'=' * 70}")

    cards_with_filter = []  # (dc_id, card_id, card_name) — cards đã thêm filter
    cards_skipped = []      # cards không thêm filter (heading, etc.)

    for dc in dashcards:
        card_id = dc.get("card_id")
        card = dc.get("card", {})

        if card_id is None:
            # Heading/text card
            cards_skipped.append(dc["id"])
            continue

        card_name = card.get("name", "?")

        # Lấy full card details
        if args.dry_run:
            full_card = temp_client.get_card(card_id)
        else:
            full_card = client.get_card(card_id)

        dq = full_card.get("dataset_query", {})
        stages = dq.get("stages", [])

        # Tìm SQL trong stages
        sql = None
        stage_idx = None
        for idx, s in enumerate(stages):
            if s.get("native"):
                sql = s["native"]
                stage_idx = idx
                break

        if not sql:
            print(f"   ⚠️  Card {card_id} ({card_name}): không có native SQL, bỏ qua.")
            cards_skipped.append(dc["id"])
            continue

        # Kiểm tra nếu đã có filter platform
        if "{{platform}}" in sql:
            print(f"   ✅ Card {card_id} ({card_name}): đã có filter, bỏ qua.")
            cards_with_filter.append((dc["id"], card_id, card_name))
            continue

        # Thêm filter vào SQL
        new_sql, has_filter = add_platform_filter_to_sql(sql)

        if not has_filter:
            print(f"   ⚠️  Card {card_id} ({card_name}): bảng không có platform_id, bỏ qua.")
            cards_skipped.append(dc["id"])
            continue

        # Hiển thị thay đổi
        print(f"\n   📝 Card {card_id}: {card_name}")
        print(f"   OLD SQL: {sql[:120]}...")
        print(f"   NEW SQL: {new_sql[:150]}...")

        # Chuẩn bị template-tags
        template_tags = {}
        if stage_idx is not None:
            template_tags = stages[stage_idx].get("template-tags", {})

        template_tags["platform"] = {
            "id": f"platform_tag_{card_id}",
            "name": "platform",
            "display-name": "Nền tảng",
            "type": "number",
        }

        # Cập nhật card
        new_stages = list(stages)
        new_stages[stage_idx] = {
            **stages[stage_idx],
            "native": new_sql,
            "template-tags": template_tags,
        }
        new_dq = {**dq, "stages": new_stages}

        if not args.dry_run:
            try:
                client.update_card(card_id, {"dataset_query": new_dq})
                print(f"   ✅ Đã cập nhật!")
                time.sleep(0.3)  # Rate limiting
            except requests.HTTPError as e:
                print(f"   ❌ Lỗi: {e}")
                if e.response is not None:
                    print(f"      {e.response.text[:300]}")
                continue
        else:
            print(f"   [DRY-RUN] Sẽ cập nhật card {card_id}")

        cards_with_filter.append((dc["id"], card_id, card_name))

    # ----- Bước 3: Thêm parameter vào dashboard -----
    print(f"\n{'=' * 70}")
    print("🎛️  THÊM PARAMETER 'NỀN TẢNG' VÀO DASHBOARD")
    print(f"{'=' * 70}")

    # Kiểm tra xem parameter đã tồn tại chưa
    existing_params = dashboard.get("parameters", [])
    has_existing = any(p.get("slug") == PARAM_SLUG for p in existing_params)

    if has_existing:
        print("   ✅ Parameter 'platform' đã tồn tại trên dashboard.")
    else:
        print("   📝 Thêm parameter mới: Platform")

    # Tạo parameter definition
    new_param = {
        "id": PARAM_ID,
        "type": "number/=",
        "name": "Nền tảng",
        "slug": PARAM_SLUG,
        "sectionId": "number",
    }

    if not has_existing:
        params = existing_params + [new_param]
    else:
        params = existing_params

    # ----- Bước 4: Tạo parameter_mappings cho mỗi dashcard -----
    print(f"\n{'=' * 70}")
    print("🔗 MAP PARAMETER → DASHCARDS")
    print(f"{'=' * 70}")

    # Chuẩn bị danh sách dashcards với parameter_mappings
    updated_dashcards = []
    for dc in dashcards:
        dc_id = dc["id"]
        card_id = dc.get("card_id")

        # Giữ nguyên tất cả thuộc tính của dashcard
        dc_copy = {
            "id": dc_id,
            "card_id": card_id,
            "size_x": dc.get("size_x"),
            "size_y": dc.get("size_y"),
            "col": dc.get("col"),
            "row": dc.get("row"),
            "dashboard_tab_id": dc.get("dashboard_tab_id"),
            "parameter_mappings": list(dc.get("parameter_mappings", [])),
        }

        # Kiểm tra dashcard có filter không
        is_filtered = any(
            c_id == card_id for (_, c_id, _) in cards_with_filter
        )

        if card_id and is_filtered:
            # Kiểm tra mapping đã tồn tại chưa
            existing_mappings = dc_copy["parameter_mappings"]
            already_mapped = any(
                m.get("parameter_id") == PARAM_ID for m in existing_mappings
            )

            if not already_mapped:
                mapping = {
                    "parameter_id": PARAM_ID,
                    "card_id": card_id,
                    "target": ["variable", ["template-tag", "platform"]],
                }
                dc_copy["parameter_mappings"].append(mapping)
                print(f"   🔗 dc_id={dc_id}, card_id={card_id}: mapped")
            else:
                print(f"   ✅ dc_id={dc_id}, card_id={card_id}: đã mapped")
        elif card_id is None:
            # Heading/text card — giữ nguyên
            pass
        else:
            print(f"   ⏭️  dc_id={dc_id}, card_id={card_id}: không có filter, bỏ qua")

        updated_dashcards.append(dc_copy)

    # ----- Bước 5: Cập nhật dashboard -----
    print(f"\n{'=' * 70}")
    print("📋 CẬP NHẬT DASHBOARD")
    print(f"{'=' * 70}")

    dashboard_payload = {
        "parameters": params,
        "dashcards": updated_dashcards,
    }

    if not args.dry_run:
        try:
            result = client.update_dashboard(DASHBOARD_ID, dashboard_payload)
            print("   ✅ Dashboard đã được cập nhật thành công!")
            result_params = result.get("parameters", [])
            print(f"   📊 Parameters: {len(result_params)}")
            for p in result_params:
                print(f"      - {p.get('name')} (slug={p.get('slug')}, type={p.get('type')})")
        except requests.HTTPError as e:
            print(f"   ❌ Lỗi cập nhật dashboard: {e}")
            if e.response is not None:
                print(f"      {e.response.text[:500]}")
    else:
        print("   [DRY-RUN] Sẽ cập nhật dashboard với payload:")
        print(f"      Parameters: {len(params)}")
        print(f"      Dashcards: {len(updated_dashcards)}")
        mapped_count = sum(
            1
            for dc in updated_dashcards
            if any(m.get("parameter_id") == PARAM_ID for m in dc.get("parameter_mappings", []))
        )
        print(f"      Dashcards có filter: {mapped_count}")

    # ----- Hoàn tất -----
    print(f"\n{'=' * 70}")
    print("🎉 HOÀN TẤT!")
    print(f"{'=' * 70}")
    print(f"   ✅ Cards có filter: {len(cards_with_filter)}")
    print(f"   ⏭️  Cards bỏ qua: {len(cards_skipped)}")
    print()
    print("📖 HƯỚNG DẪN SỬ DỤNG:")
    print("   1. Truy cập: http://localhost:3001/dashboard/3")
    print("   2. Nhấn vào filter 'Nền tảng' trên đầu dashboard")
    print("   3. Nhập giá trị platform_id:")
    print("      • 1 = Tiki")
    print("      • 2 = Sendo")
    print("      • 3 = Cho Tot")
    print("      • Để trống = Tất cả")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()

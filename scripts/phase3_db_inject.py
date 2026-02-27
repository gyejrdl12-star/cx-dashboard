#!/usr/bin/env python3
"""
CX 대시보드 Phase 3: cx-insight-store.db → index.html 자동 주입

사용법:
  python3 scripts/phase3_db_inject.py

동작:
  1. /tmp/cx-insight-store.db에서 레코드 로드
  2. index.html 기존 chatId 추출 (중복 제거)
  3. 신규 레코드만 필터
  4. BigQuery에서 세그먼트 조회 (corp_number 직접 사용)
  5. index.html ALL_RECORDS에 주입

장점:
  - Excel 없이 DB → 대시보드 자동화
  - corp_number가 DB에 이미 있어 BQ 매핑 불필요
"""

import json
import os
import re
import sqlite3
import subprocess
import sys
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
INDEX_HTML = os.path.join(ROOT_DIR, "index.html")
INSIGHT_DB = "/tmp/cx-insight-store.db"
SEGMENT_CACHE = os.path.join(ROOT_DIR, "data", "segment_cache.json")


# ── 한도 구간 매핑 ──

def to_limit_tier(grant_limit):
    if grant_limit is None:
        return "미매칭"
    try:
        v = float(grant_limit)
    except (ValueError, TypeError):
        return "미매칭"
    if v < 10_000_000:
        return "1천만 미만"
    elif v < 50_000_000:
        return "1천만~5천만"
    elif v < 100_000_000:
        return "5천만~1억"
    elif v < 500_000_000:
        return "1억~5억"
    else:
        return "5억 이상"


# ── DB 로드 ──

def load_db_records():
    if not os.path.exists(INSIGHT_DB):
        print(f"❌ DB 없음: {INSIGHT_DB}")
        print("  cx-insight-collector.py를 먼저 실행하세요.")
        sys.exit(1)

    conn = sqlite3.connect(INSIGHT_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT id, date, tags, tag_major, impact, journey,
               title, summary, customer_request, result,
               slack_ts, slack_link, corp_number, companyname, assignee
        FROM insights
        ORDER BY date ASC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── HTML에서 기존 chatId 추출 ──

def load_existing_chat_ids():
    if not os.path.exists(INDEX_HTML):
        return set()
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        content = f.read()
    match = re.search(r"const ALL_RECORDS = (\[.*?\]);", content, re.DOTALL)
    if not match:
        return set()
    records = json.loads(match.group(1))
    return set(r.get("chatId", "") for r in records)


def load_existing_records():
    if not os.path.exists(INDEX_HTML):
        return [], ""
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        content = f.read()
    match = re.search(r"const ALL_RECORDS = (\[.*?\]);", content, re.DOTALL)
    if not match:
        return [], content
    return json.loads(match.group(1)), content


# ── BQ 세그먼트 조회 ──

def load_segment_cache():
    if not os.path.exists(SEGMENT_CACHE):
        return {}
    with open(SEGMENT_CACHE, "r", encoding="utf-8") as f:
        rows = json.load(f)
    seg_map = {}
    for row in rows:
        cid = str(row.get("corp_id", ""))
        seg_map[cid] = {
            "detailCategory": row.get("detail_category") or "미매칭",
            "stabilityStatus": row.get("stability_status") or "미매칭",
            "growthStatus": row.get("growth_status") or "미매칭",
            "limitTier": to_limit_tier(row.get("grant_limit")),
        }
    return seg_map


def fetch_segments_from_bq(corp_ids):
    if not corp_ids:
        return load_segment_cache()

    id_list = ", ".join(str(c) for c in corp_ids)
    query = f"""
    SELECT corp_id, detail_category, stability_status, growth_status, grant_limit
    FROM `gowid-prd.mart_customer_segment.segment_base`
    WHERE month_id = (SELECT MAX(month_id) FROM `gowid-prd.mart_customer_segment.segment_base`)
      AND corp_id IN ({id_list})
    """
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=10000", query]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            rows = json.loads(result.stdout)
            seg_map = {}
            for row in rows:
                cid = str(row.get("corp_id", ""))
                seg_map[cid] = {
                    "detailCategory": row.get("detail_category") or "미매칭",
                    "stabilityStatus": row.get("stability_status") or "미매칭",
                    "growthStatus": row.get("growth_status") or "미매칭",
                    "limitTier": to_limit_tier(row.get("grant_limit")),
                }
            # 캐시 업데이트
            with open(SEGMENT_CACHE, "w", encoding="utf-8") as f:
                json.dump(rows, f, ensure_ascii=False, indent=2)
            print(f"  BQ 쿼리 성공 → 캐시 업데이트")
            return seg_map
    except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError):
        pass

    print(f"  BQ 쿼리 실패 → 캐시 사용")
    return load_segment_cache()


# ── DB 레코드 → ALL_RECORDS 포맷 변환 ──

def convert_record(row, seg_map):
    corp_number = re.sub(r"[^0-9]", "", row.get("corp_number") or "")
    seg = seg_map.get(corp_number, {})

    return {
        # 기존 포맷 호환 필드
        "chatId": row["slack_ts"],           # slack_ts를 chatId로
        "date": row["date"],
        "company": row.get("companyname") or "미확인",
        "oldTag": row.get("tags") or "미분류",
        "primaryTag": row.get("tag_major") or "미분류",
        "secondaryTag": row.get("impact"),    # L1/L2/L3
        "confidence": "auto",                 # Claude 자동 태깅
        "detailCategory": seg.get("detailCategory", "미매칭"),
        "stabilityStatus": seg.get("stabilityStatus", "미매칭"),
        "growthStatus": seg.get("growthStatus", "미매칭"),
        "limitTier": seg.get("limitTier", "미매칭"),
        "firstAnswerSec": None,
        "closeSec": None,
        # DB 전용 추가 필드
        "impact": row.get("impact"),
        "journey": row.get("journey"),
        "title": row.get("title"),
        "summary": row.get("summary"),
        "customerRequest": row.get("customer_request"),
        "result": row.get("result"),
        "assignee": row.get("assignee"),
        "slackLink": row.get("slack_link"),
        "source": "db",                       # 데이터 출처 구분
    }


# ── HTML 주입 ──

def inject_into_html(all_records):
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        content = f.read()

    json_str = json.dumps(all_records, ensure_ascii=False, separators=(",", ":"))
    new_line = f"    const ALL_RECORDS = {json_str};"
    content = re.sub(
        r"    const ALL_RECORDS = \[.*?\];",
        new_line,
        content,
        count=1,
        flags=re.DOTALL,
    )

    # 최종 업데이트 날짜
    all_dates = [r.get("date") for r in all_records if r.get("date")]
    if all_dates:
        max_date = max(all_dates)
        dt = datetime.strptime(max_date, "%Y-%m-%d")
        date_display = f"{dt.year}. {dt.month}. {dt.day}."
        content = re.sub(
            r'id="lastUpdateDate">[^<]+<',
            f'id="lastUpdateDate">{date_display}<',
            content,
        )

    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(content)


# ── 메인 ──

def main():
    print("🚀 Phase 3: DB → 대시보드 자동 주입\n")

    # 1. DB 로드
    print("[1/5] DB 로드")
    if not os.path.exists(INSIGHT_DB):
        print(f"  ❌ DB 없음. cx-insight-collector.py 먼저 실행하세요.")
        sys.exit(1)
    db_records = load_db_records()
    print(f"  DB 레코드: {len(db_records)}건 ({db_records[0]['date']} ~ {db_records[-1]['date']})")

    # 2. 기존 chatId 추출 (중복 제거)
    print("[2/5] 기존 데이터 로드 + 중복 제거")
    existing_records, _ = load_existing_records()
    existing_ids = set(r.get("chatId", "") for r in existing_records)
    print(f"  기존: {len(existing_records)}건, 기존 ID: {len(existing_ids)}개")

    new_rows = [r for r in db_records if r["slack_ts"] not in existing_ids]
    print(f"  신규: {len(new_rows)}건")

    if not new_rows:
        print("  새 레코드 없음. 최신 상태입니다.")
        return

    # 3. BQ 세그먼트 조회
    print("[3/5] BQ 세그먼트 조회")
    corp_ids = set()
    for r in new_rows:
        corp = re.sub(r"[^0-9]", "", r.get("corp_number") or "")
        if corp:
            corp_ids.add(int(corp))
    print(f"  고유 사업자번호: {len(corp_ids)}개")
    seg_map = fetch_segments_from_bq(list(corp_ids))
    print(f"  세그먼트 매칭: {len(seg_map)}개 법인")

    # 4. 포맷 변환
    print("[4/5] 포맷 변환")
    converted = [convert_record(r, seg_map) for r in new_rows]
    seg_matched = sum(1 for r in converted if r["detailCategory"] != "미매칭")
    print(f"  세그먼트 주입: {seg_matched}/{len(converted)}건")

    # 5. HTML 주입
    print("[5/5] index.html 주입")
    all_records = existing_records + converted
    inject_into_html(all_records)
    print(f"  ✅ 총 {len(all_records)}건 (기존 {len(existing_records)} + 신규 {len(converted)})")

    print(f"\n  다음: git commit + push → GitHub Pages 반영")
    print(f"  git -C {ROOT_DIR} add index.html && git -C {ROOT_DIR} commit -m 'chore: DB 데이터 {len(converted)}건 추가' && git -C {ROOT_DIR} push")


if __name__ == "__main__":
    main()

import math
import os
import re
from io import BytesIO
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title='BBQ 발주 분석 V6', layout='wide')

FRESH_CODES = [22000000, 22000002, 22000007]
DRUM = 22000013
WING = 22000014
BONELESS = 22000010
OIL = 13000002
RADISH = 22000237

CONV = {
    22000000: 20,
    22000002: 20,
    22000007: 20,
    22000013: 6,
    22000014: 7,
    22000010: 5,
    22000009: 4,
}

REQUIRED_COLUMNS = ['매장코드', '매장명', '코스', '제품코드', '제품명', '합계']


@st.cache_resource
def get_engine():
    db_url = None
    try:
        db_url = st.secrets.get('DATABASE_URL')
    except Exception:
        db_url = None
    db_url = db_url or os.getenv('DATABASE_URL')

    errors = []
    if db_url:
        try:
            eng = create_engine(db_url, future=True, pool_pre_ping=True)
            with eng.connect() as conn:
                conn.execute(text('SELECT 1'))
            return eng, 'external', None
        except Exception as e:
            errors.append(f'외부 DB 연결 실패: {e}')

    try:
        eng = create_engine('sqlite:///bbq_order_history_v6.db', future=True, pool_pre_ping=True)
        with eng.connect() as conn:
            conn.execute(text('SELECT 1'))
        return eng, 'sqlite', (' / '.join(errors) if errors else None)
    except Exception as e:
        errors.append(f'로컬 sqlite 연결 실패: {e}')
        return None, 'none', ' / '.join(errors)


def get_db_status():
    return get_engine()


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    engine_name = conn.engine.dialect.name
    if engine_name == 'sqlite':
        rows = conn.execute(text(f'PRAGMA table_info({table_name})')).fetchall()
        return column_name in [r[1] for r in rows]
    row = conn.execute(text('''
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = :t
          AND column_name = :c
        LIMIT 1
    '''), {'t': table_name, 'c': column_name}).fetchone()
    return row is not None


def _ensure_column(conn, table_name: str, column_name: str, ddl_sql: str) -> None:
    if not _column_exists(conn, table_name, column_name):
        conn.execute(text(ddl_sql))


def init_db() -> Tuple[bool, str]:
    eng, mode, _ = get_db_status()
    if eng is None:
        return False, 'DB 엔진 없음'
    try:
        with eng.begin() as conn:
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS order_lines (
                    source_date TEXT,
                    uploader_name TEXT,
                    upload_key TEXT,
                    source_name TEXT,
                    store_code INTEGER,
                    store_name TEXT,
                    course TEXT,
                    product_code INTEGER,
                    product_name TEXT,
                    qty REAL,
                    conv REAL,
                    converted_qty REAL
                )
            '''))
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS store_assignments (
                    store_code INTEGER PRIMARY KEY,
                    store_name TEXT,
                    bm_name TEXT,
                    updated_at TEXT
                )
            '''))
            _ensure_column(conn, 'order_lines', 'uploader_name', 'ALTER TABLE order_lines ADD COLUMN uploader_name TEXT')
            _ensure_column(conn, 'order_lines', 'upload_key', 'ALTER TABLE order_lines ADD COLUMN upload_key TEXT')
            conn.execute(text("UPDATE order_lines SET uploader_name = COALESCE(NULLIF(uploader_name, ''), '기존데이터')"))
            if conn.engine.dialect.name == 'sqlite':
                conn.execute(text("UPDATE order_lines SET upload_key = source_date || '_' || uploader_name WHERE upload_key IS NULL OR upload_key = ''"))
            else:
                conn.execute(text("UPDATE order_lines SET upload_key = CONCAT(source_date, '_', uploader_name) WHERE upload_key IS NULL OR upload_key = ''"))
        return True, f'DB 초기화 완료 ({mode})'
    except Exception as e:
        return False, f'DB 초기화 실패: {e}'


def q(sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    params = params or {}
    eng, _, _ = get_db_status()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


def exec_sql(sql: str, params: Optional[dict] = None) -> bool:
    params = params or {}
    eng, _, _ = get_db_status()
    if eng is None:
        return False
    with eng.begin() as conn:
        conn.execute(text(sql), params)
    return True


def parse_date_from_name(name: str) -> Optional[str]:
    base = Path(name).stem
    m = re.search(r'(20\d{6})', base)
    if m:
        s = m.group(1)
        return f'{s[:4]}-{s[4:6]}-{s[6:8]}'
    m = re.search(r'(?<!\d)(\d{4})(?!\d)', base)
    if m:
        s = m.group(1)
        return f'2026-{s[:2]}-{s[2:]}'
    return None


def make_upload_key(source_date: str, uploader_name: str) -> str:
    return f"{source_date}_{(uploader_name or '미지정').strip()}"


def normalize_order_df(file) -> pd.DataFrame:
    df = pd.read_excel(file)
    df.columns = [str(c).strip() for c in df.columns]
    if set(REQUIRED_COLUMNS).issubset(df.columns):
        work = df.copy()
    else:
        work = df.iloc[1:].copy()
        work.columns = [str(c).strip() for c in df.iloc[0].tolist()]
    work.columns = [str(c).strip() for c in work.columns]
    missing = [c for c in REQUIRED_COLUMNS if c not in work.columns]
    if missing:
        raise ValueError(f"필수 컬럼 누락: {', '.join(missing)}")
    for c in ['매장코드', '제품코드', '합계']:
        work[c] = pd.to_numeric(work[c], errors='coerce')
    work['합계'] = work['합계'].fillna(0)
    work = work[work['매장코드'].notna() & work['제품코드'].notna()].copy()
    work['매장코드'] = work['매장코드'].astype(int)
    work['제품코드'] = work['제품코드'].astype(int)
    work['매장명'] = work['매장명'].astype(str).str.strip()
    work['코스'] = work['코스'].astype(str).str.strip()
    work['제품명'] = work['제품명'].astype(str).str.strip()
    grouped = work.groupby(['매장코드', '매장명', '코스', '제품코드', '제품명'], as_index=False)['합계'].sum().copy()
    grouped['환산기준'] = grouped['제품코드'].map(CONV).fillna(0)
    grouped['환산수'] = grouped['합계'] * grouped['환산기준']
    return grouped


def item_qty(store_df: pd.DataFrame, codes: List[int]) -> float:
    return float(store_df[store_df['제품코드'].isin(codes)]['합계'].sum())


def pct_change(base: float, curr: float) -> Optional[float]:
    if base == 0:
        return None
    return ((curr - base) / base) * 100


def pct_text(v: Optional[float]) -> str:
    return '-' if v is None else f'{v:.1f}%'


def avg3(values: List[Optional[float]]) -> float:
    vals = [float(v) for v in values if v is not None]
    return 0.0 if not vals else sum(vals) / len(vals)


def latest_uploads() -> List[Dict[str, str]]:
    x = q('SELECT DISTINCT source_date, uploader_name, upload_key FROM order_lines ORDER BY source_date DESC, uploader_name')
    return [] if x.empty else x.to_dict('records')


def latest_dates() -> List[str]:
    x = q('SELECT DISTINCT source_date FROM order_lines ORDER BY source_date DESC')
    return [] if x.empty else x['source_date'].tolist()


def latest_upload_keys_by_user(uploader_name: str, current_date: Optional[str] = None, limit: int = 3) -> List[str]:
    if current_date:
        df = q('SELECT DISTINCT upload_key, source_date FROM order_lines WHERE uploader_name = :u AND source_date < :d ORDER BY source_date DESC', {'u': uploader_name, 'd': current_date})
    else:
        df = q('SELECT DISTINCT upload_key, source_date FROM order_lines WHERE uploader_name = :u ORDER BY source_date DESC', {'u': uploader_name})
    return [] if df.empty else df['upload_key'].tolist()[:limit]


def load_detail_by_upload_key(upload_key: str) -> pd.DataFrame:
    return q('SELECT store_code AS 매장코드, store_name AS 매장명, course AS 코스, product_code AS 제품코드, product_name AS 제품명, qty AS 합계, conv AS 환산기준, converted_qty AS 환산수 FROM order_lines WHERE upload_key = :k', {'k': upload_key})


def load_detail_by_date_all(source_date: str) -> pd.DataFrame:
    return q('SELECT store_code AS 매장코드, store_name AS 매장명, course AS 코스, product_code AS 제품코드, product_name AS 제품명, qty AS 합계, conv AS 환산기준, converted_qty AS 환산수 FROM order_lines WHERE source_date = :d', {'d': source_date})


def save_order_history(source_date: str, uploader_name: str, source_name: str, detail_df: pd.DataFrame) -> Tuple[bool, str]:
    eng, _, _ = get_db_status()
    if eng is None:
        return False, 'DB 엔진 없음'
    upload_key = make_upload_key(source_date, uploader_name)
    try:
        exec_sql('DELETE FROM order_lines WHERE source_date = :d AND uploader_name = :u', {'d': source_date, 'u': uploader_name})
        rows = detail_df.rename(columns={
            '매장코드': 'store_code', '매장명': 'store_name', '코스': 'course', '제품코드': 'product_code',
            '제품명': 'product_name', '합계': 'qty', '환산기준': 'conv', '환산수': 'converted_qty'
        }).copy()
        rows['source_date'] = source_date
        rows['uploader_name'] = uploader_name
        rows['upload_key'] = upload_key
        rows['source_name'] = source_name
        rows = rows[['source_date','uploader_name','upload_key','source_name','store_code','store_name','course','product_code','product_name','qty','conv','converted_qty']]
        rows.to_sql('order_lines', eng, if_exists='append', index=False)
        return True, upload_key
    except Exception as e:
        return False, str(e)


def delete_upload(upload_key: str) -> bool:
    return exec_sql('DELETE FROM order_lines WHERE upload_key = :k', {'k': upload_key})


def build_store_snapshot(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df is None or detail_df.empty:
        return pd.DataFrame(columns=['매장코드','매장명','코스','신선육','북채','통날개','신선순살','오일','치킨무'])
    rows = []
    for (code, name, course), d in detail_df.groupby(['매장코드', '매장명', '코스']):
        rows.append({'매장코드': int(code), '매장명': str(name), '코스': str(course), '신선육': item_qty(d, FRESH_CODES), '북채': item_qty(d, [DRUM]), '통날개': item_qty(d, [WING]), '신선순살': item_qty(d, [BONELESS]), '오일': item_qty(d, [OIL]), '치킨무': item_qty(d, [RADISH])})
    return pd.DataFrame(rows)


def get_store_row(snapshot_df: pd.DataFrame, code: int, course: str):
    if snapshot_df is None or snapshot_df.empty:
        return None
    x = snapshot_df[(snapshot_df['매장코드'] == int(code)) & (snapshot_df['코스'] == str(course))]
    return None if x.empty else x.iloc[0]


def ai_expiry_comment(row: pd.Series) -> str:
    reasons = []
    if row['미발주항목']:
        reasons.append(f"직전 대비 {row['미발주항목']}")
    if row['감소항목']:
        reasons.append(f"감소 신호 {row['감소항목']}")
    if row['평균대비감소_신선육'] not in ['', '-']:
        reasons.append(f"신선육 평균 대비 {row['평균대비감소_신선육']}")
    joined = '; '.join(reasons) if reasons else '특이 저발주 신호는 크지 않습니다'
    if row['소비기한리스크'] == '즉시확인':
        return f'현재 상태: 즉시확인. 해석: {joined}. 방향: 전화 확인 후 기존 재고 사용 여부와 소비기한 관리 상태를 우선 점검하는 것이 적절합니다.'
    if row['소비기한리스크'] == '주의':
        return f'현재 상태: 주의. 해석: {joined}. 방향: 다음 발주 전 추적 관찰하고 필요 시 전화 확인을 권장합니다.'
    return '현재 상태: 정상. 방향: 현재 기준 유지하며 이상 변동 매장만 선별 점검하는 것이 적절합니다.'


def ai_store_direction(hist_df: pd.DataFrame) -> str:
    if hist_df is None or hist_df.empty:
        return '저장된 이력이 없어 방향 제시가 어렵습니다.'
    hist_df = hist_df.sort_values('발주일')
    last = hist_df.iloc[-1]
    msg = []
    if len(hist_df) >= 3:
        recent = hist_df.tail(3)
        if recent['신선육'].is_monotonic_decreasing:
            msg.append('최근 3회 신선육이 감소 추세입니다')
        if recent['오일'].is_monotonic_decreasing:
            msg.append('오일도 함께 감소 추세입니다')
    if float(last.get('신선육', 0) or 0) == 0:
        msg.append('최근 발주에 신선육이 없습니다')
    if float(last.get('북채', 0) or 0) == 0 or float(last.get('통날개', 0) or 0) == 0 or float(last.get('신선순살', 0) or 0) == 0:
        msg.append('일부 필수 원료육 누락이 있습니다')
    if not msg:
        return '발주 패턴은 전반적으로 안정적입니다. 방향: 현재 기준 유지하며 이상 변동만 점검하는 것이 적절합니다.'
    return f"현재 상태: {'; '.join(msg)}. 방향: 단기적으로 전화 확인을 우선하고, 반복되면 현장 점검 대상으로 관리하는 것이 적절합니다."


def build_analysis_summary(curr_detail: pd.DataFrame, prev1_detail=None, prev2_detail=None, prev3_detail=None) -> pd.DataFrame:
    prev1_snap = build_store_snapshot(prev1_detail)
    prev2_snap = build_store_snapshot(prev2_detail)
    prev3_snap = build_store_snapshot(prev3_detail)
    rows = []
    if curr_detail is None or curr_detail.empty:
        return pd.DataFrame()
    for (code, name, course), d in curr_detail.groupby(['매장코드', '매장명', '코스']):
        code = int(code); name = str(name); course = str(course)
        fresh = item_qty(d, FRESH_CODES)
        drum_q = item_qty(d, [DRUM])
        wing_q = item_qty(d, [WING])
        boneless_q = item_qty(d, [BONELESS])
        oil_q = item_qty(d, [OIL])
        radish_q = item_qty(d, [RADISH])
        total = fresh * 20 + wing_q * 7 + drum_q * 6 + boneless_q * 5
        need_oil = math.ceil(total / 75) if total > 0 else 0
        oil_status = '오일부족' if oil_q < need_oil else '정상'
        p1 = get_store_row(prev1_snap, code, course)
        p2 = get_store_row(prev2_snap, code, course)
        p3 = get_store_row(prev3_snap, code, course)
        prev1_fresh = float(p1['신선육']) if p1 is not None else 0.0
        prev1_drum = float(p1['북채']) if p1 is not None else 0.0
        prev1_wing = float(p1['통날개']) if p1 is not None else 0.0
        prev1_boneless = float(p1['신선순살']) if p1 is not None else 0.0
        prev1_oil = float(p1['오일']) if p1 is not None else 0.0
        avg_fresh = avg3([float(p1['신선육']) if p1 is not None else 0, float(p2['신선육']) if p2 is not None else 0, float(p3['신선육']) if p3 is not None else 0])
        avg_drum = avg3([float(p1['북채']) if p1 is not None else 0, float(p2['북채']) if p2 is not None else 0, float(p3['북채']) if p3 is not None else 0])
        avg_wing = avg3([float(p1['통날개']) if p1 is not None else 0, float(p2['통날개']) if p2 is not None else 0, float(p3['통날개']) if p3 is not None else 0])
        avg_boneless = avg3([float(p1['신선순살']) if p1 is not None else 0, float(p2['신선순살']) if p2 is not None else 0, float(p3['신선순살']) if p3 is not None else 0])
        avg_oil = avg3([float(p1['오일']) if p1 is not None else 0, float(p2['오일']) if p2 is not None else 0, float(p3['오일']) if p3 is not None else 0])
        missing, decreases, increases = [], [], []
        def comp(prev_val: float, curr_val: float, item_name: str):
            p = pct_change(prev_val, curr_val)
            if prev_val > 0 and curr_val == 0:
                missing.append(f'{item_name} 미발주')
            elif p is not None and p <= -30:
                decreases.append(f'{item_name} {abs(p):.1f}% 감소')
            elif p is not None and p >= 30:
                increases.append(f'{item_name} {p:.1f}% 증가')
        comp(prev1_fresh, fresh, '신선육'); comp(prev1_drum, drum_q, '북채'); comp(prev1_wing, wing_q, '통날개'); comp(prev1_boneless, boneless_q, '신선순살'); comp(prev1_oil, oil_q, '오일')
        avg_drop_fresh = pct_change(avg_fresh, fresh)
        avg_drop_text = pct_text(avg_drop_fresh)
        repeat_low = 0
        for v in [float(p1['신선육']) if p1 is not None else None, float(p2['신선육']) if p2 is not None else None, float(p3['신선육']) if p3 is not None else None]:
            if v is not None and v > 0 and fresh < v:
                repeat_low += 1
        if missing or (avg_drop_fresh is not None and avg_drop_fresh <= -50) or repeat_low >= 2:
            expiry_risk = '즉시확인'
        elif (avg_drop_fresh is not None and avg_drop_fresh <= -30) or oil_status == '오일부족' or decreases:
            expiry_risk = '주의'
        else:
            expiry_risk = '정상'
        reasons = []
        if missing: reasons.append(', '.join(missing))
        if decreases: reasons.append(', '.join(decreases))
        if oil_status == '오일부족': reasons.append('오일부족')
        total_comment = ' / '.join(reasons) if reasons else '정상'
        rows.append([code, name, course, total, oil_q, need_oil, oil_status, fresh, drum_q, wing_q, boneless_q, radish_q, prev1_fresh, prev1_drum, prev1_wing, prev1_boneless, prev1_oil, round(avg_fresh,2), round(avg_drum,2), round(avg_wing,2), round(avg_boneless,2), round(avg_oil,2), avg_drop_text, ', '.join(missing), ', '.join(decreases), ', '.join(increases), expiry_risk, total_comment])
    summary_df = pd.DataFrame(rows, columns=['매장코드','매장명','코스','총환산수','오일','필요오일','오일판정','신선육','북채','통날개','신선순살','치킨무(22000237)','전발주_신선육','전발주_북채','전발주_통날개','전발주_신선순살','전발주_오일','최근3회평균_신선육','최근3회평균_북채','최근3회평균_통날개','최근3회평균_신선순살','최근3회평균_오일','평균대비감소_신선육','미발주항목','감소항목','증가항목','소비기한리스크','총평'])
    summary_df['AI코멘트'] = summary_df.apply(ai_expiry_comment, axis=1)
    return summary_df


def get_assignment_df() -> pd.DataFrame:
    df = q('SELECT store_code AS 매장코드, store_name AS 매장명, bm_name AS BM FROM store_assignments ORDER BY bm_name, store_name')
    if df.empty:
        return pd.DataFrame(columns=['매장코드', '매장명', 'BM'])
    for col in ['매장코드', '매장명', 'BM']:
        if col not in df.columns:
            df[col] = pd.Series(dtype='object')
    return df[['매장코드', '매장명', 'BM']]


def assign_store(store_code: int, store_name: str, bm_name: str) -> bool:
    exec_sql('DELETE FROM store_assignments WHERE store_code = :c', {'c': int(store_code)})
    eng, _, _ = get_db_status()
    if eng is None:
        return False
    row = pd.DataFrame([{'store_code': int(store_code), 'store_name': store_name, 'bm_name': bm_name, 'updated_at': pd.Timestamp.now().isoformat()}])
    row.to_sql('store_assignments', eng, if_exists='append', index=False)
    return True


def team_ai_comment(team_df: pd.DataFrame) -> str:
    total = len(team_df)
    urgent = int((team_df['소비기한리스크'] == '즉시확인').sum()) if '소비기한리스크' in team_df.columns else 0
    caution = int((team_df['소비기한리스크'] == '주의').sum()) if '소비기한리스크' in team_df.columns else 0
    if total == 0: return '분석 데이터가 없습니다.'
    if urgent >= max(3, total * 0.2): return '운영2팀 전체 기준 즉시확인 매장 비중이 높습니다. 저발주·미발주 매장을 우선 확인하는 방향이 적절합니다.'
    if caution >= max(5, total * 0.3): return '운영2팀 전체 기준 주의 매장이 다수 확인됩니다. 급감 패턴 중심으로 선별 관리가 필요합니다.'
    return '운영2팀 전체 기준 발주 패턴은 비교적 안정적입니다. 이상 변동 매장만 선별 점검하는 방향이 적절합니다.'


def current_user() -> str:
    default_user = st.session_state.get('user_name', '')
    user = st.sidebar.text_input('사용자 이름', value=default_user)
    if st.sidebar.button('이름 저장'):
        st.session_state['user_name'] = user
        st.sidebar.success('이름 저장 완료')
    return user


def to_excel_bytes(summary_df: pd.DataFrame, curr_detail: pd.DataFrame, prev1_detail=None, prev2_detail=None, prev3_detail=None) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        summary_df.to_excel(writer, index=False, sheet_name='요약')
        curr_detail.to_excel(writer, index=False, sheet_name='품목근거_이번발주')
        if prev1_detail is not None and not prev1_detail.empty: prev1_detail.to_excel(writer, index=False, sheet_name='품목근거_전발주1')
        if prev2_detail is not None and not prev2_detail.empty: prev2_detail.to_excel(writer, index=False, sheet_name='품목근거_전발주2')
        if prev3_detail is not None and not prev3_detail.empty: prev3_detail.to_excel(writer, index=False, sheet_name='품목근거_전발주3')
    output.seek(0)
    return output.getvalue()


db_ok, db_msg = init_db()
eng, db_mode, db_err = get_db_status()

st.title('BBQ 발주 분석 V6')
st.caption('업로더 기준 저장 / 운영2팀 총합 / BM별 조회 / Supabase 안정화 최종본')

if db_ok:
    if db_mode == 'external': st.success('외부 DB 연결 정상')
    elif db_mode == 'sqlite': st.warning('외부 DB 연결 실패로 로컬 sqlite 모드로 실행 중입니다.')
else:
    st.error(f'DB 초기화 실패: {db_msg}')
if db_err: st.caption(f'DB 참고: {db_err}')

user_name = current_user()
menu = st.sidebar.radio('메뉴', ['운영2팀 총합본', '오늘 발주 점검', '소비기한 리스크', '매장별 이력조회', '담당 매장 관리', 'DB 관리'])
uploads = latest_uploads()
dates = latest_dates()

if menu == '오늘 발주 점검':
    st.subheader('오늘 발주 점검')
    if not user_name:
        st.warning('좌측에서 사용자 이름을 먼저 입력하세요.')
    else:
        uploaded = st.file_uploader('이번 발주 파일 업로드', type=['xlsx', 'xls'], key='upload_today')
        if uploaded is not None:
            guessed_date = parse_date_from_name(uploaded.name)
            default_date = guessed_date if guessed_date else str(pd.Timestamp.today().date())
            source_date = st.text_input('발주일(YYYY-MM-DD)', value=default_date)
            save_flag = st.checkbox('업로드 시 공용 DB에 저장', value=True)
            if st.button('분석 실행', type='primary', use_container_width=True):
                curr_detail = normalize_order_df(uploaded)
                prev_keys = latest_upload_keys_by_user(user_name, source_date, limit=3)
                prev1_detail = load_detail_by_upload_key(prev_keys[0]) if len(prev_keys) > 0 else None
                prev2_detail = load_detail_by_upload_key(prev_keys[1]) if len(prev_keys) > 1 else None
                prev3_detail = load_detail_by_upload_key(prev_keys[2]) if len(prev_keys) > 2 else None
                summary_df = build_analysis_summary(curr_detail, prev1_detail, prev2_detail, prev3_detail)
                assign_df = get_assignment_df()
                if not assign_df.empty:
                    summary_df = summary_df.merge(assign_df[['매장코드', 'BM']], on='매장코드', how='left')
                    summary_df['BM'] = summary_df['BM'].fillna('미지정')
                else:
                    summary_df['BM'] = '미지정'
                st.dataframe(summary_df, use_container_width=True, height=650)
                if save_flag:
                    ok, msg = save_order_history(source_date, user_name, uploaded.name, curr_detail)
                    if ok: st.success(f'저장 완료: {msg}')
                    else: st.error(f'저장 실패: {msg}')
                excel_bytes = to_excel_bytes(summary_df, curr_detail, prev1_detail, prev2_detail, prev3_detail)
                st.download_button('엑셀 다운로드', data=excel_bytes, file_name=f"발주점검_{source_date.replace('-', '')}_{user_name}.xlsx", mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', use_container_width=True)

elif menu == '소비기한 리스크':
    st.subheader('소비기한 리스크')
    if not uploads:
        st.info('저장된 업로드가 없습니다.')
    else:
        upload_options = {f"{x['source_date']} / {x['uploader_name']}": x['upload_key'] for x in uploads}
        selected_label = st.selectbox('업로드 선택', list(upload_options.keys()))
        selected_key = upload_options[selected_label]
        curr_detail = load_detail_by_upload_key(selected_key)
        uploader_name = selected_label.split(' / ')[1]
        source_date = selected_label.split(' / ')[0]
        prev_keys = latest_upload_keys_by_user(uploader_name, source_date, limit=3)
        prev1_detail = load_detail_by_upload_key(prev_keys[0]) if len(prev_keys) > 0 else None
        prev2_detail = load_detail_by_upload_key(prev_keys[1]) if len(prev_keys) > 1 else None
        prev3_detail = load_detail_by_upload_key(prev_keys[2]) if len(prev_keys) > 2 else None
        summary_df = build_analysis_summary(curr_detail, prev1_detail, prev2_detail, prev3_detail)
        assign_df = get_assignment_df()
        if not assign_df.empty:
            summary_df = summary_df.merge(assign_df[['매장코드', 'BM']], on='매장코드', how='left')
            summary_df['BM'] = summary_df['BM'].fillna('미지정')
        else:
            summary_df['BM'] = '미지정'
        st.dataframe(summary_df[['매장명','코스','BM','신선육','전발주_신선육','최근3회평균_신선육','평균대비감소_신선육','미발주항목','감소항목','소비기한리스크','AI코멘트']], use_container_width=True, height=700)

elif menu == '매장별 이력조회':
    st.subheader('매장별 이력조회')
    if not dates:
        st.info('저장된 발주 데이터가 없습니다.')
    else:
        stores = q('SELECT DISTINCT store_name AS 매장명 FROM order_lines ORDER BY store_name')
        store_list = stores['매장명'].tolist() if not stores.empty else []
        if store_list:
            selected_store = st.selectbox('매장 선택', store_list)
            hist = q('''
                SELECT source_date AS 발주일, uploader_name AS 업로더, store_name AS 매장명,
                SUM(CASE WHEN product_code IN (22000000,22000002,22000007) THEN qty ELSE 0 END) AS 신선육,
                SUM(CASE WHEN product_code = 22000013 THEN qty ELSE 0 END) AS 북채,
                SUM(CASE WHEN product_code = 22000014 THEN qty ELSE 0 END) AS 통날개,
                SUM(CASE WHEN product_code = 22000010 THEN qty ELSE 0 END) AS 신선순살,
                SUM(CASE WHEN product_code = 13000002 THEN qty ELSE 0 END) AS 오일
                FROM order_lines WHERE store_name = :s
                GROUP BY source_date, uploader_name, store_name
                ORDER BY source_date, uploader_name
            ''', {'s': selected_store})
            st.dataframe(hist, use_container_width=True, height=550)
            hist_for_ai = hist.groupby('발주일', as_index=False)[['신선육','북채','통날개','신선순살','오일']].sum()
            st.info(ai_store_direction(hist_for_ai))
        else:
            st.info('조회 가능한 매장이 없습니다.')

elif menu == '운영2팀 총합본':
    st.subheader('운영2팀 총합본')
    if not dates:
        st.info('저장된 발주 데이터가 없습니다.')
    else:
        selected_date = st.selectbox('발주일 선택', dates, key='team_date')
        curr_detail = load_detail_by_date_all(selected_date)
        prev_dates = [d for d in dates if d < selected_date][:3]
        prev1_detail = load_detail_by_date_all(prev_dates[0]) if len(prev_dates) > 0 else None
        prev2_detail = load_detail_by_date_all(prev_dates[1]) if len(prev_dates) > 1 else None
        prev3_detail = load_detail_by_date_all(prev_dates[2]) if len(prev_dates) > 2 else None
        summary_df = build_analysis_summary(curr_detail, prev1_detail, prev2_detail, prev3_detail)
        assign_df = get_assignment_df()
        if not assign_df.empty and '매장코드' in assign_df.columns and 'BM' in assign_df.columns:
            summary_df = summary_df.merge(assign_df[['매장코드', 'BM']], on='매장코드', how='left')
            summary_df['BM'] = summary_df['BM'].fillna('미지정')
        else:
            summary_df['BM'] = '미지정'
        uploads_that_day = q('SELECT DISTINCT source_date, uploader_name, upload_key FROM order_lines WHERE source_date = :d ORDER BY uploader_name', {'d': selected_date})
        c1, c2, c3, c4 = st.columns(4)
        c1.metric('전체 매장 수', len(summary_df))
        c2.metric('즉시확인', int((summary_df['소비기한리스크'] == '즉시확인').sum()))
        c3.metric('주의', int((summary_df['소비기한리스크'] == '주의').sum()))
        c4.metric('정상', int((summary_df['소비기한리스크'] == '정상').sum()))
        st.info(team_ai_comment(summary_df))
        st.markdown('#### 당일 업로더 목록')
        st.dataframe(uploads_that_day, use_container_width=True, height=180)
        bm_options_total = ['전체'] + sorted(summary_df['BM'].dropna().unique().tolist())
        selected_bm_total = st.selectbox('BM 선택', bm_options_total, key='total_bm_filter')
        filtered_summary = summary_df.copy() if selected_bm_total == '전체' else summary_df[summary_df['BM'] == selected_bm_total].copy()
        bm_summary = summary_df.groupby('BM').agg(매장수=('매장코드', 'count'), 즉시확인=('소비기한리스크', lambda s: int((s == '즉시확인').sum())), 주의=('소비기한리스크', lambda s: int((s == '주의').sum())), 정상=('소비기한리스크', lambda s: int((s == '정상').sum())), 미발주=('미발주항목', lambda s: int((s != '').sum())), 급감=('감소항목', lambda s: int((s != '').sum()))).reset_index().sort_values(['즉시확인', '주의', '매장수'], ascending=[False, False, False])
        st.markdown('#### BM별 현황')
        st.dataframe(bm_summary, use_container_width=True, height=400)
        st.markdown('#### 우선 관리 매장')
        urgent_df = filtered_summary[filtered_summary['소비기한리스크'].isin(['즉시확인', '주의'])].copy()
        st.dataframe(urgent_df[['BM','매장명','코스','미발주항목','감소항목','오일판정','소비기한리스크','총평']].head(100), use_container_width=True, height=500)
        st.markdown('#### 선택 BM 상세 전체')
        st.dataframe(filtered_summary, use_container_width=True, height=450)

elif menu == '담당 매장 관리':
    st.subheader('담당 매장 관리')
    if not user_name:
        st.warning('좌측에서 사용자 이름을 먼저 입력하세요.')
    elif not dates:
        st.info('저장된 발주 데이터가 없습니다.')
    else:
        latest_date = dates[0]
        latest_detail = load_detail_by_date_all(latest_date)
        latest_stores = build_store_snapshot(latest_detail)[['매장코드','매장명']].drop_duplicates().sort_values('매장명')
        current_map = get_assignment_df()
        assigned_codes = current_map[current_map['BM'] == user_name]['매장코드'].tolist() if not current_map.empty else []
        selected_store_codes = st.multiselect(f'{user_name} 담당 매장 선택', options=latest_stores['매장코드'].tolist(), default=assigned_codes, format_func=lambda x: f"{int(x)} / {latest_stores.loc[latest_stores['매장코드'] == x, '매장명'].iloc[0]}")
        if st.button('내 담당 매장 저장', type='primary'):
            exec_sql('DELETE FROM store_assignments WHERE bm_name = :bm', {'bm': user_name})
            ok = True
            for code in selected_store_codes:
                store_name = latest_stores.loc[latest_stores['매장코드'] == code, '매장명'].iloc[0]
                ok = assign_store(code, store_name, user_name) and ok
            if ok: st.success('담당 매장 저장 완료')
            else: st.error('담당 매장 저장 실패')
        st.markdown('#### 현재 담당 매장')
        st.dataframe(get_assignment_df(), use_container_width=True, height=500)

elif menu == 'DB 관리':
    st.subheader('DB 관리')
    st.write(f'저장된 업로드 수: {len(uploads)}')
    st.write(f'저장된 발주일 수: {len(dates)}')
    if uploads:
        options = {f"{x['source_date']} / {x['uploader_name']}": x['upload_key'] for x in uploads}
        delete_label = st.selectbox('삭제할 업로드 선택', list(options.keys()))
        delete_key = options[delete_label]
        if st.button('선택 업로드 삭제'):
            ok = delete_upload(delete_key)
            if ok:
                st.success(f'{delete_label} 삭제 완료')
                st.rerun()
            else:
                st.error('삭제 실패')

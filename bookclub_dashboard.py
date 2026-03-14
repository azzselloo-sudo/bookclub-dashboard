import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, date, timedelta
from collections import defaultdict
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric, RunRealtimeReportRequest
from google.oauth2 import service_account
import googleapiclient.discovery
import time
import requests

# ── 설정 ──────────────────────────────────────────────
st.set_page_config(
    page_title="북클럽 성과 대시보드",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

PROPERTY_ID = '523292335'
KEY_FILE    = 'C:/Users/ozcre/Downloads/loorou-bookclub-104e72493276.json'
SHEET_ID    = '1isk4R8YiWjH2AXKzPN8QjctU5XAVMicIkJzQznYE4JE'

SUPABASE_URL      = 'https://hegohmcxglujatsnlmtj.supabase.co'
SUPABASE_ANON_KEY = 'sb_publishable_feAjcEINhj0VQVFhgGNQYQ_Vn8nIBxP'

SCOPES = [
    'https://www.googleapis.com/auth/analytics.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly',
]

def _get_credentials():
    """로컬: JSON 파일 / Streamlit Cloud: st.secrets"""
    try:
        info = dict(st.secrets['gcp_service_account'])
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    except Exception:
        return service_account.Credentials.from_service_account_file(KEY_FILE, scopes=SCOPES)

# ── GA4 연결 ──────────────────────────────────────────
@st.cache_resource
def get_client():
    return BetaAnalyticsDataClient(credentials=_get_credentials())

@st.cache_resource
def get_sheets():
    return googleapiclient.discovery.build('sheets', 'v4', credentials=_get_credentials())

# ── 구글 시트 구독자 데이터 로드 ──────────────────────
@st.cache_data(ttl=300)
def load_subscribers():
    svc = get_sheets()

    BOOK_CONFIGS = [
        ('타프티',      '타프티_구독자',      'A', 0),   # col0=timestamp
        ('매력자본',    '매력자본_구독자',    'A', 0),
        ('유혹의기술',  '유혹의기술_구독자',  'A', 0),
    ]

    # 유혹의기술 첫날 (timestamp 없는 행에 배정할 날짜)
    Y_FIRST = date(2026, 3, 7)

    def parse_ts(s):
        s = s.strip()
        for fmt in [
            '%Y. %m. %d %p %I:%M:%S',
            '%Y. %m. %d %오전 %I:%M:%S',
            '%Y. %m. %d %오후 %I:%M:%S',
        ]:
            try: return datetime.strptime(s, fmt).date()
            except: pass
        # 한국어 오전/오후 처리
        try:
            s2 = s.replace('오전','AM').replace('오후','PM')
            return datetime.strptime(s2, '%Y. %m. %d %p %I:%M:%S').date()
        except:
            return None

    result = {}
    for name, sheet_name, _, ts_col in BOOK_CONFIGS:
        res  = svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f'{sheet_name}!A:E').execute()
        rows = res.get('values', [])[1:]   # 헤더 제외

        dates = []
        first_date = None
        for row in rows:
            if len(row) <= ts_col or not row[ts_col]:
                continue
            d = parse_ts(row[ts_col])
            if d:
                if first_date is None: first_date = d
                dates.append(d)
            else:
                # timestamp 없는 행 → 첫날로
                if first_date: dates.append(first_date)
                elif name == '유혹의기술': dates.append(Y_FIRST)

        result[name] = sorted(dates)

    return result

def build_cumulative(dates, abs_start, abs_end):
    """절대 날짜 기준 누적 시리즈 (변동 없는 날도 포함)"""
    from collections import Counter
    dm = Counter(dates)
    cum, series = 0, []
    cur = abs_start
    while cur <= abs_end:
        cum += dm.get(cur, 0)
        series.append(cum)
        cur += timedelta(days=1)
    return series

def build_relative(dates, max_day):
    """D+N 기준 누적 (데이터 끝난 후 None)"""
    from collections import Counter
    if not dates: return [None]*max_day
    dm = Counter(dates)
    start = min(dates)
    last  = max(dates)
    last_day = (last - start).days + 1
    cum, series = 0, []
    for day in range(1, max_day+1):
        target = start + timedelta(days=day-1)
        cum += dm.get(target, 0)
        series.append(cum if day <= last_day else None)
    return series

def run(dims, metrics, start='2026-01-01', end='today', limit=500):
    client = get_client()
    req = RunReportRequest(
        property=f'properties/{PROPERTY_ID}',
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimensions=[Dimension(name=d) for d in dims],
        metrics=[Metric(name=m) for m in metrics],
        limit=limit
    )
    return client.run_report(req)

def get_realtime():
    client = get_client()
    req = RunRealtimeReportRequest(
        property=f'properties/{PROPERTY_ID}',
        dimensions=[Dimension(name='unifiedScreenName')],
        metrics=[Metric(name='activeUsers')]
    )
    return client.run_realtime_report(req)

# ── 데이터 로드 (캐시 5분) ────────────────────────────
@st.cache_data(ttl=300)
def load_all_data():
    # 날짜별 방문
    r1 = run(['date'], ['sessions','screenPageViews','activeUsers','averageSessionDuration','newUsers'])
    daily = []
    for row in sorted(r1.rows, key=lambda x: x.dimension_values[0].value):
        d = row.dimension_values[0].value
        v = [x.value for x in row.metric_values]
        daily.append({'date': d, 'sessions': int(v[0]), 'pageviews': int(v[1]),
                      'users': int(v[2]), 'avg_dur': float(v[3]), 'new_users': int(v[4])})

    # 시리즈 × 챕터별 클릭 (series 커스텀 측정기준 포함)
    r2 = run(['customEvent:series', 'customEvent:chapter'], ['eventCount'], limit=300)
    chap = []
    chap_by_series = defaultdict(lambda: defaultdict(int))  # series -> chapter -> count
    SERIES_NAME = {'tafti': '타프티', 'charm': '매력자본', 'seduction': '유혹의기술'}
    for row in sorted(r2.rows, key=lambda x: -int(x.metric_values[0].value)):
        s  = row.dimension_values[0].value
        ch = row.dimension_values[1].value
        cnt = int(row.metric_values[0].value)
        if ch and ch != '(not set)' and ch != 'all':
            s_name = SERIES_NAME.get(s, s) if s and s != '(not set)' else '미분류'
            label  = f'{s_name} · {ch}챕터'
            chap.append({'chapter': label, 'series': s_name, 'clicks': cnt})
            chap_by_series[s_name][f'{ch}챕터'] += cnt

    # 날짜별 챕터 클릭
    r3 = run(['date', 'customEvent:series', 'customEvent:chapter'], ['eventCount'], limit=500)
    chap_daily = defaultdict(lambda: defaultdict(int))
    for row in r3.rows:
        dt  = row.dimension_values[0].value
        s   = row.dimension_values[1].value
        ch  = row.dimension_values[2].value
        cnt = int(row.metric_values[0].value)
        if ch and ch != '(not set)' and ch != 'all':
            s_name = SERIES_NAME.get(s, s) if s and s != '(not set)' else '미분류'
            chap_daily[dt][f'{s_name}·{ch}챕터'] += cnt

    # 이벤트별 날짜별 (series 포함)
    r4 = run(['eventName', 'date', 'customEvent:series', 'customEvent:chapter'], ['eventCount'], limit=1000)
    like_by_date = defaultdict(int)
    comment_by_date = defaultdict(int)
    csub_by_date = defaultdict(int)
    reply_by_date = defaultdict(int)   # feedback_submit (대댓글/의견)
    series_by_date = defaultdict(int)
    chap_click_by_date = defaultdict(int)
    like_by_chap = defaultdict(int)
    comment_by_chap = defaultdict(int)
    for row in r4.rows:
        evt  = row.dimension_values[0].value
        dt   = row.dimension_values[1].value
        s    = row.dimension_values[2].value
        ch   = row.dimension_values[3].value
        cnt  = int(row.metric_values[0].value)
        s_name = SERIES_NAME.get(s, s) if s and s != '(not set)' else '미분류'
        if evt == 'like_click':
            like_by_date[dt] += cnt
            if ch and ch != '(not set)': like_by_chap[f'{s_name} · {ch}챕터'] += cnt
        elif evt == 'comment_toggle':
            comment_by_date[dt] += cnt
            if ch and ch != '(not set)': comment_by_chap[f'{s_name} · {ch}챕터'] += cnt
        elif evt == 'series_click':     series_by_date[dt] += cnt
        elif evt == 'chapter_click':    chap_click_by_date[dt] += cnt

    # ── Supabase에서 댓글/의견 직접 집계 (GA4보다 정확) ──────────
    sb_headers = {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
    }
    # feedbacks = 상단 의견 남기기 폼 → 사용자 기준 "댓글"
    try:
        r_feedbacks = requests.get(
            f'{SUPABASE_URL}/rest/v1/feedbacks?select=created_at&limit=10000',
            headers=sb_headers, timeout=10
        )
        if r_feedbacks.ok:
            for row in r_feedbacks.json():
                dt = row['created_at'][:10].replace('-', '')
                csub_by_date[dt] += 1
    except Exception:
        pass
    # review_comments = 리뷰 카드에 다는 댓글 → 사용자 기준 "대댓글"
    try:
        r_comments = requests.get(
            f'{SUPABASE_URL}/rest/v1/review_comments?select=created_at&limit=10000',
            headers=sb_headers, timeout=10
        )
        if r_comments.ok:
            for row in r_comments.json():
                dt = row['created_at'][:10].replace('-', '')
                reply_by_date[dt] += 1
    except Exception:
        pass

    # 신규 vs 재방문
    r5 = run(['date', 'newVsReturning'], ['sessions', 'averageSessionDuration'])
    new_by_date = defaultdict(int)
    ret_by_date = defaultdict(int)
    new_dur = defaultdict(float)
    ret_dur = defaultdict(float)
    for row in r5.rows:
        dt  = row.dimension_values[0].value
        typ = row.dimension_values[1].value
        sess = int(row.metric_values[0].value)
        dur  = float(row.metric_values[1].value)
        if 'new' in typ.lower():
            new_by_date[dt] += sess
            new_dur[dt] = dur
        elif 'return' in typ.lower():
            ret_by_date[dt] += sess
            ret_dur[dt] = dur

    return {
        'daily': daily,
        'chap': chap,
        'chap_daily': dict(chap_daily),
        'like_by_date': dict(like_by_date),
        'comment_by_date': dict(comment_by_date),
        'csub_by_date': dict(csub_by_date),
        'reply_by_date': dict(reply_by_date),
        'series_by_date': dict(series_by_date),
        'chap_click_by_date': dict(chap_click_by_date),
        'like_by_chap': dict(like_by_chap),
        'comment_by_chap': dict(comment_by_chap),
        'new_by_date': dict(new_by_date),
        'ret_by_date': dict(ret_by_date),
        'new_dur': dict(new_dur),
        'ret_dur': dict(ret_dur),
    }

# ── 날짜 범위 전체 생성 ───────────────────────────────
def full_date_range(start='20260127', end=None):
    if end is None:
        end = date.today().strftime('%Y%m%d')
    s = datetime.strptime(start, '%Y%m%d').date()
    e = datetime.strptime(end, '%Y%m%d').date()
    dates = []
    cur = s
    while cur <= e:
        dates.append(cur.strftime('%Y%m%d'))
        cur += timedelta(days=1)
    return dates

COLORS = {'타프티': '#2E75B6', '매력자본': '#70AD47', '유혹의기술': '#FFC000'}

# ────────────────────────────────────────────────────
# UI 시작
# ────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: #f8f9fa; border-radius: 10px;
        padding: 16px 20px; margin: 4px;
        border-left: 4px solid #2E75B6;
    }
    .metric-val { font-size: 2rem; font-weight: 700; color: #1F4E79; }
    .metric-lbl { font-size: 0.85rem; color: #666; margin-top: 2px; }
    .section-title { font-size: 1.2rem; font-weight: 700; color: #1F4E79;
                     border-bottom: 2px solid #2E75B6; padding-bottom: 6px; margin: 20px 0 12px; }
</style>
""", unsafe_allow_html=True)

# ── 사이드바 ──────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📚 북클럽 대시보드")
    st.markdown("---")

    page = st.radio("페이지 선택", [
        "📊 전체 개요",
        "👥 구독자 추이",
        "📖 챕터 참여도",
        "❤️ 좋아요 & 댓글",
        "🔁 리텐션",
        "🔴 실시간"
    ])

    st.markdown("---")
    start_date = st.date_input("시작일", value=date(2026, 1, 27))
    end_date   = st.date_input("종료일", value=date.today())

    if st.button("🔄 데이터 새로고침"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.caption(f"마지막 업데이트: {datetime.now().strftime('%H:%M:%S')}")
    st.caption("5분마다 자동 갱신")

# ── 데이터 로드 ───────────────────────────────────────
with st.spinner("GA4 & 구글시트 데이터 불러오는 중..."):
    data = load_all_data()
    subs = load_subscribers()

daily        = data['daily']
all_dates    = full_date_range()
date_map     = {d['date']: d for d in daily}

# 날짜 필터
sd = start_date.strftime('%Y%m%d')
ed = end_date.strftime('%Y%m%d')
filtered_dates = [dt for dt in all_dates if sd <= dt <= ed]

# ────────────────────────────────────────────────────
# 페이지1: 전체 개요
# ────────────────────────────────────────────────────
if page == "📊 전체 개요":
    st.title("📊 전체 개요")

    # KPI 카드
    total_sess = sum(date_map.get(d, {}).get('sessions', 0) for d in filtered_dates)
    total_users = sum(date_map.get(d, {}).get('users', 0) for d in filtered_dates)
    total_pv   = sum(date_map.get(d, {}).get('pageviews', 0) for d in filtered_dates)
    w_dur = sum(date_map.get(d, {}).get('avg_dur', 0) * date_map.get(d, {}).get('sessions', 0)
                for d in filtered_dates)
    avg_dur = w_dur / total_sess if total_sess else 0
    total_like = sum(data['like_by_date'].get(d, 0) for d in filtered_dates)
    total_comment = sum(data['csub_by_date'].get(d, 0) + data['reply_by_date'].get(d, 0) for d in filtered_dates)

    total_subs = sum(len(v) for v in subs.values())

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    for col, val, lbl, color in [
        (c1, total_subs,  "총 구독자 👥", "#1F4E79"),
        (c2, total_sess,  "총 세션", "#2E75B6"),
        (c3, total_users, "활성 사용자", "#70AD47"),
        (c4, total_pv,    "페이지뷰", "#FFC000"),
        (c5, f"{avg_dur/60:.1f}분", f"평균 체류시간 ({avg_dur:.0f}초)", "#7030A0"),
        (c6, total_like,  "좋아요 ❤️", "#C00000"),
        (c7, total_comment, "댓글+대댓글 💬", "#375623"),
    ]:
        col.markdown(f"""
        <div class="metric-card" style="border-left-color:{color}">
            <div class="metric-val">{val}</div>
            <div class="metric-lbl">{lbl}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">일자별 세션 & 페이지뷰</div>', unsafe_allow_html=True)

    # 구간 색상
    def get_period(dt):
        if dt >= '20260307': return '유혹의기술'
        elif dt >= '20260214': return '매력자본'
        else: return '타프티'

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    xs = [dt[4:6]+'/'+dt[6:] for dt in filtered_dates]
    sess_vals = [date_map.get(d, {}).get('sessions', 0) for d in filtered_dates]
    pv_vals   = [date_map.get(d, {}).get('pageviews', 0) for d in filtered_dates]
    dur_vals  = [date_map.get(d, {}).get('avg_dur', 0) for d in filtered_dates]
    bar_colors = [COLORS[get_period(d)] for d in filtered_dates]

    fig.add_trace(go.Bar(x=xs, y=sess_vals, name='세션', marker_color=bar_colors,
                         opacity=0.8), secondary_y=False)
    fig.add_trace(go.Scatter(x=xs, y=dur_vals, name='평균체류(초)', mode='lines+markers',
                             line=dict(color='#7030A0', width=3), marker=dict(size=6)),
                  secondary_y=True)

    # 북클럽 구간 배경 표시
    periods = [('2026-01-27', '2026-02-13', '타프티', 'rgba(46,117,182,0.07)'),
               ('2026-02-14', '2026-03-06', '매력자본', 'rgba(112,173,71,0.07)'),
               ('2026-03-07', '2026-03-13', '유혹의기술', 'rgba(255,192,0,0.12)')]
    for ps, pe, pn, pc in periods:
        fig.add_vrect(x0=ps[5:].replace('-','/'), x1=pe[5:].replace('-','/'),
                      fillcolor=pc, line_width=0,
                      annotation_text=pn, annotation_position="top left",
                      annotation_font_size=11)

    fig.update_layout(height=360, plot_bgcolor='white', paper_bgcolor='white',
                      legend=dict(orientation='h', y=-0.15),
                      xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
                      margin=dict(l=40, r=40, t=20, b=60))
    fig.update_yaxes(title_text="세션 수", secondary_y=False, gridcolor='#eee')
    fig.update_yaxes(title_text="평균 체류시간 (초)", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    # 북클럽별 요약 테이블
    st.markdown('<div class="section-title">북클럽별 성과 비교</div>', unsafe_allow_html=True)
    p_data = []
    for pn, ps, pe in [('타프티','20260127','20260213'),
                        ('매력자본','20260214','20260306'),
                        ('유혹의기술','20260307','20261231')]:
        pd_dates = [d for d in all_dates if ps <= d <= pe]
        s  = sum(date_map.get(d,{}).get('sessions',0) for d in pd_dates)
        u  = sum(date_map.get(d,{}).get('users',0) for d in pd_dates)
        pv = sum(date_map.get(d,{}).get('pageviews',0) for d in pd_dates)
        wd = sum(date_map.get(d,{}).get('avg_dur',0)*date_map.get(d,{}).get('sessions',0) for d in pd_dates)
        ad = wd/s if s else 0
        lk = sum(data['like_by_date'].get(d,0) for d in pd_dates)
        cm = sum(data['csub_by_date'].get(d,0) for d in pd_dates)
        days = len([d for d in pd_dates if date_map.get(d,{}).get('sessions',0)>0])
        p_data.append({'북클럽': pn, '총세션': s, '사용자': u, '페이지뷰': pv,
                        '평균체류': f'{ad:.0f}초', '일평균세션': f'{s/max(len(pd_dates),1):.1f}',
                        '좋아요': lk, '댓글제출': cm})
    st.dataframe(pd.DataFrame(p_data), use_container_width=True, hide_index=True)

# ────────────────────────────────────────────────────
# 페이지2: 구독자 추이
# ────────────────────────────────────────────────────
elif page == "👥 구독자 추이":
    st.title("👥 구독자 추이")

    ABS_START = date(2026, 1, 27)
    ABS_END   = date(2026, 3, 13)
    n_days    = (ABS_END - ABS_START).days + 1
    xs_abs    = [(ABS_START + timedelta(days=i)).strftime('%m/%d') for i in range(n_days)]

    BOOK_META = {
        '타프티':     {'color': '#2E75B6', 'start': date(2026,1,27)},
        '매력자본':   {'color': '#70AD47', 'start': date(2026,2,14)},
        '유혹의기술': {'color': '#FFC000', 'start': date(2026,3,7)},
    }

    # ── KPI ──
    c1, c2, c3, c4 = st.columns(4)
    for col, name in zip([c1,c2,c3], ['타프티','매력자본','유혹의기술']):
        cnt = len(subs.get(name, []))
        clr = BOOK_META[name]['color']
        col.markdown(f"""
        <div class="metric-card" style="border-left-color:{clr}">
            <div class="metric-val">{cnt}명</div>
            <div class="metric-lbl">{name} 구독자</div>
        </div>""", unsafe_allow_html=True)
    total_subs2 = sum(len(v) for v in subs.values())
    c4.markdown(f"""
    <div class="metric-card" style="border-left-color:#1F4E79">
        <div class="metric-val">{total_subs2}명</div>
        <div class="metric-lbl">전체 누적 구독자</div>
    </div>""", unsafe_allow_html=True)

    # ── 탭: 절대일 / 상대일 ──
    tab1, tab2 = st.tabs(["📅 실제 날짜 기준", "📈 D+N 상대일 기준"])

    with tab1:
        st.markdown('<div class="section-title">전체 날짜 기준 누적 구독자 (1/27 ~ 3/13)</div>', unsafe_allow_html=True)

        fig_abs = go.Figure()

        # 북클럽별 개별 선
        for name, meta in BOOK_META.items():
            series = build_cumulative(subs.get(name,[]), ABS_START, ABS_END)
            # 시작 전은 0, 데이터 없으면 0
            fig_abs.add_trace(go.Scatter(
                x=xs_abs, y=series, name=name, mode='lines',
                line=dict(color=meta['color'], width=3),
                hovertemplate='%{x}<br>'+name+': %{y}명<extra></extra>'
            ))

        # 전체 합산
        total_series = []
        t_cum = m_cum = y_cum = 0
        from collections import Counter
        t_dm = Counter(subs.get('타프티',[]))
        m_dm = Counter(subs.get('매력자본',[]))
        y_dm = Counter(subs.get('유혹의기술',[]))
        cur = ABS_START
        while cur <= ABS_END:
            t_cum += t_dm.get(cur, 0)
            m_cum += m_dm.get(cur, 0)
            y_cum += y_dm.get(cur, 0)
            total_series.append(t_cum + m_cum + y_cum)
            cur += timedelta(days=1)

        fig_abs.add_trace(go.Scatter(
            x=xs_abs, y=total_series, name='전체 합산',
            mode='lines', line=dict(color='#7030A0', width=3, dash='dash'),
            hovertemplate='%{x}<br>전체: %{y}명<extra></extra>'
        ))

        # 북클럽 시작 수직선 (인덱스 기준)
        for name, idx_date, color in [
            ('타프티',    date(2026,1,27), '#2E75B6'),
            ('매력자본',  date(2026,2,14), '#70AD47'),
            ('유혹의기술',date(2026,3,7),  '#FFC000'),
        ]:
            idx = (idx_date - ABS_START).days
            if 0 <= idx < len(xs_abs):
                fig_abs.add_vline(x=idx, line_dash='dot', line_color=color, line_width=1.5,
                                  annotation_text=name, annotation_position='top left',
                                  annotation_font_size=10, annotation_font_color=color)

        fig_abs.update_layout(
            height=400, plot_bgcolor='white', paper_bgcolor='white',
            legend=dict(orientation='h', y=-0.2),
            xaxis=dict(tickangle=-45, tickfont=dict(size=10), gridcolor='#eee',
                       tickmode='array',
                       tickvals=[xs_abs[i] for i in range(0, len(xs_abs), 3)],
                       ticktext=[xs_abs[i] for i in range(0, len(xs_abs), 3)]),
            yaxis=dict(title='누적 구독자 수', gridcolor='#eee'),
            margin=dict(l=40, r=20, t=30, b=80),
            hovermode='x unified'
        )
        st.plotly_chart(fig_abs, use_container_width=True)

        # 세션과 구독자 오버레이
        st.markdown('<div class="section-title">구독자 + 세션 동시 비교</div>', unsafe_allow_html=True)
        fig_ov = make_subplots(specs=[[{"secondary_y": True}]])

        fig_ov.add_trace(go.Bar(
            x=xs_abs,
            y=[data['daily'] and next((d['sessions'] for d in data['daily'] if d['date'] == (ABS_START+timedelta(days=i)).strftime('%Y%m%d')), 0) for i in range(n_days)],
            name='세션 수', marker_color='rgba(46,117,182,0.4)',
            hovertemplate='%{x}<br>세션: %{y}<extra></extra>'
        ), secondary_y=False)

        fig_ov.add_trace(go.Scatter(
            x=xs_abs, y=total_series, name='누적 구독자',
            mode='lines+markers', line=dict(color='#7030A0', width=3),
            marker=dict(size=5),
            hovertemplate='%{x}<br>누적: %{y}명<extra></extra>'
        ), secondary_y=True)

        fig_ov.update_layout(
            height=320, plot_bgcolor='white', paper_bgcolor='white',
            legend=dict(orientation='h', y=-0.25),
            xaxis=dict(tickangle=-45, tickfont=dict(size=9), gridcolor='#eee',
                       tickmode='array',
                       tickvals=[xs_abs[i] for i in range(0, len(xs_abs), 3)],
                       ticktext=[xs_abs[i] for i in range(0, len(xs_abs), 3)]),
            margin=dict(l=40, r=40, t=10, b=80)
        )
        fig_ov.update_yaxes(title_text="세션 수", secondary_y=False, gridcolor='#eee')
        fig_ov.update_yaxes(title_text="누적 구독자", secondary_y=True)
        st.plotly_chart(fig_ov, use_container_width=True)

    with tab2:
        st.markdown('<div class="section-title">D+N 기준 누적 구독자 (시작일=D+1, 이후 공란)</div>', unsafe_allow_html=True)

        MAX_DAY = max(
            (max(subs['타프티']) - min(subs['타프티'])).days + 1 if subs.get('타프티') else 1,
            (max(subs['매력자본']) - min(subs['매력자본'])).days + 1 if subs.get('매력자본') else 1,
            (max(subs['유혹의기술']) - min(subs['유혹의기술'])).days + 1 if subs.get('유혹의기술') else 1,
        )
        xs_rel = [f'D+{i+1}' for i in range(MAX_DAY)]

        fig_rel = go.Figure()
        for name, meta in BOOK_META.items():
            series = build_relative(subs.get(name,[]), MAX_DAY)
            fig_rel.add_trace(go.Scatter(
                x=xs_rel, y=series, name=name, mode='lines+markers',
                line=dict(color=meta['color'], width=3),
                marker=dict(size=6),
                connectgaps=False,
                hovertemplate='%{x}<br>'+name+': %{y}명<extra></extra>'
            ))

        fig_rel.update_layout(
            height=400, plot_bgcolor='white', paper_bgcolor='white',
            legend=dict(orientation='h', y=-0.2),
            xaxis=dict(tickangle=-45, tickfont=dict(size=10), gridcolor='#eee',
                       tickmode='array',
                       tickvals=[xs_rel[i] for i in range(0, MAX_DAY, 4)],
                       ticktext=[xs_rel[i] for i in range(0, MAX_DAY, 4)]),
            yaxis=dict(title='누적 구독자 수', gridcolor='#eee'),
            margin=dict(l=40, r=20, t=10, b=80),
            hovermode='x unified'
        )
        st.plotly_chart(fig_rel, use_container_width=True)

        # D+N 일별 신규 바차트
        st.markdown('<div class="section-title">D+N 일별 신규 구독자</div>', unsafe_allow_html=True)
        from collections import Counter
        fig_new = go.Figure()
        for name, meta in BOOK_META.items():
            dates_list = subs.get(name, [])
            if not dates_list: continue
            start = min(dates_list)
            dm    = Counter(dates_list)
            ys    = [dm.get(start + timedelta(days=i), 0) for i in range(MAX_DAY)]
            fig_new.add_trace(go.Bar(
                x=xs_rel, y=ys, name=name,
                marker_color=meta['color'], opacity=0.8,
                hovertemplate='%{x}<br>'+name+' 신규: %{y}명<extra></extra>'
            ))
        fig_new.update_layout(
            barmode='group', height=300, plot_bgcolor='white',
            legend=dict(orientation='h', y=-0.25),
            xaxis=dict(tickangle=-45, tickfont=dict(size=9), gridcolor='#eee',
                       tickmode='array',
                       tickvals=[xs_rel[i] for i in range(0, MAX_DAY, 4)],
                       ticktext=[xs_rel[i] for i in range(0, MAX_DAY, 4)]),
            yaxis=dict(title='신규 구독자 수', gridcolor='#eee'),
            margin=dict(l=40, r=20, t=10, b=80)
        )
        st.plotly_chart(fig_new, use_container_width=True)

    # ── 중복 구독자 분석 ──────────────────────────────
    st.markdown('<div class="section-title">📧 이메일 기준 중복 구독자 현황</div>', unsafe_allow_html=True)

    @st.cache_data(ttl=300)
    def load_emails():
        svc = get_sheets()
        emails = {}
        sheet_map = {
            '타프티':     ('타프티_구독자',     1),
            '매력자본':   ('매력자본_구독자',   1),
            '유혹의기술': ('유혹의기술_구독자', 1),
        }
        for name, (sheet_name, email_col) in sheet_map.items():
            res  = svc.spreadsheets().values().get(
                spreadsheetId=SHEET_ID, range=f'{sheet_name}!A:E').execute()
            rows = res.get('values', [])[1:]
            email_set = set()
            for row in rows:
                if len(row) > email_col and row[email_col] and '@' in str(row[email_col]):
                    email_set.add(row[email_col].strip().lower())
            emails[name] = email_set
        return emails

    emails = load_emails()
    T = emails.get('타프티', set())
    M = emails.get('매력자본', set())
    Y = emails.get('유혹의기술', set())

    TM  = T & M
    TY  = T & Y
    MY  = M & Y
    TMY = T & M & Y

    # KPI 카드
    ca, cb, cc, cd = st.columns(4)
    for col, label, val, detail, clr in [
        (ca, '타프티 + 매력자본',          len(TM),  sorted(TM),  '#5B9BD5'),
        (cb, '타프티 + 유혹의기술',        len(TY),  sorted(TY),  '#70AD47'),
        (cc, '매력자본 + 유혹의기술',      len(MY),  sorted(MY),  '#FFC000'),
        (cd, '타프티+매력자본+유혹의기술', len(TMY), sorted(TMY), '#C00000'),
    ]:
        col.markdown(f"""
        <div class="metric-card" style="border-left-color:{clr}">
            <div class="metric-val">{val}명</div>
            <div class="metric-lbl">{label}</div>
        </div>""", unsafe_allow_html=True)

    # 벤 다이어그램 대용 - 수평 바
    overlap_data = {
        '타프티만':           len(T - M - Y),
        '매력자본만':         len(M - T - Y),
        '유혹의기술만':       len(Y - T - M),
        '타프티+매력자본':    len(TM - Y),
        '타프티+유혹의기술':  len(TY - M),
        '매력자본+유혹의기술':len(MY - T),
        '세 곳 모두':         len(TMY),
    }
    bar_colors_ov = ['#2E75B6','#70AD47','#FFC000','#5B9BD5','#A9D18E','#FFE699','#C00000']

    fig_ov2 = go.Figure(go.Bar(
        y=list(overlap_data.keys()),
        x=list(overlap_data.values()),
        orientation='h',
        marker_color=bar_colors_ov,
        text=list(overlap_data.values()),
        textposition='outside',
    ))
    fig_ov2.update_layout(
        height=300, plot_bgcolor='white',
        xaxis=dict(title='구독자 수', gridcolor='#eee'),
        margin=dict(l=160, r=60, t=10, b=40)
    )
    st.plotly_chart(fig_ov2, use_container_width=True)




# ────────────────────────────────────────────────────
# 페이지3: 챕터 참여도
# ────────────────────────────────────────────────────
elif page == "📖 챕터 참여도":
    st.title("📖 챕터 참여도")

    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        st.markdown('<div class="section-title">챕터별 총 클릭수</div>', unsafe_allow_html=True)
        chap_df = pd.DataFrame(data['chap'])
        if not chap_df.empty:
            chap_df = chap_df.sort_values('clicks', ascending=True)
            fig_chap = go.Figure(go.Bar(
                x=chap_df['clicks'], y=chap_df['chapter'],
                orientation='h',
                marker=dict(color='#FFC000', line=dict(color='#CC9900', width=1)),
                text=chap_df['clicks'], textposition='outside'
            ))
            fig_chap.update_layout(height=420, plot_bgcolor='white',
                                   xaxis=dict(title='클릭 수', gridcolor='#eee'),
                                   margin=dict(l=10, r=40, t=10, b=40))
            st.plotly_chart(fig_chap, use_container_width=True)

    with col2:
        st.markdown('<div class="section-title">좋아요 챕터별 분포</div>', unsafe_allow_html=True)
        like_chap = data['like_by_chap']
        if like_chap:
            lc_df = pd.DataFrame(list(like_chap.items()), columns=['chapter','likes'])
            lc_df = lc_df.sort_values('likes', ascending=False)
            fig_lc = go.Figure(go.Bar(
                x=lc_df['chapter'], y=lc_df['likes'],
                marker=dict(color='#C00000'),
                text=lc_df['likes'], textposition='outside'
            ))
            fig_lc.update_layout(height=420, plot_bgcolor='white',
                                  yaxis=dict(title='좋아요 수', gridcolor='#eee'),
                                  margin=dict(l=10, r=10, t=10, b=40))
            st.plotly_chart(fig_lc, use_container_width=True)
        else:
            st.info("좋아요 챕터 데이터 없음")

    with col3:
        st.markdown('<div class="section-title">댓글 챕터별 분포</div>', unsafe_allow_html=True)
        comment_chap = data['comment_by_chap']
        if comment_chap:
            cc_df = pd.DataFrame(list(comment_chap.items()), columns=['chapter','comments'])
            cc_df = cc_df.sort_values('comments', ascending=False)
            fig_cc = go.Figure(go.Bar(
                x=cc_df['chapter'], y=cc_df['comments'],
                marker=dict(color='#2E75B6'),
                text=cc_df['comments'], textposition='outside'
            ))
            fig_cc.update_layout(height=420, plot_bgcolor='white',
                                  yaxis=dict(title='댓글 열기 수', gridcolor='#eee'),
                                  margin=dict(l=10, r=10, t=10, b=40))
            st.plotly_chart(fig_cc, use_container_width=True)
        else:
            st.info("댓글 챕터 데이터 없음")

    st.markdown('<div class="section-title">날짜별 챕터 클릭 히트맵</div>', unsafe_allow_html=True)
    chap_daily = data['chap_daily']
    def chap_sort_key(x):
        try: return int(x.replace('챕터','').replace('장',''))
        except: return 9999
    all_chaps_list = sorted(set(ch for d in chap_daily.values() for ch in d.keys()),
                             key=chap_sort_key)
    heatmap_data = []
    for dt in filtered_dates:
        row_d = {'날짜': dt[4:6]+'/'+dt[6:]}
        for ch in all_chaps_list:
            row_d[ch] = chap_daily.get(dt, {}).get(ch, 0)
        heatmap_data.append(row_d)
    hm_df = pd.DataFrame(heatmap_data).set_index('날짜')
    if not hm_df.empty and hm_df.values.max() > 0:
        fig_hm = go.Figure(go.Heatmap(
            z=hm_df.values.T,
            x=hm_df.index.tolist(),
            y=hm_df.columns.tolist(),
            colorscale='YlOrRd',
            text=hm_df.values.T,
            texttemplate='%{text}',
            showscale=True
        ))
        fig_hm.update_layout(height=300, plot_bgcolor='white',
                              xaxis=dict(tickangle=-45, tickfont=dict(size=9)),
                              margin=dict(l=60, r=20, t=10, b=60))
        st.plotly_chart(fig_hm, use_container_width=True)

# ────────────────────────────────────────────────────
# 페이지3: 좋아요 & 댓글
# ────────────────────────────────────────────────────
elif page == "❤️ 좋아요 & 댓글":
    st.title("❤️ 좋아요 & 댓글 인터랙션")

    total_like = sum(data['like_by_date'].values())
    total_ctog = sum(data['comment_by_date'].values())
    total_csub = sum(data['csub_by_date'].values())
    total_reply = sum(data['reply_by_date'].values())
    total_series = sum(data['series_by_date'].values())

    c1, c2, c3, c4, c5 = st.columns(5)
    for col, val, lbl, clr in [
        (c1, total_like,            "좋아요 클릭",    "#C00000"),
        (c2, total_ctog,            "댓글창 열기",    "#375623"),
        (c3, total_csub,            "댓글 제출",      "#375623"),
        (c4, total_reply,           "대댓글 제출",    "#2E75B6"),
        (c5, f"{(total_csub+total_reply)/total_ctog*100:.0f}%" if total_ctog else "0%", "댓글 전환율", "#7030A0"),
    ]:
        col.markdown(f"""
        <div class="metric-card" style="border-left-color:{clr}">
            <div class="metric-val">{val}</div>
            <div class="metric-lbl">{lbl}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">날짜별 인터랙션 추이</div>', unsafe_allow_html=True)
    xs = [dt[4:6]+'/'+dt[6:] for dt in filtered_dates]
    fig_eng = go.Figure()
    for key, name, color, width in [
        ('like_by_date',    '좋아요',       '#C00000', 3),
        ('comment_by_date', '댓글열기',     '#70AD47', 2),
        ('csub_by_date',    '댓글제출',     '#375623', 2),
        ('reply_by_date',   '대댓글',       '#2E75B6', 2),
        ('series_by_date',  '시리즈클릭',  '#9B59B6', 2),
    ]:
        ys = [data[key].get(d, 0) for d in filtered_dates]
        fig_eng.add_trace(go.Scatter(
            x=xs, y=ys, name=name, mode='lines+markers',
            line=dict(color=color, width=width),
            marker=dict(size=7 if width==3 else 5)
        ))
    fig_eng.update_layout(height=350, plot_bgcolor='white', paper_bgcolor='white',
                           legend=dict(orientation='h', y=-0.2),
                           xaxis=dict(tickangle=-45, tickfont=dict(size=10), gridcolor='#eee'),
                           yaxis=dict(title='건수', gridcolor='#eee'),
                           margin=dict(l=40, r=20, t=10, b=70))
    st.plotly_chart(fig_eng, use_container_width=True)

    # 로우 데이터 테이블
    st.markdown('<div class="section-title">날짜별 상세 데이터</div>', unsafe_allow_html=True)
    rows = []
    for dt in filtered_dates:
        lk = data['like_by_date'].get(dt, 0)
        ct = data['comment_by_date'].get(dt, 0)
        cs = data['csub_by_date'].get(dt, 0)
        rp = data['reply_by_date'].get(dt, 0)
        sr = data['series_by_date'].get(dt, 0)
        cc = data['chap_click_by_date'].get(dt, 0)
        if any([lk, ct, cs, rp, sr, cc]):
            rows.append({'날짜': dt[4:6]+'/'+dt[6:], '좋아요': lk or '',
                         '댓글열기': ct or '', '댓글제출': cs or '', '대댓글': rp or '',
                         '시리즈클릭': sr or '', '챕터클릭': cc or ''})
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# ────────────────────────────────────────────────────
# 페이지4: 리텐션
# ────────────────────────────────────────────────────
elif page == "🔁 리텐션":
    st.title("🔁 신규 vs 재방문 리텐션")

    total_new = sum(data['new_by_date'].values())
    total_ret = sum(data['ret_by_date'].values())
    ret_rate  = total_ret / (total_new + total_ret) * 100 if (total_new + total_ret) else 0
    all_new_dur = [data['new_dur'].get(d,0) for d in filtered_dates if data['new_dur'].get(d,0)]
    all_ret_dur = [data['ret_dur'].get(d,0) for d in filtered_dates if data['ret_dur'].get(d,0)]
    avg_new_dur = sum(all_new_dur)/len(all_new_dur) if all_new_dur else 0
    avg_ret_dur = sum(all_ret_dur)/len(all_ret_dur) if all_ret_dur else 0

    c1, c2, c3, c4 = st.columns(4)
    for col, val, lbl, clr in [
        (c1, total_new,         "신규 방문",      "#2E75B6"),
        (c2, total_ret,         "재방문",         "#ED7D31"),
        (c3, f"{ret_rate:.1f}%","재방문율",       "#7030A0"),
        (c4, f"+{avg_ret_dur-avg_new_dur:.0f}초","재방문 추가 체류", "#375623"),
    ]:
        col.markdown(f"""
        <div class="metric-card" style="border-left-color:{clr}">
            <div class="metric-val">{val}</div>
            <div class="metric-lbl">{lbl}</div>
        </div>""", unsafe_allow_html=True)

    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown('<div class="section-title">일자별 신규 vs 재방문 세션</div>', unsafe_allow_html=True)
        xs = [dt[4:6]+'/'+dt[6:] for dt in filtered_dates]
        fig_ret = go.Figure()
        fig_ret.add_trace(go.Bar(x=xs, y=[data['new_by_date'].get(d,0) for d in filtered_dates],
                                  name='신규', marker_color='#5B9BD5'))
        fig_ret.add_trace(go.Bar(x=xs, y=[data['ret_by_date'].get(d,0) for d in filtered_dates],
                                  name='재방문', marker_color='#ED7D31'))
        fig_ret.update_layout(barmode='stack', height=320, plot_bgcolor='white',
                               legend=dict(orientation='h', y=-0.25),
                               xaxis=dict(tickangle=-45, tickfont=dict(size=9), gridcolor='#eee'),
                               yaxis=dict(gridcolor='#eee'),
                               margin=dict(l=40, r=20, t=10, b=70))
        st.plotly_chart(fig_ret, use_container_width=True)

    with col2:
        st.markdown('<div class="section-title">신규 vs 재방문 비율</div>', unsafe_allow_html=True)
        fig_pie = go.Figure(go.Pie(
            labels=['신규', '재방문'],
            values=[total_new, total_ret],
            hole=0.4,
            marker=dict(colors=['#5B9BD5', '#ED7D31']),
            textinfo='label+percent',
            textfont_size=13
        ))
        fig_pie.add_annotation(text=f"{ret_rate:.0f}%\n재방문", x=0.5, y=0.5,
                                showarrow=False, font=dict(size=14, color='#ED7D31'))
        fig_pie.update_layout(height=320, showlegend=False,
                               margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig_pie, use_container_width=True)

    st.markdown('<div class="section-title">신규 vs 재방문 체류시간 비교</div>', unsafe_allow_html=True)
    xs = [dt[4:6]+'/'+dt[6:] for dt in filtered_dates]
    fig_dur = go.Figure()
    fig_dur.add_trace(go.Scatter(
        x=xs, y=[data['new_dur'].get(d,None) for d in filtered_dates],
        name='신규 체류(초)', mode='lines+markers',
        line=dict(color='#5B9BD5', width=2.5), marker=dict(size=6)
    ))
    fig_dur.add_trace(go.Scatter(
        x=xs, y=[data['ret_dur'].get(d,None) for d in filtered_dates],
        name='재방문 체류(초)', mode='lines+markers',
        line=dict(color='#ED7D31', width=2.5), marker=dict(size=6)
    ))
    fig_dur.update_layout(height=280, plot_bgcolor='white',
                           legend=dict(orientation='h', y=-0.25),
                           xaxis=dict(tickangle=-45, tickfont=dict(size=9), gridcolor='#eee'),
                           yaxis=dict(title='초', gridcolor='#eee'),
                           margin=dict(l=40, r=20, t=10, b=70))
    st.plotly_chart(fig_dur, use_container_width=True)

# ────────────────────────────────────────────────────
# 페이지5: 실시간
# ────────────────────────────────────────────────────
elif page == "🔴 실시간":
    st.title("🔴 실시간 현황")
    st.caption("30초마다 자동 갱신")

    try:
        rt = get_realtime()
        total_rt = sum(int(r.metric_values[0].value) for r in rt.rows)

        st.markdown(f"""
        <div class="metric-card" style="border-left-color:#C00000; max-width:300px">
            <div class="metric-val" style="color:#C00000">🔴 {total_rt}명</div>
            <div class="metric-lbl">지금 이 순간 활성 사용자</div>
        </div>""", unsafe_allow_html=True)

        if rt.rows:
            st.markdown('<div class="section-title">페이지별 실시간 사용자</div>', unsafe_allow_html=True)
            rt_data = [{'페이지': r.dimension_values[0].value,
                         '활성 사용자': int(r.metric_values[0].value)} for r in rt.rows]
            rt_df = pd.DataFrame(rt_data).sort_values('활성 사용자', ascending=False)
            st.dataframe(rt_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.warning(f"실시간 데이터 조회 중 오류: {e}")

    # 오늘 현황
    st.markdown('<div class="section-title">오늘 현황</div>', unsafe_allow_html=True)
    today_str = date.today().strftime('%Y%m%d')
    td = date_map.get(today_str, {})
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("오늘 세션",     td.get('sessions', 0))
    c2.metric("오늘 사용자",   td.get('users', 0))
    c3.metric("오늘 페이지뷰", td.get('pageviews', 0))
    c4.metric("오늘 좋아요",   data['like_by_date'].get(today_str, 0))

    # 30초 자동 새로고침
    time.sleep(30)
    st.rerun()

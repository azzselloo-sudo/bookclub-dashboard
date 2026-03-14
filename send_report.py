import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import io
import json
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from datetime import datetime, date, timedelta
from collections import defaultdict
import requests

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.oauth2 import service_account
import googleapiclient.discovery

# ── 설정 ──────────────────────────────────────────────
GMAIL_USER     = 'azzselloo@gmail.com'
GMAIL_PASSWORD = os.environ.get('GMAIL_APP_PASSWORD', '')   # 환경변수로 설정
RECIPIENTS     = ['azzselloo@gmail.com', 'eom183@naver.com']

PROPERTY_ID       = '523292335'
KEY_FILE          = 'C:/Users/ozcre/Downloads/loorou-bookclub-104e72493276.json'
SHEET_ID          = '1isk4R8YiWjH2AXKzPN8QjctU5XAVMicIkJzQznYE4JE'
SUPABASE_URL      = 'https://hegohmcxglujatsnlmtj.supabase.co'
SUPABASE_ANON_KEY = 'sb_publishable_feAjcEINhj0VQVFhgGNQYQ_Vn8nIBxP'
SCOPES      = [
    'https://www.googleapis.com/auth/analytics.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly',
]

TODAY = date.today().strftime('%Y년 %m월 %d일')
START = '2026-01-27'
START8 = '20260127'

# ── 인증 (GitHub Actions: 환경변수 / 로컬: JSON 파일) ──
_gcp_env = os.environ.get('GCP_SERVICE_ACCOUNT', '')
if _gcp_env:
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(_gcp_env), scopes=SCOPES)
else:
    credentials = service_account.Credentials.from_service_account_file(KEY_FILE, scopes=SCOPES)

ga4_client  = BetaAnalyticsDataClient(credentials=credentials)
sheets_svc  = googleapiclient.discovery.build('sheets', 'v4', credentials=credentials)

# ── GA4 데이터 수집 ───────────────────────────────────
def run(dims, metrics, start=START, end='today', limit=500):
    req = RunReportRequest(
        property=f'properties/{PROPERTY_ID}',
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimensions=[Dimension(name=d) for d in dims],
        metrics=[Metric(name=m) for m in metrics],
        limit=limit
    )
    return ga4_client.run_report(req)

print("GA4 데이터 수집 중...")
SERIES_NAME = {'tafti': '타프티', 'charm': '매력자본', 'seduction': '유혹의기술'}

# 날짜별 기본 지표
r1 = run(['date'], ['sessions','screenPageViews','activeUsers','averageSessionDuration','newUsers'])
daily, date_map = [], {}
for row in sorted(r1.rows, key=lambda x: x.dimension_values[0].value):
    d = row.dimension_values[0].value
    v = [x.value for x in row.metric_values]
    rec = {'date': d, 'sessions': int(v[0]), 'pageviews': int(v[1]),
           'users': int(v[2]), 'avg_dur': float(v[3]), 'new_users': int(v[4])}
    daily.append(rec)
    date_map[d] = rec

# 챕터별 클릭
r2 = run(['customEvent:series', 'customEvent:chapter'], ['eventCount'], limit=300)
chap = []
for row in sorted(r2.rows, key=lambda x: -int(x.metric_values[0].value)):
    s   = row.dimension_values[0].value
    ch  = row.dimension_values[1].value
    cnt = int(row.metric_values[0].value)
    if ch and ch not in ('(not set)', 'all'):
        s_name = SERIES_NAME.get(s, s) if s and s != '(not set)' else '미분류'
        chap.append({'chapter': f'{s_name} · {ch}챕터', 'clicks': cnt})

# 이벤트별 (좋아요·댓글)
r4 = run(['eventName', 'date', 'customEvent:series', 'customEvent:chapter'], ['eventCount'], limit=1000)
like_by_date    = defaultdict(int)
csub_by_date    = defaultdict(int)   # 댓글 (feedbacks + 구글시트)
reply_by_date   = defaultdict(int)   # 대댓글 (review_comments)
comment_by_date = defaultdict(int)
like_by_chap    = defaultdict(int)
comment_by_chap = defaultdict(int)
new_by_date     = defaultdict(int)
ret_by_date     = defaultdict(int)
for row in r4.rows:
    evt = row.dimension_values[0].value
    dt  = row.dimension_values[1].value
    s   = row.dimension_values[2].value
    ch  = row.dimension_values[3].value
    cnt = int(row.metric_values[0].value)
    s_name = SERIES_NAME.get(s, s) if s and s != '(not set)' else '미분류'
    if evt == 'like_click':
        like_by_date[dt] += cnt
        if ch and ch != '(not set)': like_by_chap[f'{s_name} · {ch}챕터'] += cnt
    elif evt == 'comment_toggle':
        comment_by_date[dt] += cnt
        if ch and ch != '(not set)': comment_by_chap[f'{s_name} · {ch}챕터'] += cnt

# Supabase: feedbacks → 댓글, review_comments → 대댓글
sb_headers = {'apikey': SUPABASE_ANON_KEY, 'Authorization': f'Bearer {SUPABASE_ANON_KEY}'}
try:
    r_fb = requests.get(f'{SUPABASE_URL}/rest/v1/feedbacks?select=created_at&limit=10000',
                        headers=sb_headers, timeout=10)
    if r_fb.ok:
        for row in r_fb.json():
            csub_by_date[row['created_at'][:10].replace('-', '')] += 1
except Exception: pass
try:
    r_cm = requests.get(f'{SUPABASE_URL}/rest/v1/review_comments?select=created_at&limit=10000',
                        headers=sb_headers, timeout=10)
    if r_cm.ok:
        for row in r_cm.json():
            reply_by_date[row['created_at'][:10].replace('-', '')] += 1
except Exception: pass

# 구글 시트 리뷰 → 댓글에 합산
try:
    gviz_url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?gid=438092292&tqx=out:json'
    r_gs = requests.get(gviz_url, timeout=12)
    if r_gs.ok:
        m = re.search(r'google\.visualization\.Query\.setResponse\((.*)\);', r_gs.text, re.DOTALL)
        if m:
            for row in json.loads(m.group(1)).get('table', {}).get('rows', []):
                cells = row.get('c', [])
                if not cells or not cells[0]: continue
                ts_raw = (cells[0] or {}).get('v', '')
                mm = re.match(r'Date\((\d+),(\d+),(\d+)', str(ts_raw))
                if mm:
                    y, mo, d = int(mm.group(1)), int(mm.group(2)) + 1, int(mm.group(3))
                    csub_by_date[f'{y}{mo:02d}{d:02d}'] += 1
except Exception: pass

# 신규 vs 재방문
r5 = run(['date', 'newVsReturning'], ['sessions'])
for row in r5.rows:
    dt  = row.dimension_values[0].value
    typ = row.dimension_values[1].value
    cnt = int(row.metric_values[0].value)
    if 'new' in typ.lower():    new_by_date[dt] += cnt
    elif 'return' in typ.lower(): ret_by_date[dt] += cnt

# ── 구독자 데이터 ─────────────────────────────────────
print("구독자 데이터 수집 중...")
def parse_ts(s):
    s = s.strip().replace('오전','AM').replace('오후','PM')
    for fmt in ['%Y. %m. %d %p %I:%M:%S']:
        try: return datetime.strptime(s, fmt).date()
        except: pass
    return None

subs = {}
Y_FIRST = date(2026, 3, 7)
for name, sheet_name in [('타프티','타프티_구독자'), ('매력자본','매력자본_구독자'), ('유혹의기술','유혹의기술_구독자')]:
    res  = sheets_svc.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=f'{sheet_name}!A:E').execute()
    rows = res.get('values', [])[1:]
    dates, first_date = [], None
    for row in rows:
        if not row or not row[0]: continue
        d = parse_ts(row[0])
        if d:
            if first_date is None: first_date = d
            dates.append(d)
        else:
            dates.append(first_date or Y_FIRST)
    subs[name] = sorted(dates)

# ── 날짜 축 생성 ──────────────────────────────────────
def all_dates_range(start_str=START8):
    s = datetime.strptime(start_str, '%Y%m%d').date()
    e = date.today()
    cur, result = s, []
    while cur <= e:
        result.append(cur.strftime('%Y%m%d'))
        cur += timedelta(days=1)
    return result

all_dates = all_dates_range()
xs = [dt[4:6]+'/'+dt[6:] for dt in all_dates]

# ── 차트 PNG 생성 (kaleido) ───────────────────────────
COLORS_SERIES = {'타프티': '#2E75B6', '매력자본': '#70AD47', '유혹의기술': '#FFC000'}
chart_images = []  # (title, bytes)

def fig_to_bytes(fig, title):
    buf = io.BytesIO(fig.to_image(format='png', width=900, height=420, scale=2))
    chart_images.append((title, buf.getvalue()))

print("차트 생성 중...")

# ① 일자별 세션 & 페이지뷰
fig1 = make_subplots(specs=[[{"secondary_y": True}]])
fig1.add_trace(go.Bar(x=xs, y=[date_map.get(d,{}).get('sessions',0) for d in all_dates],
                      name='세션', marker_color='#2E75B6', opacity=0.8), secondary_y=False)
fig1.add_trace(go.Scatter(x=xs, y=[date_map.get(d,{}).get('avg_dur',None) for d in all_dates],
                          name='평균체류(초)', mode='lines+markers',
                          line=dict(color='#7030A0', width=2), marker=dict(size=4)), secondary_y=True)
fig1.update_layout(title='① 일자별 세션 & 평균 체류시간', height=420, plot_bgcolor='white',
                   legend=dict(orientation='h', y=-0.25),
                   xaxis=dict(tickangle=-45, tickfont=dict(size=8), gridcolor='#eee'),
                   margin=dict(l=40,r=40,t=50,b=80))
fig_to_bytes(fig1, '일자별 세션 & 평균 체류시간')

# ② 구독자 누적 추이 (절대일)
abs_start = datetime.strptime(START8, '%Y%m%d').date()
abs_end   = date.today()
fig2 = go.Figure()
for name, color in COLORS_SERIES.items():
    if name in subs:
        from collections import Counter
        dm  = Counter(subs[name])
        cum, series = 0, []
        cur = abs_start
        while cur <= abs_end:
            cum += dm.get(cur, 0)
            series.append(cum)
            cur += timedelta(days=1)
        fig2.add_trace(go.Scatter(x=xs[:len(series)], y=series, name=name,
                                  mode='lines+markers', line=dict(color=color, width=2.5),
                                  marker=dict(size=5)))
fig2.update_layout(title='② 북클럽별 누적 구독자 추이', height=420, plot_bgcolor='white',
                   legend=dict(orientation='h', y=-0.25),
                   xaxis=dict(tickangle=-45, tickfont=dict(size=8), gridcolor='#eee'),
                   yaxis=dict(title='누적 구독자', gridcolor='#eee'),
                   margin=dict(l=40,r=20,t=50,b=80))
fig_to_bytes(fig2, '북클럽별 누적 구독자 추이')

# ③ 챕터별 클릭수 (상위 15)
if chap:
    chap_df = pd.DataFrame(chap[:15]).sort_values('clicks', ascending=True)
    fig3 = go.Figure(go.Bar(x=chap_df['clicks'], y=chap_df['chapter'],
                            orientation='h', marker=dict(color='#FFC000'),
                            text=chap_df['clicks'], textposition='outside'))
    fig3.update_layout(title='③ 챕터별 클릭수 (상위 15)', height=420, plot_bgcolor='white',
                       xaxis=dict(title='클릭 수', gridcolor='#eee'),
                       margin=dict(l=10,r=60,t=50,b=40))
    fig_to_bytes(fig3, '챕터별 클릭수')

# ④ 좋아요 챕터별 분포
if like_by_chap:
    lc_df = pd.DataFrame(list(like_by_chap.items()), columns=['chapter','likes']).sort_values('likes', ascending=False)
    fig4 = go.Figure(go.Bar(x=lc_df['chapter'], y=lc_df['likes'],
                            marker=dict(color='#C00000'),
                            text=lc_df['likes'], textposition='outside'))
    fig4.update_layout(title='④ 좋아요 챕터별 분포', height=360, plot_bgcolor='white',
                       yaxis=dict(title='좋아요 수', gridcolor='#eee'),
                       margin=dict(l=10,r=20,t=50,b=60))
    fig_to_bytes(fig4, '좋아요 챕터별 분포')

# ⑤ 댓글 챕터별 분포
if comment_by_chap:
    cc_df = pd.DataFrame(list(comment_by_chap.items()), columns=['chapter','comments']).sort_values('comments', ascending=False)
    fig5 = go.Figure(go.Bar(x=cc_df['chapter'], y=cc_df['comments'],
                            marker=dict(color='#2E75B6'),
                            text=cc_df['comments'], textposition='outside'))
    fig5.update_layout(title='⑤ 댓글 챕터별 분포', height=360, plot_bgcolor='white',
                       yaxis=dict(title='댓글 열기 수', gridcolor='#eee'),
                       margin=dict(l=10,r=20,t=50,b=60))
    fig_to_bytes(fig5, '댓글 챕터별 분포')

# ⑥ 일자별 좋아요 & 댓글 추이
fig6 = go.Figure()
fig6.add_trace(go.Scatter(x=xs, y=[like_by_date.get(d,0) for d in all_dates],
                          name='좋아요', mode='lines+markers',
                          line=dict(color='#C00000', width=2.5), marker=dict(size=5)))
fig6.add_trace(go.Scatter(x=xs, y=[csub_by_date.get(d,0) for d in all_dates],
                          name='댓글 제출', mode='lines+markers',
                          line=dict(color='#2E75B6', width=2.5), marker=dict(size=5)))
fig6.add_trace(go.Scatter(x=xs, y=[comment_by_date.get(d,0) for d in all_dates],
                          name='댓글 열기', mode='lines+markers',
                          line=dict(color='#70AD47', width=2, dash='dot'), marker=dict(size=4)))
fig6.update_layout(title='⑥ 일자별 좋아요 & 댓글 추이', height=380, plot_bgcolor='white',
                   legend=dict(orientation='h', y=-0.3),
                   xaxis=dict(tickangle=-45, tickfont=dict(size=8), gridcolor='#eee'),
                   yaxis=dict(gridcolor='#eee'),
                   margin=dict(l=40,r=20,t=50,b=80))
fig_to_bytes(fig6, '일자별 좋아요 & 댓글 추이')

# ⑦ 신규 vs 재방문 누적
total_new = sum(new_by_date.values())
total_ret = sum(ret_by_date.values())
fig7 = go.Figure()
fig7.add_trace(go.Bar(x=xs, y=[new_by_date.get(d,0) for d in all_dates],
                      name='신규', marker_color='#5B9BD5'))
fig7.add_trace(go.Bar(x=xs, y=[ret_by_date.get(d,0) for d in all_dates],
                      name='재방문', marker_color='#ED7D31'))
fig7.update_layout(title='⑦ 신규 vs 재방문 세션', barmode='stack', height=380, plot_bgcolor='white',
                   legend=dict(orientation='h', y=-0.3),
                   xaxis=dict(tickangle=-45, tickfont=dict(size=8), gridcolor='#eee'),
                   yaxis=dict(gridcolor='#eee'),
                   margin=dict(l=40,r=20,t=50,b=80))
fig_to_bytes(fig7, '신규 vs 재방문 세션')

# ⑧ 신규 vs 재방문 파이
ret_rate = total_ret / (total_new + total_ret) * 100 if (total_new + total_ret) else 0
fig8 = go.Figure(go.Pie(
    labels=['신규', '재방문'], values=[total_new, total_ret],
    hole=0.4, marker=dict(colors=['#5B9BD5', '#ED7D31']),
    textinfo='label+percent', textfont_size=13
))
fig8.add_annotation(text=f"{ret_rate:.0f}%\n재방문", x=0.5, y=0.5,
                    showarrow=False, font=dict(size=14, color='#ED7D31'))
fig8.update_layout(title='⑧ 신규 vs 재방문 비율', height=380, showlegend=False,
                   margin=dict(l=10,r=10,t=50,b=10))
fig_to_bytes(fig8, '신규 vs 재방문 비율')

print(f"차트 {len(chart_images)}개 생성 완료")

# ── KPI 요약 텍스트 ───────────────────────────────────
total_sess  = sum(r.get('sessions', 0) for r in daily)
total_pv    = sum(r.get('pageviews', 0) for r in daily)
total_subs  = sum(len(v) for v in subs.values())
w_dur = sum(r.get('avg_dur',0) * r.get('sessions',0) for r in daily)
avg_dur = w_dur / total_sess if total_sess else 0
total_like   = sum(like_by_date.values())
total_csub   = sum(csub_by_date.values())
total_reply  = sum(reply_by_date.values())

kpi_html = f"""
<table style="border-collapse:collapse; width:100%; font-family:Arial,sans-serif;">
  <tr style="background:#1F4E79; color:white;">
    <th style="padding:10px; text-align:center;">총 구독자</th>
    <th style="padding:10px; text-align:center;">총 세션</th>
    <th style="padding:10px; text-align:center;">페이지뷰</th>
    <th style="padding:10px; text-align:center;">평균 체류</th>
    <th style="padding:10px; text-align:center;">좋아요</th>
    <th style="padding:10px; text-align:center;">댓글</th>
    <th style="padding:10px; text-align:center;">대댓글</th>
    <th style="padding:10px; text-align:center;">재방문율</th>
  </tr>
  <tr style="background:#f0f4f8; text-align:center; font-size:1.3em; font-weight:bold;">
    <td style="padding:12px; color:#1F4E79;">{total_subs}명</td>
    <td style="padding:12px; color:#2E75B6;">{total_sess}</td>
    <td style="padding:12px; color:#FFC000;">{total_pv}</td>
    <td style="padding:12px; color:#7030A0;">{avg_dur/60:.1f}분</td>
    <td style="padding:12px; color:#C00000;">{total_like}❤️</td>
    <td style="padding:12px; color:#375623;">{total_csub}💬</td>
    <td style="padding:12px; color:#2E75B6;">{total_reply}💬</td>
    <td style="padding:12px; color:#ED7D31;">{ret_rate:.1f}%</td>
  </tr>
</table>
"""

subs_html = "".join(
    f'<li><b>{name}</b>: {len(dates)}명 (최근 가입 {max(dates).strftime("%m/%d") if dates else "-"})</li>'
    for name, dates in subs.items()
)

html_body = f"""
<html><body style="font-family:Arial,sans-serif; color:#333; max-width:900px; margin:0 auto;">
  <h2 style="color:#1F4E79; border-bottom:3px solid #2E75B6; padding-bottom:8px;">
    📚 북클럽 성과 리포트 — {TODAY}
  </h2>

  <h3 style="color:#1F4E79;">📊 전체 KPI 요약</h3>
  {kpi_html}

  <h3 style="color:#1F4E79; margin-top:24px;">👥 북클럽별 구독자</h3>
  <ul>{subs_html}</ul>

  <h3 style="color:#1F4E79; margin-top:24px;">📈 차트 리포트</h3>
  <p style="color:#666; font-size:0.9em;">아래 차트는 2026/01/27 ~ 오늘 기준입니다.</p>
"""

for i, (title, _) in enumerate(chart_images):
    html_body += f'<h4 style="color:#2E75B6; margin-top:20px;">{title}</h4>'
    html_body += f'<img src="cid:chart{i}" style="max-width:100%; border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,0.1);">'

html_body += """
  <br><hr>
  <p style="color:#999; font-size:0.8em;">
    이 리포트는 매일 오전 9시 자동 발송됩니다. |
    <a href="https://azzselloo-bookclub.streamlit.app" style="color:#2E75B6;">대시보드 바로가기</a>
  </p>
</body></html>
"""

# ── 이메일 발송 ───────────────────────────────────────
print("이메일 발송 중...")
msg = MIMEMultipart('related')
msg['Subject'] = f'📚 북클럽 일일 리포트 — {TODAY}'
msg['From']    = GMAIL_USER
msg['To']      = ', '.join(RECIPIENTS)

msg_alt = MIMEMultipart('alternative')
msg.attach(msg_alt)
msg_alt.attach(MIMEText(html_body, 'html', 'utf-8'))

for i, (title, img_bytes) in enumerate(chart_images):
    img = MIMEImage(img_bytes, 'png')
    img.add_header('Content-ID', f'<chart{i}>')
    img.add_header('Content-Disposition', 'inline', filename=f'chart_{i}.png')
    msg.attach(img)

with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
    server.login(GMAIL_USER, GMAIL_PASSWORD)
    server.sendmail(GMAIL_USER, RECIPIENTS, msg.as_string())

print(f"✅ 이메일 발송 완료 → {', '.join(RECIPIENTS)}")

# BBQ 발주 분석 V4

반영 내용
- 웹공용 사용 기준 구조
- 운영2팀 총합본 별도 페이지
- BM(사용자 이름) 직접 입력 후 개인 페이지 자동 필터
- 담당 매장 관리 페이지
- 점수제 제거
- 컵소스 제거
- 소비기한 리스크는 저발주/미발주/급감 중심
- 소비기한 페이지 AI 판단 보조
- 매장별 이력조회 AI 관리 방향 제시

웹배포 권장 구조
- Streamlit Community Cloud
- DATABASE_URL을 Streamlit Secrets에 저장
- PostgreSQL/Supabase 권장

로컬 실행
pip install -r requirements.txt
streamlit run streamlit_app.py
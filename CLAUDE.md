# US Recession Indicator Monitoring

이 폴더는 미국 경기침체 지표를 모니터링하는 용도로만 사용한다. 아래 규칙은 이 폴더 안에서만 적용된다.

## 자동 발동 규칙

이 폴더에서 시장 국면 / 경기침체 / 매크로 지표 / 리스크온-오프에 관한 요청이 들어오면,
사용자가 스킬 이름을 언급하지 않아도 아래 스킬을 관련도 순으로 우선 검토하고 사용한다.

1. **macro-regime-detector** — 기본 진입점. RSP/SPY, 10Y-2Y, HYG/LQD, IWM/SPY, SPY/TLT, XLY/XLP 6개
   크로스에셋 비율로 구조적 국면(Concentration/Broadening/Contraction/Inflationary/Transitional)을 판정한다.
   "지금 국면이 어떤지", "침체 국면으로 전환 중인지" 같은 질문에는 항상 이 스킬부터 실행한다.
   실행: `python3 skills/macro-regime-detector/scripts/macro_regime_detector.py --output-dir reports/`
   **FMP_API_KEY 환경변수 필요** — 아래 "API 키" 절 참고.

2. **market-top-detector** — Distribution Day, 리딩주 붕괴, 방어섹터 로테이션으로 2~8주 단기 고점 리스크를 스코어링.
   "지금 비중을 줄여야 하는지" 전술적 타이밍 질문에 사용.

3. **ftd-detector** — Follow-Through Day 탐지로 시장 바닥 확인. 조정 이후 재진입 타이밍 질문에 사용.

4. **market-breadth-analyzer** — 시장 참여 폭(advance-decline 건강도) 0-100 스코어. API 키 불필요.

5. **us-market-bubble-detector** — 미국 증시 버블 신호 점검.

6. **exposure-coach** — 위 스킬들의 신호를 종합해 순노출 한도·신규진입 가능 여부를 한 페이지로 요약.
   여러 지표를 한 번에 봤을 때 "그래서 지금 뭘 해야 하는지" 결론이 필요하면 마지막에 실행.

7. **economic-calendar-fetcher / economics-analysis / market-environment-analysis** — 이번 주 발표되는
   고용·물가·GDP 지표 캘린더, 개별 지표 해설이 필요할 때 보조로 사용.

## 사용 원칙

- 새 대화에서 이 폴더를 열고 "국면 어때", "침체 신호 있어?" 같은 모호한 질문이 와도, 위 1번(macro-regime-detector)부터
  자동으로 실행해서 답한다. 스킬 이름을 대라고 되묻지 않는다.
- 이 폴더 밖(다른 프로젝트)에서는 이 규칙을 적용하지 않는다 — 스킬 자동 판단에 맡긴다.
- 리포트는 이 폴더 하위 `reports/`에 누적 저장한다. 과거 리포트와 비교해 추세 변화를 언급할 것.

## API 키

`macro-regime-detector`는 FMP(Financial Modeling Prep) 무료 API 키가 필요하다.
`.env` 파일을 만들고 아래처럼 채워넣는다 (커밋 금지, `.gitignore`에 이미 포함):

```
FMP_API_KEY=your_key_here
```

키가 없으면 발급 절차를 안내하고, 다른 무료(API 불필요) 스킬인 market-breadth-analyzer로 대체 진행할지 물어본다.

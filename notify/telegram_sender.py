"""
notify/telegram_sender.py ─ 텔레그램 전송 모듈
=====================================================================
진단 요약 텍스트 메시지 + HTML 대시보드 파일 첨부를 전송합니다.
점수·판정 없이, 각 지표의 현재값·변화를 팩트 기반으로 정리합니다.
"""

from __future__ import annotations

import math
import os
from datetime import datetime

import requests

from indicators.base import IndicatorResult


class TelegramSender:
    """텔레그램 Bot API를 통해 리포트를 전송합니다."""

    MAX_LEN = 4096    # Telegram 메시지 최대 길이

    def __init__(self, token: str, chat_id: str):
        self._token   = token
        self._chat_id = chat_id
        self._ok      = bool(token and chat_id)
        self._base    = f"https://api.telegram.org/bot{token}"

    # ── Public API ───────────────────────────────────────────────────

    def send_report(
        self,
        results:       list[IndicatorResult],
        html_path:     str,
        category_meta: dict,
        title:         str = "매크로 유동성 대시보드",
    ) -> None:
        """요약 메시지 전송 → HTML 파일 첨부."""
        if not self._ok:
            print("  ⚠️  TELEGRAM 설정 없음 → 전송 건너뜀")
            return

        print("  → 텔레그램 전송 시작...")

        # ① 헤더 + 카테고리별 요약
        messages = self._build_messages(results, category_meta, title)
        for i, msg in enumerate(messages, 1):
            if self._send_text(msg):
                print(f"  ✅  메시지 {i}/{len(messages)} 전송 완료")

        # ② HTML 파일 첨부
        caption = (
            f"📊 <b>{title}</b>\n"
            f"{datetime.now().strftime('%Y-%m-%d')}\n"
            f"브라우저에서 열면 인터랙티브 차트를 확인할 수 있습니다."
        )
        if self._send_document(html_path, caption):
            print("  ✅  HTML 대시보드 첨부 완료")

    # ── 메시지 빌더 ──────────────────────────────────────────────────

    def _build_messages(
        self, results: list[IndicatorResult], category_meta: dict, title: str
    ) -> list[str]:
        """
        카테고리별로 지표 현황을 정리한 메시지 목록 반환.
        4096자 초과 시 자동 분할.
        """
        result_map = {r.key: r for r in results}

        header = (
            f"🏦 <b>{title}</b>\n"
            f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M KST')}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
        )

        blocks: list[str] = []

        for cat, (name, desc, indicators) in category_meta.items():
            block = f"\n<b>{name}</b>\n"
            for meta in indicators:
                r = result_map.get(meta.key)
                if r is None:
                    continue
                block += self._format_indicator_line(r)
            blocks.append(block)

        # 4096자 기준 분할
        messages: list[str] = []
        buf = header
        for block in blocks:
            if len(buf) + len(block) > self.MAX_LEN:
                messages.append(buf)
                buf = ""
            buf += block
        if buf:
            messages.append(buf)

        return messages

    @staticmethod
    def _format_indicator_line(r: IndicatorResult) -> str:
        """지표 한 줄 요약."""
        def fmt_val(v, unit):
            if v is None or math.isnan(v):
                return "N/A"
            if unit == "$B":
                return f"${v:,.0f}B"
            if unit in ("%", "bps"):
                return f"{v:.2f}{unit}"
            return f"{v:.2f}"

        def fmt_chg(v, unit):
            if v is None or math.isnan(v):
                return "—"
            sign = "+" if v >= 0 else ""
            arrow = "▲" if v > 0 else ("▼" if v < 0 else "—")
            if unit == "$B":
                return f"{arrow}{sign}${abs(v):,.0f}B"
            return f"{arrow}{sign}{abs(v):.2f}"

        latest = fmt_val(r.latest_value, r.unit)
        chg_1m = fmt_chg(r.chg_1m, r.unit)
        chg_1y = fmt_chg(r.chg_1y, r.unit)

        return (
            f"  <code>{r.name_ko:<16}</code> "
            f"<b>{latest:<10}</b> "
            f"1M:{chg_1m}  1Y:{chg_1y}\n"
        )

    # ── HTTP 헬퍼 ────────────────────────────────────────────────────

    def _post(self, method: str, **kwargs) -> bool:
        try:
            resp = requests.post(
                f"{self._base}/{method}", timeout=30, **kwargs
            )
            if not resp.ok:
                print(f"  ⚠️  {method} 실패: {resp.status_code} {resp.text[:100]}")
            return resp.ok
        except Exception as e:
            print(f"  ❌  {method} 오류: {e}")
            return False

    def _send_text(self, text: str) -> bool:
        return self._post(
            "sendMessage",
            data={"chat_id": self._chat_id, "text": text, "parse_mode": "HTML"},
        )

    def _send_document(self, path: str, caption: str = "") -> bool:
        if not os.path.exists(path):
            print(f"  ⚠️  파일 없음: {path}")
            return False
        with open(path, "rb") as f:
            return self._post(
                "sendDocument",
                data={
                    "chat_id":    self._chat_id,
                    "caption":    caption,
                    "parse_mode": "HTML",
                },
                files={"document": (os.path.basename(path), f, "text/html")},
            )

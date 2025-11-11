#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigRise 종합 파이프라인 (Prefect Orion 통합 버전)
------------------------------------------------
① Naver 뉴스 → ② RISE ETF → ③ BigFinance → ④ ETF–산업 매칭
"""

from prefect import flow, get_run_logger
from prefect.context import get_run_context
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from pipelines.common.tasks import run_script, notify

BASE_DIR = Path(__file__).resolve().parent


@flow(name="BigRise Pipeline", log_prints=True)
def bigrise_pipeline(target_date: Optional[str] = None):
    """
    BigRise 메인 파이프라인 (Prefect 3.6)
    ------------------------------------------------
    Args:
        target_date (str, optional): YYYYMMDD 형식의 기준일.
            - 미지정 시 Flow 실행 기준일의 '전일'로 자동 설정됨.
    """
    logger = get_run_logger()
    logger.info("🧭 BigRise 파이프라인 시작")

    KST = timezone(timedelta(hours=9))

    # Prefect Context 기반 기준일 계산
    if target_date is None or not isinstance(target_date, str):
        # Prefect UTC context는 참고만 하고, 실제 기준은 현지시간 기준으로 계산
        now_kst = datetime.now(KST)
        target_date = (now_kst - timedelta(days=1)).strftime("%Y%m%d")
        run_date = now_kst.strftime("%Y%m%d")

    # ① Naver 뉴스 수집
    logger.info(f"📰 Target 수집 시작 📅 기준일: {run_date}")
    logger.info(f"📰 Naver 뉴스 수집 시작 📅 기준일: {target_date}")
    naver_fut = run_script.submit(BASE_DIR / "naver_news.py", "--date", target_date)

    # ② RISE ETF 수집
    logger.info("📈 RISE ETF 수집 시작")
    riseetf_fut = run_script.submit(BASE_DIR / "riseetf.py")

    # ③ BigFinance 산업 데이터 수집
    logger.info("💰 BigFinance 산업 데이터 수집 시작")
    bigfinance_fut = run_script.submit(BASE_DIR / "bigfinance.py")

    # ④ 종합 매칭 (위 세 작업 완료 후 실행)
    logger.info("🔗 BigRise 산업 매칭 시작")
    bigrise_pre_fut = run_script.submit(
        BASE_DIR / "bigrise_pre.py",
        wait_for=[naver_fut, riseetf_fut, bigfinance_fut],
    )

    # 완료 알림
    notify.submit(
        f"🎯 BigRise 파이프라인 완료 ({target_date})",
        wait_for=[bigrise_pre_fut],
    )

    # 결과 확인 및 실패 감지
    results = [
        naver_fut.result(),
        riseetf_fut.result(),
        bigfinance_fut.result(),
        bigrise_pre_fut.result(),
    ]
    if any(r is None for r in results):
        raise RuntimeError("❌ 일부 Task가 실패했습니다.")

    logger.info("✅ 전체 파이프라인 정상 완료")
    return target_date


if __name__ == "__main__":
    bigrise_pipeline()

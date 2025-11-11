#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
공통 Task 유틸리티 (모든 Prefect 파이프라인에서 import)
"""

import subprocess
from pathlib import Path
from typing import Optional, List
from prefect import task, get_run_logger


@task(retries=1, retry_delay_seconds=60)
def run_script(script_path: str, *args: str):
    """
    지정된 Python 스크립트를 subprocess로 실행.
    Prefect Task로 감싸져 있어 UI에서 개별 모니터링 가능.
    ex) run_script.submit("naver_news.py", "--date", "20251109")
    """
    logger = get_run_logger()
    path = Path(script_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"❌ 파일을 찾을 수 없습니다: {path}")

    cmd = ["python3", str(path), *args]
    logger.info(f"🚀 실행 명령어: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info(result.stdout.strip())

    if result.returncode != 0:
        logger.error(result.stderr.strip())
        raise RuntimeError(f"❌ 실행 실패: {path.name}")

    logger.info(f"✅ 완료: {path.name}")
    return result.stdout.strip()


@task
def notify(message: str):
    """
    단순 알림용 Task (예: Slack, Email 연동 전 단계)
    """
    logger = get_run_logger()
    logger.info(f"🔔 알림: {message}")

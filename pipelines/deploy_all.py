#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prefect 3.6 배포 자동화 스크립트 (확정 안정 버전)
"""

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PIPELINES_DIR = ROOT
EXCLUDE_DIRS = {"common", "__pycache__"}

def build_and_apply_pipeline(flow_path: Path):
    name = flow_path.stem.replace("_", " ").title()
    entrypoint = f"{os.path.relpath(flow_path, Path.cwd())}:{flow_path.stem}_pipeline"

    print(f"⚙️  [{name}] Prefect 배포 중...")
    try:
        subprocess.run(
            [
                "prefect",
                "deploy",
                str(entrypoint),
                "--name", f"{name} Daily",
                "--pool", "default",             # ✅ 올바른 3.6 옵션
                "--work-queue", "default",
                "--tag", "automation",           # ✅ 단수 --tag 사용
                "--description", f"자동 등록된 {name} 파이프라인",
            ],
            check=True,
        )
        print(f"✅ [{name}] 배포 완료\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ [{name}] Prefect 배포 실패 ({e.returncode})")
        raise


def main():
    print("🔍 모든 파이프라인 자동 배포 시작...\n")
    for subdir in PIPELINES_DIR.iterdir():
        if not subdir.is_dir() or subdir.name in EXCLUDE_DIRS:
            continue
        for flow_py in subdir.glob("*.py"):
            if flow_py.stem.endswith("_prefect") or flow_py.stem in {"bigrise"}:
                try:
                    build_and_apply_pipeline(flow_py)
                except Exception as e:
                    print(f"⚠️ {flow_py.name} 실패: {e}\n")
                    continue
    print("🎯 모든 파이프라인 자동 등록 완료!\n")

if __name__ == "__main__":
    main()

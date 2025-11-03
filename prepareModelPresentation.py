#!/usr/bin/env python3

"""
[발표 전 실행]
- atio 모델 스냅샷 시연을 위한 '무거운' 준비 스크립트
- 약 1GB의 더미 모델 파일 2개(v1, v2)를 생성합니다.
- atio.write_model_snapshot을 미리 실행하여 'ATIO_MODEL_SNAPSHOT' 폴더를 생성합니다.
- 실행에 1~2분 정도 소요될 수 있습니다.
"""

import os
import shutil
import numpy as np
import torch
import atio
import sys

# --- 설정 ---
DEMO_DIR = "ATIO_MODEL_DEMO_PREP"
MODEL_V1_PATH = os.path.join(DEMO_DIR, "model_v1_base.pth")
MODEL_V2_PATH = os.path.join(DEMO_DIR, "model_v2_finetuned.pth")
SNAPSHOT_PATH = "ATIO_MODEL_SNAPSHOT" # 메인 데모 스크립트가 참조할 경로

# --- 헬퍼 ---
def print_header(title):
    print("\n" + "=" * 70)
    print(f"🔧 {title}")
    print("=" * 70)

# (create_dummy_model_file 함수는 이제 사용되지 않으므로 삭제하거나 주석 처리)
# def create_dummy_model_file(path, size_mb, change_ratio=0.0):
#     ...

def setup():
    # 이전 데모 파일/폴더 정리
    shutil.rmtree(DEMO_DIR, ignore_errors=True)
    shutil.rmtree(SNAPSHOT_PATH, ignore_errors=True)
    os.makedirs(DEMO_DIR, exist_ok=True)
    
    print_header("1. 더미 모델 파일 생성 (v1, v2)")
    
    # --- v1 생성 ---
    print(f"  > v1: '{os.path.basename(MODEL_V1_PATH)}' 생성 중 (약 500MB)...")
    size_mb = 500
    num_elements = (size_mb * 1024 * 1024) // 4
    
    # 100% 랜덤 데이터로 v1 텐서 생성
    v1_tensor = torch.randn(num_elements) 
    v1_state_dict = {'layer1.weight': v1_tensor}
    torch.save(v1_state_dict, MODEL_V1_PATH)
    print(f"  ✅ v1 생성 완료 (크기: {os.path.getsize(MODEL_V1_PATH) / 1024 / 1024:.0f}MB)")

    # --- v2 생성 (v1 기반으로 1% 수정) ---
    print(f"  > v2: '{os.path.basename(MODEL_V2_PATH)}' 생성 중 (v1에서 1% 수정)...")
    
    # v1 텐서를 그대로 복제 (99% 동일)
    v2_tensor = v1_tensor.clone() 
    
    # 1%만 새로운 데이터로 덮어쓰기
    change_ratio = 0.01
    change_elements = int(num_elements * change_ratio)
    v2_tensor[:change_elements] = torch.randn(change_elements) # 이 부분만 v1과 다름
    
    v2_state_dict = {'layer1.weight': v2_tensor}
    torch.save(v2_state_dict, MODEL_V2_PATH)
    print(f"  ✅ v2 생성 완료 (크기: {os.path.getsize(MODEL_V2_PATH) / 1024 / 1024:.0f}MB)")


    print_header("2. atio 모델 스냅샷 생성 (v1, v2)")
    print("  > v1 스냅샷 저장 중 (500MB)...")
    atio.write_model_snapshot(MODEL_V1_PATH, SNAPSHOT_PATH, show_progress=True)
    print("  ✅ v1 스냅샷 저장 완료.")
    
    print("\n  > v2 스냅샷 저장 중 (v1과 비교하여 변경분 1%만 저장)...")
    atio.write_model_snapshot(MODEL_V2_PATH, SNAPSHOT_PATH, show_progress=True)
    print("  ✅ v2 스냅샷 저장 완료.")

    print_header("3. 결과 확인")
    size_v1 = os.path.getsize(MODEL_V1_PATH)
    size_v2 = os.path.getsize(MODEL_V2_PATH)
    
    # 스냅샷 'data' 폴더의 실제 크기 계산
    snapshot_data_dir = os.path.join(SNAPSHOT_PATH, "data")
    snapshot_size = sum(
        os.path.getsize(os.path.join(snapshot_data_dir, f))
        for f in os.listdir(snapshot_data_dir)
        if os.path.isfile(os.path.join(snapshot_data_dir, f))
    )
    
    print(f"  > 원본 v1 크기:    {size_v1 / 1024 / 1024:,.0f} MB")
    print(f"  > 원본 v2 크기:    {size_v2 / 1024 / 1024:,.0f} MB")
    print(f"  > 원본 총합:     {(size_v1 + size_v2) / 1024 / 1024:,.0f} MB")
    print("-" * 30)
    # (수정) f-string 포맷팅 수정
    print(f"  > atio 스냅샷 크기: {snapshot_size / 1024 / 1024:,.0f} MB  (v1 + v2 변경분)")
    print("\n🎉 준비 완료! 'MAIN_DEMO_SCRIPT.py'를 실행하여 시연을 시작하세요.")

if __name__ == "__main__":
    try:
        import torch
    except ImportError:
        print("❌ 'torch'가 필요합니다. 'pip install torch'로 설치해주세요.")
        sys.exit(1)
    
    setup()
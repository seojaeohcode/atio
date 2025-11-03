#!/usr/bin/env python3

"""
[발표 시연용 메인 스크립트]

- Act 1: atio.write (원자적 쓰기) - Pandas CSV 손상 및 보호 시연
- Act 2: atio.write_snapshot (데이터 스냅샷) - Tag, Delete, Revert 시연
- Act 3: atio.write_model_snapshot (모델 스냅샷) - 저장 공간 절약 및 복원 시연
"""

import os
import shutil
import multiprocessing
import time
import pandas as pd
import numpy as np
import polars as pl

# --- 모듈 경로 설정을 위한 코드 ---
try:
    import atio
except ImportError:
    print("Error: 'atio' 라이브러리를 찾을 수 없습니다.")
    import sys
    project_root = os.path.dirname(os.path.abspath(__file__))
    src_path = os.path.join(project_root, 'src')
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    try:
        import atio
    except ImportError:
        sys.exit("atio 모듈 로드에 실패했습니다.")
# ------------------------------------

# Act 2 헬퍼를 위한 Rich 임포트
try:
    from rich.console import Console
    from rich.table import Table
    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    print("Info: 'rich' 라이브러리가 없으면 Act 2의 스냅샷 목록이 간단하게 출력됩니다.")

# --- 공통 헬퍼 ---
def print_header(title):
    print("\n" + "=" * 70)
    print(f"🧪 {title}")
    print("=" * 70)

def pause_for_user(message):
    print("\n" + "-" * 50)
    print(f"🔵 [발표자 확인] {message}")
    print("  (확인 후 Enter 키를 누르면 다음 단계로 진행합니다...)")
    input()
    print("-" * 50)

def cleanup_all(dirs):
    print("\n" + "=" * 70)
    print("🧹 모든 시연 디렉토리 정리 중...")
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)
    print("✅ 정리 완료.")

# ========== Act 1: atio.write (원자적 쓰기) ==========

DEMO_DIR_ACT1 = "ATIO_DEMO_ACT1_ATOMIC"
PANDAS_UNSAFE_FILE = os.path.join(DEMO_DIR_ACT1, "1_pandas_UNSAFE.csv")
PANDAS_SAFE_FILE = os.path.join(DEMO_DIR_ACT1, "2_pandas_SAFE_atio.csv")

# 5x5 원본 데이터
data_5x5 = np.arange(1, 26).reshape(5, 5)
df_pd_original = pd.DataFrame(data_5x5, columns=['A', 'B', 'C', 'D', 'E'])

# 대용량 수정본 데이터
try:
    large_pd_df = pd.DataFrame(np.random.randn(5_000_000, 10))
except MemoryError:
    large_pd_df = pd.DataFrame(np.random.randn(1_000_000, 5))

def setup_act1():
    shutil.rmtree(DEMO_DIR_ACT1, ignore_errors=True)
    os.makedirs(DEMO_DIR_ACT1, exist_ok=True)
    
    df_pd_original.to_csv(PANDAS_UNSAFE_FILE, index=False)
    df_pd_original.to_csv(PANDAS_SAFE_FILE, index=False)
    print(f"✅ Act 1 준비 완료. '{os.path.abspath(DEMO_DIR_ACT1)}' 폴더 생성됨.")

def task_unsafe_pandas(path, data):
    print(f"  [PID: {os.getpid()}] (Unsafe) Pandas 쓰기 시작...")
    data.to_csv(path, index=False)

def task_safe_atio(path, data, format, **kwargs):
    print(f"  [PID: {os.getpid()}] (Safe) atio.write 시작...")
    atio.write(data, path, format=format, verbose=True, **kwargs)

def demo_act_1_atomic_write():
    print_header("Act 1: atio.write (원자적 쓰기) 시연")
    setup_act1()
    
    # --- 1-1. Unsafe (Pandas) ---
    pause_for_user(f"'{os.path.basename(PANDAS_UNSAFE_FILE)}' 파일을 뷰어로 열어 '정상 (5x5)'임을 확인하세요.")
    print(f"  [Main] 'Unsafe Pandas' 쓰기 프로세스를 50ms 후 강제 종료합니다...")
    p_unsafe_pd = multiprocessing.Process(target=task_unsafe_pandas, args=(PANDAS_UNSAFE_FILE, large_pd_df))
    p_unsafe_pd.start()
    time.sleep(0.05)
    p_unsafe_pd.terminate()
    p_unsafe_pd.join()
    print(f"  [Main] 💥 'Unsafe Pandas' 프로세스 강제 종료 완료.")
    pause_for_user(f"'{os.path.basename(PANDAS_UNSAFE_FILE)}' 파일을 뷰어로 다시 열어 '손상'되었는지(0바이트 등) 확인하세요.")

    # --- 1-2. Safe (atio) ---
    pause_for_user(f"'{os.path.basename(PANDAS_SAFE_FILE)}' 파일을 뷰어로 열어 '정상 (5x5)'임을 확인하세요.")
    print(f"  [Main] 'Safe atio (csv)' 쓰기 프로세스를 50ms 후 강제 종료합니다...")
    p_safe_pd = multiprocessing.Process(target=task_safe_atio, args=(PANDAS_SAFE_FILE, large_pd_df, "csv"), kwargs={"index": False})
    p_safe_pd.start()
    time.sleep(0.05)
    p_safe_pd.terminate()
    p_safe_pd.join()
    print(f"  [Main] 💥 'Safe atio' 프로세스 강제 종료 완료.")
    pause_for_user(f"'{os.path.basename(PANDAS_SAFE_FILE)}' 파일을 뷰어로 다시 열어 '완벽하게 보존'되었는지 확인하세요.")

# ========== Act 2: atio.write_snapshot (Tag, Delete, Revert) ==========

DEMO_DIR_ACT2 = "ATIO_DEMO_ACT2_SNAPSHOT"

def list_snapshots_helper(table_path):
    """(신규) 스냅샷 목록을 cli.py처럼 예쁘게 출력하는 헬퍼 함수"""
    print("\n--- [스냅샷 목록] ---")
    snapshots = atio.list_snapshots(table_path)
    
    if not RICH_AVAILABLE:
        for s in snapshots:
            tags = f" (Tags: {', '.join(s['tags'])})" if s['tags'] else ""
            latest = " (LATEST)" if s['is_latest'] else ""
            print(f"  v{s['version_id']}: {s['message']}{tags}{latest}")
        return

    table = Table(title=f"History for '{os.path.basename(table_path)}'")
    table.add_column("Ver", style="cyan")
    table.add_column("Latest", style="magenta")
    table.add_column("Tags", style="green")
    table.add_column("Message")

    for s in snapshots:
        table.add_row(
            str(s['version_id']),
            "✅" if s['is_latest'] else "",
            ", ".join(s['tags']),
            s['message']
        )
    console.print(table)


def setup_act2():
    shutil.rmtree(DEMO_DIR_ACT2, ignore_errors=True)
    
    # v1: 5x5 원본
    v1_data = df_pd_original
    
    # v2: 13번 값을 999로 수정
    v2_data = v1_data.copy()
    v2_data.at[2, 'C'] = 999
    
    # v3: 'NEW_COL' 추가
    v3_data = v1_data.copy()
    v3_data['NEW_COL'] = ['a', 'b', 'c', 'd', 'e']
    
    print("  > v1, v2, v3 스냅샷 순차 생성 중...")
    atio.write_snapshot(v1_data, DEMO_DIR_ACT2, message="v1: Base 5x5 data")
    pause_for_user("스냅샷 v1이 생성되었습니다.")
    atio.write_snapshot(v2_data, DEMO_DIR_ACT2, message="v2: Added 999")
    pause_for_user("스냅샷 v2이 생성되었습니다.")
    # atio.write_snapshot(v3_data, DEMO_DIR_ACT2, message="v3: Added NEW_COL")
    # pause_for_user("스냅샷 v3이 생성되었습니다.")
    print("✅ Act 2 준비 완료.")

def demo_act_2_data_snapshot():
    print_header("Act 2: atio.write_snapshot (Tag, Delete, Revert) 시연")
    setup_act2()

    pause_for_user("v1, v2(값 수정) 스냅샷을 생성했습니다. (v2가 최신)")
    list_snapshots_helper(DEMO_DIR_ACT2)
    
    # --- 1. Tag ---
    pause_for_user("v2에 태그(final_release)를 지정합니다. [atio.tag_version]")
    atio.tag_version(DEMO_DIR_ACT2, version_id=2, tag_name="final_release")
    list_snapshots_helper(DEMO_DIR_ACT2)

    # --- 2. Delete ---
    pause_for_user("v1는 삭제합니다. [atio.delete_version]")
    atio.delete_version(DEMO_DIR_ACT2, version_id=1)
    print("\n  ✅ v1 삭제 완료!")
    list_snapshots_helper(DEMO_DIR_ACT2)

    # # --- 3. Revert ---
    # pause_for_user("v1을 'revert'하여 v4를 생성합니다. [atio.revert]")
    # atio.revert(DEMO_DIR_ACT2, version_id_to_revert=1, message="Revert to v1 state")
    # print("\n  ✅ v1의 상태로 v4 생성 완료!")
    # list_snapshots_helper(DEMO_DIR_ACT2)

    # pause_for_user("Revert 후 '최신' 버전을 다시 읽습니다.")
    # latest_v4 = atio.read_table(DEMO_DIR_ACT2)
    # print("--- [Revert 후 최신 v4] --- (v1과 동일) ---")
    # print(latest_v4)
    # # print("\n  (v4가 최신이 되었고, 내용은 v1과 동일한 5x5 데이터인 것을 확인)")

# ========== Act 3: atio.write_model_snapshot (공간 절약) ==========

MODEL_SNAPSHOT_PATH = "ATIO_MODEL_SNAPSHOT" # [준비] 스크립트가 생성한 폴더
DEMO_DIR_ACT3_RESTORE = "ATIO_DEMO_ACT3_RESTORE"

def setup_act3():
    shutil.rmtree(DEMO_DIR_ACT3_RESTORE, ignore_errors=True)
    os.makedirs(DEMO_DIR_ACT3_RESTORE, exist_ok=True)
    
    if not os.path.exists(MODEL_SNAPSHOT_PATH):
        print("="*70)
        print("❌ [치명적 오류] Act 3 시연 준비가 안 됐습니다!")
        print(f"  > '{MODEL_SNAPSHOT_PATH}' 폴더를 찾을 수 없습니다.")
        print("  > 'prepare_model_demo.py' 스크립트를 먼저 실행해주세요.")
        print("="*70)
        return False
    
    print(f"✅ Act 3 준비 완료. '{MODEL_SNAPSHOT_PATH}' 폴더 확인됨.")
    return True

def demo_act_3_model_snapshot():
    print_header("Act 3: atio.write_model_snapshot (공간 절약) 시연")
    if not setup_act3():
        return # 준비 안 됐으면 중단

    # 1. 공간 절약 확인 (시각적)
    pause_for_user(f"'{MODEL_SNAPSHOT_PATH}' 폴더를 확인.")
    
    print("  [설명] 준비 스크립트가 500MB(v1) + 505MB(v2) = 총 1,005MB의 모델을 생성했습니다.")
    print("         하지만 atio 스냅샷 폴더의 'data' 폴더 용량을 확인해보세요.")
    print("  > (예상) 약 505MB~510MB일 것입니다. (v1 전체 + v2 변경분 1%)")
    print("  > `atio`가 중복을 제거하고 변경된 부분만 저장하여 500MB를 절약했습니다.")

    # 2. 복원 시연
    pause_for_user("이제 이 작은 스냅샷에서 v1과 v2를 모두 복원합니다. [atio.read_model_snapshot]")
    
    restore_v1_path = os.path.join(DEMO_DIR_ACT3_RESTORE, "RESTORED_v1.pth")
    restore_v2_path = os.path.join(DEMO_DIR_ACT3_RESTORE, "RESTORED_v2.pth")
    
    print("  > v1 복원 중...")
    atio.read_model_snapshot(MODEL_SNAPSHOT_PATH, version=1, mode='restore', 
                             destination_path=restore_v1_path, show_progress=True)
    
    print("  > v2 복원 중...")
    atio.read_model_snapshot(MODEL_SNAPSHOT_PATH, version=2, mode='restore', 
                             destination_path=restore_v2_path, show_progress=True)
    
    print("  ✅ v1, v2 복원 완료!")
    pause_for_user(f"'{DEMO_DIR_ACT3_RESTORE}' 폴더를 확인하세요. 500MB짜리 v1과 v2가 모두 복원되었습니다.")


# ========== 메인 실행 ==========

def main():
    all_demo_dirs = [DEMO_DIR_ACT1, DEMO_DIR_ACT2, DEMO_DIR_ACT3_RESTORE, 
                     "ATIO_MODEL_DEMO_PREP", "ATIO_MODEL_SNAPSHOT"]
    
    try:
        # Act 1: 원자적 쓰기 (파일 손상 방지)
        demo_act_1_atomic_write()
        
        # Act 2: 데이터 스냅샷 (Tag, Delete, Revert)
        demo_act_2_data_snapshot()
        
        # Act 3: 모델 스냅샷 (공간 절약)
        demo_act_3_model_snapshot()

    except Exception as e:
        print(f"\n" + "!" * 70)

if __name__ == "__main__":
    main()
    # cleanup_all([
    #     DEMO_DIR_ACT1,
    #     DEMO_DIR_ACT2,
    #     DEMO_DIR_ACT3_RESTORE,
    #     "ATIO_MODEL_DEMO_PREP",
    #     "ATIO_MODEL_SNAPSHOT"
    # ])
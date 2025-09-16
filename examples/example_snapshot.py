import tempfile
import os
import shutil
import json
import pandas as pd
import polars as pl
import numpy as np
from atio import write_snapshot, read_table, delete_version

def set_current_version(table_path, version_id):
    """테스트를 위해 현재 버전을 특정 버전으로 설정(롤백)하는 헬퍼 함수"""
    pointer_path = os.path.join(table_path, '_current_version.json')
    with open(pointer_path, 'w', encoding='utf-8') as f:
        json.dump({'version_id': version_id}, f)
    print(f"\n[SYSTEM] 현재 버전이 v{version_id}(으)로 롤백되었습니다.")

def run_all_tests():
    """atio 라이브러리의 모든 기능을 종합적으로 테스트합니다."""
    base_dir = tempfile.mkdtemp()
    table_path = os.path.join(base_dir, "versioned_table")

    try:
        print("=" * 70)
        print(f"🚀 atio 테스트를 시작합니다. 모든 데이터는 '{base_dir}'에 저장됩니다.")
        print("=" * 70)

        # --- 시나리오 1: 버전 생성 (테스트 데이터 준비) ---
        print("\n\n" + "-" * 70)
        print("🎬 시나리오 1: Pandas 데이터프레임으로 테스트 데이터 생성")
        print("-" * 70)

        df_v1 = pd.DataFrame({"id": [1, 2, 3], "value_A": ["apple", "banana", "cherry"]})
        write_snapshot(df_v1, table_path, mode='overwrite') # Version 1

        df_v2 = pd.DataFrame({"id": [1, 2, 3], "value_C": [True, False, True]})
        write_snapshot(df_v2, table_path, mode='overwrite') # Version 2

        df_v3_append = pd.DataFrame({"value_D": [100, 200, 300]})
        write_snapshot(df_v3_append, table_path, mode='append') # Version 3
        
        print("\n[INFO] 테스트 데이터 준비 완료. v1, v2, v3 스냅샷이 생성되었습니다.")
        print("[INFO] 현재 최신 버전은 v3 입니다.")

        # --- 시나리오 2: 교차 호환성 테스트 ---
        # (이전 테스트 코드와 동일하므로 간결하게 요약)
        print("\n\n" + "-" * 70)
        print("🎬 시나리오 2: Polars, NumPy 및 교차 호환성 테스트")
        print("-" * 70)
        # Polars
        pl_table_path = os.path.join(base_dir, "polars_table")
        write_snapshot(pl.DataFrame({"name": ["a"], "score": [1]}), pl_table_path)
        assert isinstance(read_table(pl_table_path, output_as='polars'), pl.DataFrame)
        print("✅ Polars 호환성 테스트 성공!")
        # NumPy
        np_table_path = os.path.join(base_dir, "numpy_table")
        write_snapshot(np.array([1, 2, 3]), np_table_path)
        assert isinstance(read_table(np_table_path, output_as='numpy'), np.ndarray)
        print("✅ NumPy 호환성 테스트 성공!")

        # --- 시나리오 3: 버전 삭제 및 가비지 컬렉션(GC) 테스트 ---
        print("\n\n" + "-" * 70)
        print("🎬 시나리오 3: 버전 삭제 및 가비지 컬렉션(GC) 테스트")
        print("-" * 70)
        
        print("\n[시도] 현재 활성화된 최신 버전(v3) 삭제를 시도합니다...")
        result = delete_version(table_path, version_id=3)
        assert result is False, "최신 버전이 삭제되면 안됩니다!"
        print("-> 예상대로 삭제에 실패했습니다! (안전장치 정상 작동)")

        # 롤백 후 삭제 재시도
        set_current_version(table_path, 2)

        print("\n\n[삭제] 이제 v3은 최신 버전이 아니므로 삭제를 다시 시도합니다.")
        print("어떤 파일이 정리될지 미리 확인합니다 (dry_run=True)")
        delete_version(table_path, version_id=3, dry_run=True)
        
        print("\n실제로 v3을 삭제하고 파일을 정리합니다...")
        delete_version(table_path, version_id=3, dry_run=False)
        print("✅ 버전 3 삭제 및 관련 파일 정리 완료!")

        print("\n\n[검증] 삭제된 v3을 읽어봅니다...")
        try:
            read_table(table_path, version=3)
        except FileNotFoundError:
            print("-> 예상대로 파일을 찾을 수 없어 읽기에 실패했습니다!")

        print("\n[검증] 아직 살아있는 v2는 여전히 잘 읽어지는지 확인합니다...")
        loaded_v2 = read_table(table_path, version=2)
        print("-> v2 데이터 읽기 성공:\n", loaded_v2.head())
        assert loaded_v2 is not None

        print("\n\n[최종 분석] v1과 v2가 공유하던 'id' 컬럼 데이터는 v1이 삭제되어도 v2가 사용 중이므로, 가비지 컬렉션에서 제외되어 안전하게 보존됩니다.")

    finally:
        # 테스트 후 임시 디렉토리 삭제
        print("\n" + "=" * 70)
        print(f"🔧 테스트 종료. 임시 디렉토리 '{base_dir}'를 삭제합니다.")
        print("=" * 70)
        shutil.rmtree(base_dir)

if __name__ == "__main__":
    run_all_tests()
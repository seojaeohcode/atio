#!/usr/bin/env python3

# Copyright 2025 Atio Developers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Atio Data Snapshot 기능 테스트 스크립트
(write_snapshot, read_table, rollback, delete_version, export_to_datalake)
"""

# --- 모듈 경로 설정을 위한 코드 ---
import sys
import os
import shutil
import time

# 현재 스크립트의 경로를 기준으로 'src' 폴더의 절대 경로를 계산합니다.
project_root = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ------------------------------------

import atio
import pandas as pd
import numpy as np
import polars as pl
import pyarrow as pa
# sqlalchemy는 이 파일에서 필요 없음

# --- 전역 테스트 설정 ---
TEST_OUTPUT_DIR = "atio_data_snapshot_tests" # 디렉토리 이름 변경
DATA_SNAPSHOT_PATH = os.path.join(TEST_OUTPUT_DIR, "data_table")
# 모델 관련 경로는 필요 없음

# --- 헬퍼 함수 ---
def print_test_header(title):
    print("\n" + "=" * 60)
    print(f"🧪 테스트 시작: {title}")
    print("=" * 60)

def print_test_result(success, message=""):
    if success:
        print(f"  ✅ [성공] {message}")
    else:
        print(f"  ❌ [실패] {message}")

def setup_test_environment():
    """테스트 실행 전 테스트 디렉토리를 정리하고 생성합니다."""
    print(f"테스트 환경 설정: '{TEST_OUTPUT_DIR}' 디렉토리 정리 및 생성...")
    if os.path.exists(TEST_OUTPUT_DIR):
        shutil.rmtree(TEST_OUTPUT_DIR)
    os.makedirs(TEST_OUTPUT_DIR, exist_ok=True)
    # 모델 복원 디렉토리 생성은 필요 없음
    print("✅ 테스트 환경 준비 완료.")

# --- 데이터 스냅샷 테스트 함수 ---

def test_data_snapshot_lifecycle():
    """데이터 스냅샷의 전체 생명주기(생성, 추가, 읽기, 롤백, 삭제, 내보내기)를 테스트합니다."""
    print_test_header("데이터 스냅샷 생명주기 (종합 테스트)")
    try:
        # # --- write_snapshot (overwrite) ---
        # print("\n  [1] write_snapshot (overwrite) - 다양한 타입")

        # # Pandas DataFrame
        # df_pd = pd.DataFrame({'id': [1, 2], 'pd_val': ['a', 'b']})
        # atio.write_snapshot(df_pd, DATA_SNAPSHOT_PATH)
        # print_test_result(True, "v1 (Pandas, 2 rows) 생성")

        # # Polars DataFrame
        # df_pl = pl.DataFrame({'id': [1, 2], 'pl_val': [True, False]})
        # atio.write_snapshot(df_pl, DATA_SNAPSHOT_PATH)
        # print_test_result(True, "v2 (Polars, 2 rows) 생성")

        # # NumPy Array
        # arr_np = np.array([[1.1, 1.2], [2.1, 2.2]])
        # atio.write_snapshot(arr_np, DATA_SNAPSHOT_PATH)
        # print_test_result(True, "v3 (NumPy, 2 rows) 생성")

        # # PyArrow Table
        # arr_pa = pa.Table.from_pydict({'id': [10], 'pa_val': ['arrow']})
        # atio.write_snapshot(arr_pa, DATA_SNAPSHOT_PATH)
        # print_test_result(True, "v4 (Arrow, 1 row) 생성")

        # # --- 3. read_table() 테스트 ---
        # print("\n  [3] read_table() - output_as 옵션 및 내용 확인") # 제목 수정

        # # v1(Pandas) -> Pandas
        # read_v1 = atio.read_table(DATA_SNAPSHOT_PATH, version=1, output_as='pandas')
        # assert isinstance(read_v1, pd.DataFrame) and 'pd_val' in read_v1.columns
        # print_test_result(True, "v1(Pandas) -> output_as='pandas' 읽기 성공")
        # print("  --- v1 (Pandas) 내용 ---")
        # print(read_v1) # 읽어온 Pandas DataFrame 출력
        # print("  -------------------------")

        # # v2(Polars) -> Polars
        # read_v2 = atio.read_table(DATA_SNAPSHOT_PATH, version=2, output_as='polars')
        # assert isinstance(read_v2, pl.DataFrame) and 'pl_val' in read_v2.columns
        # print_test_result(True, "v2(Polars) -> output_as='polars' 읽기 성공")
        # print("  --- v2 (Polars) 내용 ---")
        # print(read_v2) # 읽어온 Polars DataFrame 출력
        # print("  -------------------------")

        # # v3(NumPy) -> NumPy
        # read_v3 = atio.read_table(DATA_SNAPSHOT_PATH, version=3, output_as='numpy')
        # assert isinstance(read_v3, np.ndarray) and read_v3.shape == (2, 2)
        # print_test_result(True, "v3(NumPy) -> output_as='numpy' 읽기 성공")
        # print("  --- v3 (NumPy) 내용 ---")
        # print(read_v3) # 읽어온 NumPy Array 출력
        # print("  ------------------------")

        # # v4(Arrow) -> Arrow
        # read_v4 = atio.read_table(DATA_SNAPSHOT_PATH, version=4, output_as='arrow')
        # assert isinstance(read_v4, pa.Table) and 'pa_val' in read_v4.schema.names
        # print_test_result(True, "v4(Arrow) -> output_as='arrow' 읽기 성공")
        # print("  --- v4 (Arrow) 내용 ---")
        # print(read_v4) # 읽어온 PyArrow Table 출력
        # print("  ------------------------")


        # # Pandas DataFrame V1 생성
        # df_pd = pd.DataFrame({'id': [1, 2], 'pd_val': ['a', 'b']})
        # atio.write_snapshot(df_pd, DATA_SNAPSHOT_PATH)
        # print_test_result(True, "v1 (Pandas, 2 rows) 생성")

        # # --- 2. write_snapshot (append) --- V1에 열 추가하여 저장
        # print("\n  [2] write_snapshot (mode='append')")
        # df_append = pd.DataFrame({'appended_col': [100, 200]})
        # atio.write_snapshot(df_append, DATA_SNAPSHOT_PATH, mode='append')
        # print_test_result(True, "v2 (Append, 2 rows) 생성")

        # read_v1 = atio.read_table(DATA_SNAPSHOT_PATH, version=1, output_as='pandas')
        # assert isinstance(read_v1, pd.DataFrame) and 'pd_val' in read_v1.columns
        # print_test_result(True, "v1(Pandas) -> output_as='pandas' 읽기 성공")
        # print("  --- v1 (Pandas) 내용 ---")
        # print(read_v1) # 읽어온 Pandas DataFrame 출력
        # print("  -------------------------")

        # read_v2 = atio.read_table(DATA_SNAPSHOT_PATH, version=2, output_as='pandas')
        # assert isinstance(read_v2, pd.DataFrame) and 'pd_val' in read_v2.columns
        # print_test_result(True, "v2(Pandas) -> output_as='pandas' 읽기 성공")
        # print("  --- v2 (Pandas) 내용 ---")
        # print(read_v2) # 읽어온 Pandas DataFrame 출력
        # print("  -------------------------")


        # 테스트용 스냅샷 4개 생성 rollback 테스트

        # # NumPy Array
        # arr_np = np.array([[1.1, 1.2], [2.1, 2.2]])
        # atio.write_snapshot(arr_np, DATA_SNAPSHOT_PATH)
        # print_test_result(True, "v3 (NumPy, 2 rows) 생성")

        # # PyArrow Table
        # arr_pa = pa.Table.from_pydict({'id': [10], 'pa_val': ['arrow']})
        # atio.write_snapshot(arr_pa, DATA_SNAPSHOT_PATH)
        # print_test_result(True, "v4 (Arrow, 1 row) 생성")

        # # --- 4. rollback() ---
        # print("\n  [4] rollback() - v4 -> v2 롤백")
        # atio.rollback(DATA_SNAPSHOT_PATH, version_id=2)
        # read_after_rollback = atio.read_table(DATA_SNAPSHOT_PATH, output_as='numpy')
        # assert read_after_rollback.shape == (2, 2)
        # print_test_result(True, "v2 롤백 성공 (최신 버전이 v2가 됨)")

        # # --- 5. delete_version() ---
        # print("\n  [5] delete_version() - v4 삭제")
        # delete_result = atio.delete_version(DATA_SNAPSHOT_PATH, version_id=4)
        # assert delete_result is True
        # print_test_result(True, "v4 (구버전) 삭제 성공")
        # try:
        #     atio.read_table(DATA_SNAPSHOT_PATH, version=4)
        #     print_test_result(False, "삭제된 v4 읽기 시도 (오류가 발생해야 함)")
        # except FileNotFoundError:
        #     print_test_result(True, "삭제된 v4 읽기 시도 (예상대로 FileNotFoundError 발생)")
        # except Exception:
        #     print_test_result(True, "삭제된 v4 읽기 시도 (예상대로 오류 발생)")
        # print("\n  [5-2] delete_version() - v2 (최신) 삭제 시도")
        # delete_latest_result = atio.delete_version(DATA_SNAPSHOT_PATH, version_id=2)
        # assert delete_latest_result is False
        # print_test_result(True, "v2 (최신 버전) 삭제 시도")
        

        # Pandas DataFrame
        df_pd = pd.DataFrame({'id': [1, 2], 'pd_val': ['a', 'b']})
        atio.write_snapshot(df_pd, DATA_SNAPSHOT_PATH)
        print_test_result(True, "v1 (Pandas, 2 rows) 생성")

        # Polars DataFrame
        df_pl = pl.DataFrame({'id': [1, 2], 'pl_val': [True, False]})
        atio.write_snapshot(df_pl, DATA_SNAPSHOT_PATH)
        print_test_result(True, "v2 (Polars, 2 rows) 생성")

        # --- 6. export_to_datalake() ---
        print("\n  [6] export_to_datalake() - v2(Polars) 내보내기")
        export_path = os.path.join(TEST_OUTPUT_DIR, "datalake_export.parquet")
        atio.export_to_datalake(DATA_SNAPSHOT_PATH, version=2, output_path=export_path)
        assert os.path.exists(export_path)
        print_test_result(True, f"Parquet 파일 생성 완료: {export_path}")

    except Exception as e:
        print_test_result(False, f"데이터 스냅샷 테스트 중 예기치 않은 오류: {e}")
        import traceback
        traceback.print_exc()

# --- 파일 정리 함수 ---
def cleanup_demo_files():
    """테스트 실행 후 생성된 파일들을 정리합니다."""
    print("\n" + "=" * 60)
    print("🧹 테스트 파일 정리")
    print("=" * 60)
    if not os.path.exists(TEST_OUTPUT_DIR):
        print(f"🗑️ 정리할 디렉토리가 없습니다: '{TEST_OUTPUT_DIR}'")
        return
    print(f"🗑️ 생성된 테스트 디렉토리: '{TEST_OUTPUT_DIR}'")
    print("\n❓ 테스트 디렉토리를 삭제하시겠습니까? (y/n): ", end="")
    try:
        response = input().lower().strip()
    except (EOFError, KeyboardInterrupt):
        response = 'n'
        print("\n입력 없이 종료하여 파일을 보존합니다.")
    if response == 'y':
        try:
            shutil.rmtree(TEST_OUTPUT_DIR)
            print(f"\n✅ '{TEST_OUTPUT_DIR}' 디렉토리와 모든 내용이 삭제되었습니다.")
        except Exception as e:
            print(f"\n❌ 디렉토리 삭제 중 오류 발생: {e}")
    else:
        print(f"\n📁 '{TEST_OUTPUT_DIR}' 디렉토리가 보존되었습니다.")

# --- 메인 실행 함수 ---
def main():
    """데이터 스냅샷 기능 테스트 실행 함수"""
    try:
        setup_test_environment()
        # --- 데이터 스냅샷 테스트 호출 ---
        test_data_snapshot_lifecycle()
    except Exception as e:
        print(f"\n" + "!" * 60)
        print(f" CRITICAL: 테스트 실행 중 치명적인 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        print("!" * 60)
    finally:
        cleanup_demo_files()
    print("\n" + "=" * 60)
    print("🎉 Atio Data Snapshot 기능 테스트 완료!")
    print("=" * 60)

if __name__ == "__main__":
    main()
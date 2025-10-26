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
Atio Atomic Write (`atio.write`) 기능 테스트 스크립트
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
from sqlalchemy import create_engine

# --- 전역 테스트 설정 ---
TEST_OUTPUT_DIR = "atio_atomic_write_tests" # 디렉토리 이름 변경

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
    # 모델 관련 디렉토리 생성은 필요 없음
    print("✅ 테스트 환경 준비 완료.")

# --- atio.write() 테스트 함수들 ---

def test_write_pandas():
    """atio.write() - Pandas DataFrame 지원 포맷 테스트"""
    print_test_header("atio.write() - Pandas")
    try:
        df = pd.DataFrame({
            "A": [1, 2, 3],
            "B": ["foo", "bar", "baz"],
            "C": [0.1, 0.2, 0.3]
        })
        formats_to_test = ['csv', 'parquet', 'json', 'pickle', 'html', 'xlsx']
        for fmt in formats_to_test:
            try:
                target_path = os.path.join(TEST_OUTPUT_DIR, f"pd_test.{fmt}")
                kwargs = {}
                format_to_write = fmt
                if fmt == 'xlsx':
                    kwargs['engine'] = 'openpyxl'
                    format_to_write = 'excel'
                atio.write(df, target_path, format=format_to_write, **kwargs)
                print_test_result(True, f"format='{format_to_write}' (.{fmt}) 저장")
            except Exception as e:
                print_test_result(False, f"format='{format_to_write}' (.{fmt}) 저장 중 오류: {e}")
        # SQL 테스트
        try:
            engine = create_engine('sqlite:///:memory:')
            atio.write(df, format="sql", name='pd_test_table', con=engine, index=False)
            with engine.connect() as conn:
                read_df = pd.read_sql("SELECT * FROM pd_test_table", conn)
            assert len(read_df) == 3
            print_test_result(True, "format='sql' 저장 (in-memory)")
        except Exception as e:
            print_test_result(False, f"format='sql' 저장 중 오류: {e}")
    except Exception as e:
        print(f"Pandas 테스트 중 예기치 않은 오류: {e}")

def test_write_polars():
    """atio.write() - Polars DataFrame 지원 포맷 테스트"""
    print_test_header("atio.write() - Polars")
    try:
        df = pl.DataFrame({
            "X": [10, 20, 30],
            "Y": [True, False, True]
        })
        formats_to_test = ['csv', 'parquet', 'json', 'ipc', 'avro']
        for fmt in formats_to_test:
            try:
                target_path = os.path.join(TEST_OUTPUT_DIR, f"pl_test.{fmt}")
                atio.write(df, target_path, format=fmt)
                print_test_result(True, f"format='{fmt}' 저장")
            except Exception as e:
                print_test_result(False, f"format='{fmt}' 저장 중 오류: {e}")
    except Exception as e:
        print(f"Polars 테스트 중 예기치 않은 오류: {e}")

def test_write_numpy():
    """atio.write() - NumPy Array 지원 포맷 테스트"""
    print_test_header("atio.write() - NumPy")
    try:
        arr_1d = np.array([1, 2, 3, 4, 5])
        arr_2d = np.random.rand(5, 3)
        arr_dict = {'a': arr_1d, 'b': arr_2d}
        atio.write(arr_2d, os.path.join(TEST_OUTPUT_DIR, "np_test.npy"), "npy")
        print_test_result(True, "format='npy' 저장")
        atio.write(arr_dict, os.path.join(TEST_OUTPUT_DIR, "np_test.npz"), "npz")
        print_test_result(True, "format='npz' 저장")
        atio.write(arr_dict, os.path.join(TEST_OUTPUT_DIR, "np_test_comp.npz"), "npz_compressed")
        print_test_result(True, "format='npz_compressed' 저장")
        atio.write(arr_2d, os.path.join(TEST_OUTPUT_DIR, "np_test.csv"), "csv")
        print_test_result(True, "format='csv' 저장")
        atio.write(arr_1d.astype(np.float32), os.path.join(TEST_OUTPUT_DIR, "np_test.bin"), "bin")
        print_test_result(True, "format='bin' 저장")
    except Exception as e:
        print(f"  ❌ NumPy 테스트 중 예기치 않은 오류: {e}")

def test_write_options():
    """atio.write() - show_progress 및 verbose 옵션 테스트"""
    print_test_header("atio.write() - 옵션 (show_progress, verbose)")
    try:
        print("\n  [1] show_progress=True 테스트 (진행도 표시줄이 나타나야 함):")
        large_df = pd.DataFrame(np.random.randn(100000000, 5), columns=list("ABCDE"))
        atio.write(large_df, os.path.join(TEST_OUTPUT_DIR, "large.parquet"), "parquet", show_progress=True)
        print_test_result(True, "show_progress=True 실행 완료 (시각적 확인 필요)")
        print("\n  [2] verbose=True 테스트 (상세 로그 출력):")
        small_df = pd.DataFrame({"id": [1]})
        atio.write(small_df, os.path.join(TEST_OUTPUT_DIR, "verbose.csv"), "csv", verbose=True)
        print_test_result(True, "verbose=True 실행 완료")
    except Exception as e:
        print(f"옵션 테스트 중 예기치 않은 오류: {e}")

def test_write_database():
    """atio.write() - 데이터베이스 쓰기 (Pandas, Polars) 테스트 및 내용 확인"""
    print_test_header("atio.write() - 데이터베이스 쓰기 (sql, database)")

    # 1. Pandas (sql) - in-memory
    try:
        pd_df = pd.DataFrame({"pd_col": [1, 2]})
        engine_memory = create_engine('sqlite:///:memory:')
        atio.write(pd_df, format="sql", name='pd_sql_test', con=engine_memory)

        # ==================== DB에 저장된 내용 출력 ==========================
        with engine_memory.connect() as conn:
             read_pd_df = pd.read_sql_table('pd_sql_test', conn)
        print("\n  --- Pandas (in-memory) DB 저장 내용 ---")
        print(read_pd_df)
        print("  --------------------------------------")
        print_test_result(True, "Pandas format='sql' (in-memory) 저장 및 내용 출력")
    except Exception as e:
        print_test_result(False, f"Pandas format='sql' 저장 중 오류: {e}")

    # 2. Polars (database) - file-based
    db_path = os.path.join(TEST_OUTPUT_DIR, "polars.db")
    engine_uri = f"sqlite:///{db_path}"

    try:
        pl_df = pl.DataFrame({"pl_col": [True, False]})

        # --- 쓰기 ---
        atio.write(pl_df, format="database", table_name='pl_db_test',
                   connection_uri=engine_uri, connection=engine_uri)
        print_test_result(True, "Polars format='database' (file-based) 저장")

        # ==================== DB에 저장된 내용 출력 ==========================
        print("\n  --- Polars (file-based) DB 저장 내용 ---")
        # 파일 DB에 연결
        verify_engine = create_engine(engine_uri)
        read_back_df_pd = None
        with verify_engine.connect() as conn:
            # Pandas를 사용하여 DB에서 데이터 읽기
            read_back_df_pd = pd.read_sql_table('pl_db_test', conn)

        # 읽어온 DataFrame 출력
        print(read_back_df_pd)
        print("  --------------------------------------")
        print_test_result(True, "Polars DB 내용 읽기 및 출력 완료")
        # --- 출력 끝 ---

    except Exception as e:
        # 쓰기 또는 읽기 단계에서 오류 발생 시
        print_test_result(False, f"Polars format='database' 저장 또는 읽기 중 오류: {e}")
        import traceback
        traceback.print_exc()

# 파일 정리
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

def main():

    try:
        setup_test_environment()
        # --- atio.write() 테스트 호출
        # test_write_pandas()
        # test_write_polars()
        # test_write_numpy()
        # test_write_options()
        test_write_database()
    except Exception as e:
        print(f"\n" + "=" * 60)
        print(f"테스트 실행 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        print("=" * 60)
    finally:
        cleanup_demo_files()
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
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
Atio Core 기능 테스트 스크립트
core.py의 주요 기능들(write, snapshot, model_snapshot 등)을 테스트합니다.
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
from sqlalchemy import create_engine

# --- 선택적 라이브러리 임포트 (모델 테스트용) ---
try:
    import torch
    _TORCH_AVAILABLE = True
except ImportError:
    _TORCH_AVAILABLE = False
    print("⚠️  PyTorch가 설치되지 않았습니다. 모델 스냅샷(PyTorch) 테스트를 건너뜁니다.")

try:
    import tensorflow as tf
    _TENSORFLOW_AVAILABLE = True
except ImportError:
    _TENSORFLOW_AVAILABLE = False
    print("⚠️  TensorFlow가 설치되지 않았습니다. 모델 스냅샷(TensorFlow) 테스트를 건너뜁니다.")


# --- 전역 테스트 설정 ---
TEST_OUTPUT_DIR = "atio_functional_tests"
DATA_SNAPSHOT_PATH = os.path.join(TEST_OUTPUT_DIR, "data_table")
MODEL_SNAPSHOT_PATH = os.path.join(TEST_OUTPUT_DIR, "model_table")
MODEL_RESTORE_PATH = os.path.join(TEST_OUTPUT_DIR, "model_restore")

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
    print(f"🔧 테스트 환경 설정: '{TEST_OUTPUT_DIR}' 디렉토리 정리 및 생성...")
    if os.path.exists(TEST_OUTPUT_DIR):
        shutil.rmtree(TEST_OUTPUT_DIR)
    os.makedirs(TEST_OUTPUT_DIR, exist_ok=True)
    os.makedirs(MODEL_RESTORE_PATH, exist_ok=True)
    print("✅ 테스트 환경 준비 완료.")

def create_dummy_models():
    """테스트에 사용할 더미 모델 파일(.pth, saved_model)을 생성합니다."""
    print("🔧 더미 모델 파일 생성 중...")
    
    # 1. PyTorch 더미 모델 생성
    if _TORCH_AVAILABLE:
        try:
            pth_path = os.path.join(TEST_OUTPUT_DIR, "dummy_model_v1.pth")
            state_dict = {'layer1.weight': torch.randn(10, 5), 'layer1.bias': torch.randn(10)}
            torch.save(state_dict, pth_path)
            print(f"  ✅ PyTorch 더미 모델 생성 완료: {pth_path}")
        except Exception as e:
            print(f"  ❌ PyTorch 더미 모델 생성 실패: {e}")

    # 2. TensorFlow 더미 모델 생성
    if _TENSORFLOW_AVAILABLE:
        try:
            tf_path = os.path.join(TEST_OUTPUT_DIR, "dummy_model_tf_v1")
            
            class SimpleModule(tf.Module):
                def __init__(self, name=None):
                    super().__init__(name=name)
                    self.v = tf.Variable(5.0, name="v1")
                @tf.function
                def __call__(self, x):
                    return self.v * x
            
            model = SimpleModule()
            tf.saved_model.save(model, tf_path)
            print(f"  ✅ TensorFlow 더미 모델 생성 완료: {tf_path}")
        except Exception as e:
            print(f"  ❌ TensorFlow 더미 모델 생성 실패: {e}")

# --- 1. atio.write() 테스트 ---

def test_write_pandas():
    """atio.write() - Pandas DataFrame 지원 포맷 테스트"""
    print_test_header("atio.write() - Pandas")
    try:
        df = pd.DataFrame({
            "A": [1, 2, 3],
            "B": ["foo", "bar", "baz"],
            "C": [0.1, 0.2, 0.3]
        })
        
        # 테스트할 포맷 리스트 (sql은 별도 처리)
        # (수정) 'excel' 포맷의 대상 경로를 '.xlsx'로 변경
        formats_to_test = ['csv', 'parquet', 'json', 'pickle', 'html', 'xlsx']
        
        for fmt in formats_to_test:
            try:
                # (수정) 파일명 포맷을 fmt 그대로 사용 (pd_test.xlsx)
                target_path = os.path.join(TEST_OUTPUT_DIR, f"pd_test.{fmt}")
                kwargs = {}
                format_to_write = fmt
                if fmt == 'xlsx':
                    # openpyxl 필요, format은 'excel'로 지정
                    kwargs['engine'] = 'openpyxl'
                    format_to_write = 'excel'
                    
                atio.write(df, target_path, format=format_to_write, **kwargs)
                print_test_result(True, f"format='{format_to_write}' (.{fmt}) 저장")
            except Exception as e:
                print_test_result(False, f"format='{format_to_write}' (.{fmt}) 저장 중 오류: {e}")
        
        # SQL 테스트 (in-memory)
        try:
            engine = create_engine('sqlite:///:memory:')
            atio.write(df, format="sql", name='pd_test_table', con=engine, index=False)
            # 검증
            with engine.connect() as conn:
                read_df = pd.read_sql("SELECT * FROM pd_test_table", conn)
            assert len(read_df) == 3
            print_test_result(True, "format='sql' 저장 (in-memory)")
        except Exception as e:
            print_test_result(False, f"format='sql' 저장 중 오류: {e}")
            
    except Exception as e:
        print(f"  ❌ Pandas 테스트 중 예기치 않은 오류: {e}")

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
        print(f"  ❌ Polars 테스트 중 예기치 않은 오류: {e}")

def test_write_numpy():
    """atio.write() - NumPy Array 지원 포맷 테스트"""
    print_test_header("atio.write() - NumPy")
    try:
        arr_1d = np.array([1, 2, 3, 4, 5])
        arr_2d = np.random.rand(5, 3)
        arr_dict = {'a': arr_1d, 'b': arr_2d}

        # npy (1D/2D)
        atio.write(arr_2d, os.path.join(TEST_OUTPUT_DIR, "np_test.npy"), "npy")
        print_test_result(True, "format='npy' 저장")

        # npz
        atio.write(arr_dict, os.path.join(TEST_OUTPUT_DIR, "np_test.npz"), "npz")
        print_test_result(True, "format='npz' 저장")
        
        # npz_compressed
        atio.write(arr_dict, os.path.join(TEST_OUTPUT_DIR, "np_test_comp.npz"), "npz_compressed")
        print_test_result(True, "format='npz_compressed' 저장")

        # csv (1D 또는 2D)
        atio.write(arr_2d, os.path.join(TEST_OUTPUT_DIR, "np_test.csv"), "csv")
        print_test_result(True, "format='csv' 저장")

        # bin (1D)
        atio.write(arr_1d.astype(np.float32), os.path.join(TEST_OUTPUT_DIR, "np_test.bin"), "bin")
        print_test_result(True, "format='bin' 저장")

    except Exception as e:
        print(f"  ❌ NumPy 테스트 중 예기치 않은 오류: {e}")

def test_write_options():
    """atio.write() - show_progress 및 verbose 옵션 테스트"""
    print_test_header("atio.write() - 옵션 (show_progress, verbose)")
    try:
        # 1. show_progress=True 테스트
        print("\n  [1] show_progress=True 테스트 (진행도 표시줄이 나타나야 함):")
        large_df = pd.DataFrame(np.random.randn(1000000, 5), columns=list("ABCDE"))
        atio.write(large_df, os.path.join(TEST_OUTPUT_DIR, "large.parquet"), "parquet", show_progress=True)
        print_test_result(True, "show_progress=True 실행 완료 (시각적 확인 필요)")

        # 2. verbose=True 테스트
        print("\n  [2] verbose=True 테스트 (상세 로그가 출력되어야 함):")
        small_df = pd.DataFrame({"id": [1]})
        atio.write(small_df, os.path.join(TEST_OUTPUT_DIR, "verbose.csv"), "csv", verbose=True)
        print_test_result(True, "verbose=True 실행 완료 (로그 확인 필요)")

    except Exception as e:
        print(f"  ❌ 옵션 테스트 중 예기치 않은 오류: {e}")

def test_write_database():
    """atio.write() - 데이터베이스 쓰기 (Pandas, Polars) 테스트"""
    print_test_header("atio.write() - 데이터베이스 쓰기 (sql, database)")
    
    # 1. Pandas (sql) - in-memory
    try:
        pd_df = pd.DataFrame({"pd_col": [1, 2]})
        engine = create_engine('sqlite:///:memory:')
        atio.write(pd_df, format="sql", name='pd_sql_test', con=engine)
        print_test_result(True, "Pandas format='sql' (in-memory) 저장")
    except Exception as e:
        print_test_result(False, f"Pandas format='sql' 저장 중 오류: {e}")

    # 2. Polars (database) - file-based (connection_uri)
    try:
        pl_df = pl.DataFrame({"pl_col": [True, False]})
        db_path = os.path.join(TEST_OUTPUT_DIR, "polars.db")
        engine_uri = f"sqlite:///{db_path}" 
        
        # (수정) Polars는 'connection' 인자를, core.py의 검증 로직은 'connection_uri'를 기대
        # 두 인자를 모두 전달하여 core.py의 검증과 polars의 실행을 모두 만족시킴
        atio.write(pl_df, format="database", table_name='pl_db_test', 
                   connection_uri=engine_uri, connection=engine_uri)
        print_test_result(True, "Polars format='database' (file-based) 저장")
    except Exception as e:
        print_test_result(False, f"Polars format='database' 저장 중 오류: {e}")


# --- 2. 데이터 스냅샷 테스트 (write_snapshot, read_table, rollback, delete, export) ---

def test_data_snapshot_lifecycle():
    """데이터 스냅샷의 전체 생명주기(생성, 추가, 읽기, 롤백, 삭제, 내보내기)를 테스트합니다."""
    print_test_header("데이터 스냅샷 생명주기 (종합 테스트)")
    
    try:
        # --- 1. write_snapshot (overwrite) 테스트 ---
        print("\n  [1] write_snapshot (overwrite) - 다양한 타입")
        
        # v1: Pandas
        df_pd = pd.DataFrame({'id': [1, 2], 'pd_val': ['a', 'b']})
        atio.write_snapshot(df_pd, DATA_SNAPSHOT_PATH)
        print_test_result(True, "v1 (Pandas, 2 rows) 생성")

        # v2: Polars
        df_pl = pl.DataFrame({'id': [1, 2], 'pl_val': [True, False]})
        atio.write_snapshot(df_pl, DATA_SNAPSHOT_PATH)
        print_test_result(True, "v2 (Polars, 2 rows) 생성")

        # v3: NumPy
        arr_np = np.array([[1.1, 1.2], [2.1, 2.2]])
        atio.write_snapshot(arr_np, DATA_SNAPSHOT_PATH)
        print_test_result(True, "v3 (NumPy, 2 rows) 생성")

        # v4: Arrow
        arr_pa = pa.Table.from_pydict({'id': [10], 'pa_val': ['arrow']})
        atio.write_snapshot(arr_pa, DATA_SNAPSHOT_PATH)
        print_test_result(True, "v4 (Arrow, 1 row) 생성")

        # --- 2. write_snapshot (append) 테스트 ---
        print("\n  [2] write_snapshot (mode='append')")
        
        # (수정) v4 (1 row)에 append 하므로, 1 row짜리 데이터프레임을 생성해야 함
        df_append = pd.DataFrame({'appended_col': [100]})
        
        # v4에 'appended_col'을 추가하여 v5 생성
        atio.write_snapshot(df_append, DATA_SNAPSHOT_PATH, mode='append')
        print_test_result(True, "v5 (Append, 1 row) 생성")

        # --- 3. read_table() 테스트 ---
        print("\n  [3] read_table() - output_as 옵션")
        
        # v1(Pandas) -> Pandas
        read_v1 = atio.read_table(DATA_SNAPSHOT_PATH, version=1, output_as='pandas')
        assert isinstance(read_v1, pd.DataFrame) and 'pd_val' in read_v1.columns
        print_test_result(True, "v1(Pandas) -> output_as='pandas' 읽기")

        # v2(Polars) -> Polars
        read_v2 = atio.read_table(DATA_SNAPSHOT_PATH, version=2, output_as='polars')
        assert isinstance(read_v2, pl.DataFrame) and 'pl_val' in read_v2.columns
        print_test_result(True, "v2(Polars) -> output_as='polars' 읽기")

        # v3(NumPy) -> NumPy
        read_v3 = atio.read_table(DATA_SNAPSHOT_PATH, version=3, output_as='numpy')
        assert isinstance(read_v3, np.ndarray) and read_v3.shape == (2, 2)
        print_test_result(True, "v3(NumPy) -> output_as='numpy' 읽기")
        
        # v4(Arrow) -> Arrow
        read_v4 = atio.read_table(DATA_SNAPSHOT_PATH, version=4, output_as='arrow')
        assert isinstance(read_v4, pa.Table) and 'pa_val' in read_v4.schema.names
        print_test_result(True, "v4(Arrow) -> output_as='arrow' 읽기")

        # v5(Append) -> Pandas
        read_v5 = atio.read_table(DATA_SNAPSHOT_PATH, version=5, output_as='pandas')
        # (수정) v5는 1 row 여야 함
        assert 'pa_val' in read_v5.columns and 'appended_col' in read_v5.columns and len(read_v5) == 1
        print_test_result(True, "v5(Append) 읽기 (v4 + appended_col)")
        
        # 최신 버전(v5) 읽기
        read_latest = atio.read_table(DATA_SNAPSHOT_PATH, version=None, output_as='pandas')
        assert 'appended_col' in read_latest.columns
        print_test_result(True, "최신 버전 (v5) 읽기 (version=None)")


        # --- 4. rollback() 테스트 ---
        print("\n  [4] rollback() - v5 -> v3 롤백")
        # Pre-condition: 현재 v5
        atio.rollback(DATA_SNAPSHOT_PATH, version_id=3)
        read_after_rollback = atio.read_table(DATA_SNAPSHOT_PATH, output_as='numpy')
        assert read_after_rollback.shape == (2, 2)
        print_test_result(True, "v3 롤백 성공 (최신 버전이 v3가 됨)")

        # --- 5. delete_version() 테스트 ---
        print("\n  [5] delete_version() - v5 삭제")
        # Pre-condition: 현재 v3이므로 v5는 최신이 아님 -> 삭제 가능
        delete_result = atio.delete_version(DATA_SNAPSHOT_PATH, version_id=5)
        assert delete_result is True
        print_test_result(True, "v5 (비활성 버전) 삭제 성공")
        
        # 삭제된 v5 읽기 시도 (실패해야 정상)
        try:
            atio.read_table(DATA_SNAPSHOT_PATH, version=5)
            print_test_result(False, "삭제된 v5 읽기 시도 (오류가 발생해야 함)")
        except FileNotFoundError:
            print_test_result(True, "삭제된 v5 읽기 시도 (예상대로 FileNotFoundError 발생)")
        except Exception:
            # core.py 구현에 따라 다른 오류가 날 수도 있음
            print_test_result(True, "삭제된 v5 읽기 시도 (예상대로 오류 발생)")
            
        # [Pre condition] 최신 버전(v3) 삭제 시도 (실패해야 정상)
        print("\n  [5-2] delete_version() - v3 (최신) 삭제 시도")
        delete_latest_result = atio.delete_version(DATA_SNAPSHOT_PATH, version_id=3)
        assert delete_latest_result is False
        print_test_result(True, "v3 (활성 버전) 삭제 시도 (예상대로 거부됨)")

        # --- 6. export_to_datalake() 테스트 ---
        print("\n  [6] export_to_datalake() - v2(Polars) 내보내기")
        export_path = os.path.join(TEST_OUTPUT_DIR, "datalake_export.parquet")
        atio.export_to_datalake(DATA_SNAPSHOT_PATH, version=2, output_path=export_path)
        assert os.path.exists(export_path)
        print_test_result(True, f"Parquet 파일 생성 완료: {export_path}")

    except Exception as e:
        print_test_result(False, f"데이터 스냅샷 테스트 중 예기치 않은 오류: {e}")
        # 오류 발생 시에도 테스트가 계속 진행되도록 함 (필요시)
        import traceback
        traceback.print_exc()


# --- 3. 모델 스냅샷 테스트 (write_model_snapshot, read_model_snapshot, rollback, delete) ---

def test_model_snapshot_lifecycle():
    """모델 스냅샷의 전체 생명주기(생성, 읽기, 롤백, 삭제)를 테스트합니다."""
    print_test_header("모델 스냅샷 생명주기 (종합 테스트)")
    
    if not _TORCH_AVAILABLE and not _TENSORFLOW_AVAILABLE:
        print("  ⚠️  Torch와 TensorFlow가 모두 없어 모델 스냅샷 테스트를 건너뜁니다.")
        return

    try:
        # --- 1. write_model_snapshot (PyTorch) ---
        if _TORCH_AVAILABLE:
            print("\n  [1] write_model_snapshot (PyTorch)")
            pth_path = os.path.join(TEST_OUTPUT_DIR, "dummy_model_v1.pth")
            if not os.path.exists(pth_path):
                print_test_result(False, "PyTorch 더미 모델 파일 없음. (create_dummy_models 실패?)")
                return

            atio.write_model_snapshot(pth_path, MODEL_SNAPSHOT_PATH, show_progress=True)
            print_test_result(True, "v1 (PyTorch) 생성")
        else:
            print("\n  [1] write_model_snapshot (PyTorch) - 건너뜀")

        # --- 2. write_model_snapshot (TensorFlow) ---
        if _TENSORFLOW_AVAILABLE:
            print("\n  [2] write_model_snapshot (TensorFlow)")
            tf_path = os.path.join(TEST_OUTPUT_DIR, "dummy_model_tf_v1")
            if not os.path.exists(tf_path):
                print_test_result(False, "TensorFlow 더미 모델 파일 없음. (create_dummy_models 실패?)")
                return
            
            atio.write_model_snapshot(tf_path, MODEL_SNAPSHOT_PATH, show_progress=True)
            print_test_result(True, "v2 (TensorFlow) 생성")
        else:
            print("\n  [2] write_model_snapshot (TensorFlow) - 건너뜀")

        # --- 3. read_model_snapshot (mode='auto') ---
        print("\n  [3] read_model_snapshot (mode='auto')")
        if _TORCH_AVAILABLE:
            try:
                model_v1 = atio.read_model_snapshot(MODEL_SNAPSHOT_PATH, version=1, mode='auto')
                assert isinstance(model_v1, dict) and 'layer1.weight' in model_v1
                print_test_result(True, "v1 (PyTorch) 'auto' 모드 로딩 성공")
            except Exception as e:
                print_test_result(False, f"v1 (PyTorch) 'auto' 로딩 실패: {e}")
        
        if _TENSORFLOW_AVAILABLE:
            try:
                # v2가 있어야 함 (torch만 테스트 시 v2가 없음)
                if not os.path.exists(os.path.join(MODEL_SNAPSHOT_PATH, 'metadata', 'v2.metadata.json')):
                     print("  - v2 (TensorFlow) 스냅샷이 없어 'auto' 읽기 테스트를 건너뜁니다.")
                else:
                    model_v2 = atio.read_model_snapshot(MODEL_SNAPSHOT_PATH, version=2, mode='auto')
                    assert "v" in dir(model_v2) # SimpleModule의 'v' 변수 확인
                    print_test_result(True, "v2 (TensorFlow) 'auto' 모드 로딩 성공")
            except Exception as e:
                print_test_result(False, f"v2 (TensorFlow) 'auto' 로딩 실패: {e}")

        # --- 4. read_model_snapshot (mode='restore') ---
        print("\n  [4] read_model_snapshot (mode='restore')")
        if _TORCH_AVAILABLE:
            try:
                restore_pth_path = os.path.join(MODEL_RESTORE_PATH, "restored_v1.pth")
                atio.read_model_snapshot(MODEL_SNAPSHOT_PATH, version=1, mode='restore', destination_path=restore_pth_path, show_progress=True)
                assert os.path.exists(restore_pth_path)
                print_test_result(True, f"v1 (PyTorch) 'restore' 모드 복원 성공: {restore_pth_path}")
            except Exception as e:
                print_test_result(False, f"v1 (PyTorch) 'restore' 복원 실패: {e}")
        
        if _TENSORFLOW_AVAILABLE:
            try:
                if not os.path.exists(os.path.join(MODEL_SNAPSHOT_PATH, 'metadata', 'v2.metadata.json')):
                    print("  - v2 (TensorFlow) 스냅샷이 없어 'restore' 읽기 테스트를 건너뜁니다.")
                else:
                    restore_tf_path = os.path.join(MODEL_RESTORE_PATH, "restored_v2_tf")
                    atio.read_model_snapshot(MODEL_SNAPSHOT_PATH, version=2, mode='restore', destination_path=restore_tf_path, show_progress=True)
                    assert os.path.exists(os.path.join(restore_tf_path, "saved_model.pb"))
                    print_test_result(True, f"v2 (TensorFlow) 'restore' 모드 복원 성공: {restore_tf_path}")
            except Exception as e:
                print_test_result(False, f"v2 (TensorFlow) 'restore' 복원 실패: {e}")

        # --- 5. rollback() 테스트 ---
        print("\n  [5] rollback() - v2 -> v1 롤백")
        if _TORCH_AVAILABLE and _TENSORFLOW_AVAILABLE:
            # Pre-condition: 현재 v2
            atio.rollback(MODEL_SNAPSHOT_PATH, version_id=1)
            model_after_rollback = atio.read_model_snapshot(MODEL_SNAPSHOT_PATH, mode='auto')
            assert isinstance(model_after_rollback, dict) # PyTorch(v1)인지 확인
            print_test_result(True, "v1 롤백 성공 (최신 버전이 v1가 됨)")
        else:
            print("  - 롤백 테스트는 PyTorch와 TensorFlow 스냅샷이 모두 필요하므로 건너뜁니다.")

        # --- 6. delete_version() 테스트 ---
        print("\n  [6] delete_version() - v2 삭제")
        if _TORCH_AVAILABLE and _TENSORFLOW_AVAILABLE:
            # Pre-condition: 현재 v1이므로 v2는 최신이 아님 -> 삭제 가능
            delete_result = atio.delete_version(MODEL_SNAPSHOT_PATH, version_id=2)
            assert delete_result is True
            print_test_result(True, "v2 (비활성 버전) 삭제 성공")
            
            # 삭제된 v2 읽기 시도 (실패해야 정상)
            try:
                atio.read_model_snapshot(MODEL_SNAPSHOT_PATH, version=2, mode='auto')
                print_test_result(False, "삭제된 v2 읽기 시도 (오류가 발생해야 함)")
            except FileNotFoundError:
                print_test_result(True, "삭제된 v2 읽기 시도 (예상대로 FileNotFoundError 발생)")
            except Exception:
                print_test_result(True, "삭제된 v2 읽기 시도 (예상대로 오류 발생)")
        else:
            print("  - 삭제 테스트는 PyTorch와 TensorFlow 스냅샷이 모두 필요하므로 건너뜁니다.")

    except Exception as e:
        print_test_result(False, f"모델 스냅샷 테스트 중 예기치 않은 오류: {e}")
        import traceback
        traceback.print_exc()

def cleanup_demo_files():
    """데모 실행 후 생성된 파일들을 정리합니다."""
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
    """
    메인 기능 테스트 실행 함수
    Atio의 모든 주요 기능을 순차적으로 테스트합니다.
    """
    try:
        setup_test_environment()
        create_dummy_models()

        # --- atio.write() 테스트 ---
        test_write_pandas()
        test_write_polars()
        test_write_numpy()
        test_write_options()
        test_write_database()

        # --- 데이터 스냅샷 테스트 ---
        test_data_snapshot_lifecycle()
        
        # --- 모델 스냅샷 테스트 ---
        test_model_snapshot_lifecycle()

    except Exception as e:
        print(f"\n" + "!" * 60)
        print(f" CRITICAL: 테스트 실행 중 치명적인 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        print("!" * 60)
    finally:
        # 파일 정리
        cleanup_demo_files()
    
    print("\n" + "=" * 60)
    print("🎉 Atio 기능 테스트 완료!")
    print("=" * 60)

if __name__ == "__main__":
    main()
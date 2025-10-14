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

import pandas as pd
import numpy as np
import pytest
import tempfile
import shutil
import os
import json

# 테스트 대상 함수들을 atio 라이브러리에서 가져옵니다.
from atio import write_snapshot, read_table, delete_version, rollback

@pytest.fixture
def table_dir():
    """각 테스트를 위한 임시 디렉토리를 생성하고 테스트 종료 후 삭제하는 Fixture"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir  # 테스트 함수에 임시 디렉토리 경로를 전달
    shutil.rmtree(temp_dir) # 테스트 종료 후 디렉토리 정리

def test_create_and_read_versions(table_dir):
    """
    버전 생성(overwrite, append)과 특정 버전 읽기 기능을 테스트합니다.
    """
    # v1: 덮어쓰기로 첫 데이터 생성
    df1 = pd.DataFrame({'date': ['2025-08-11'], 'sales': [100]})
    write_snapshot(df1, table_dir, mode='overwrite')

    # v2: append 모드로 데이터 추가 (기존 v1 데이터에 df2 데이터가 열 기준으로 합쳐져야 함)
    # v1은 2열, v2는 2열 -> 합쳐서 4열이 되어야 함
    df2_append = pd.DataFrame({'store': ['A'], 'manager': ['Kim']})
    write_snapshot(df2_append, table_dir, mode='append')
    
    # v3: 덮어쓰기로 v2 상태를 무시하고 새로 생성
    df3 = pd.DataFrame({'item': ['apple', 'banana'], 'price': [10, 20]})
    write_snapshot(df3, table_dir)

    # --- 검증 ---
    # 최신 버전(v3) 읽기
    latest_df = read_table(table_dir)
    assert latest_df.equals(df3), "최신 버전(v3) 데이터가 일치하지 않습니다."
    
    # 과거 버전(v1) 읽기
    v1_df = read_table(table_dir, version=1)
    assert v1_df.equals(df1), "과거 버전(v1) 데이터가 일치하지 않습니다."
    
    # append로 생성된 v2 읽기
    v2_df = read_table(table_dir, version=2)
    expected_v2 = pd.concat([df1, df2_append], axis=1)
    assert v2_df.equals(expected_v2), "Append 모드로 생성된 v2 데이터가 일치하지 않습니다."

def test_data_deduplication(table_dir):
    """
    내용이 동일한 컬럼은 중복 저장되지 않아 공간이 절약되는지 테스트합니다.
    """
    data_path = os.path.join(table_dir, 'data')

    # v1 생성
    df1 = pd.DataFrame({'id': range(100), 'value': ['A'] * 100})
    write_snapshot(df1, table_dir)
    assert len(os.listdir(data_path)) == 2 # 'id'와 'value' 2개 파일 생성

    # v2 생성 (id 컬럼은 v1과 동일)
    df2 = pd.DataFrame({'id': range(100), 'new_value': ['B'] * 100})
    write_snapshot(df2, table_dir)
    
    # --- 검증 ---
    # 'id' 컬럼 파일은 재사용하고 'new_value' 파일만 추가되어 총 3개여야 함
    assert len(os.listdir(data_path)) == 3, "데이터 중복 제거 기능이 작동하지 않았습니다."

def test_rollback_and_delete_version(table_dir):
    """
    롤백, 버전 삭제, 가비지 컬렉션(GC) 기능을 테스트합니다.
    """
    # 테스트용 버전 3개 생성
    write_snapshot(pd.DataFrame({'a': [1]}), table_dir)  # v1
    write_snapshot(pd.DataFrame({'b': [2]}), table_dir)  # v2
    write_snapshot(pd.DataFrame({'c': [3]}), table_dir)  # v3

    # --- 1. 최신 버전 삭제 시도 (실패해야 정상) ---
    assert not delete_version(table_dir, version_id=3), "최신 버전을 삭제할 수 없어야 합니다."

    # --- 2. v2로 롤백 ---
    assert rollback(table_dir, version_id=2), "롤백에 실패했습니다."
    # _current_version.json 파일 확인
    with open(os.path.join(table_dir, '_current_version.json')) as f:
        current_ver = json.load(f)['version_id']
    assert current_ver == 2, "롤백 후 현재 버전이 올바르지 않습니다."

    # --- 3. 이제 최신이 아닌 v3 삭제 (성공해야 정상) ---
    assert delete_version(table_dir, version_id=3), "v3 삭제에 실패했습니다."
    
    # --- 4. 삭제된 버전 읽기 시도 (실패해야 정상) ---
    with pytest.raises(FileNotFoundError):
        read_table(table_dir, version=3)
        
    # --- 5. 남아있는 v2는 정상적으로 읽어져야 함 ---
    df2_read = read_table(table_dir, version=2)
    assert df2_read is not None, "삭제 후 남아있는 버전을 읽을 수 없습니다."
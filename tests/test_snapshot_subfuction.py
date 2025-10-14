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
import pytest
import tempfile
import shutil
import os
import json

from atio import write_snapshot, read_table, revert, list_snapshots, tag_version

@pytest.fixture
def table_dir():
    """각 테스트를 위한 임시 디렉토리를 생성하고 테스트 종료 후 삭제하는 Fixture"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir  # 테스트 함수에 임시 디렉토리 경로를 전달
    shutil.rmtree(temp_dir) # 테스트 종료 후 디렉토리 정리

def test_revert_and_log_functionality(table_dir):
    """
    revert 기능으로 과거 버전을 새 버전으로 생성하고, 
    list_snapshots(로그) 기능이 모든 변경 이력을 정확히 추적하는지 테스트합니다.
    """
    # --- 1. 테스트를 위한 버전 이력 생성 ---
    df1 = pd.DataFrame({'a': [1, 2]})
    write_snapshot(df1, table_dir, message="v1: Initial commit")

    df2 = pd.DataFrame({'b': ['x', 'y']})
    write_snapshot(df2, table_dir, message="v2: Add feature 'b'")
    
    df3 = pd.DataFrame({'c': [True, False]})
    write_snapshot(df3, table_dir, message="v3: Add feature 'c'")

    # --- 2. revert 기능 테스트 ---
    # v1의 상태를 가져와 새로운 버전(v4)으로 생성합니다.
    revert_message = "Revert to the state of v1"
    result = revert(table_dir, version_id_to_revert=1, message=revert_message)
    
    assert result is True, "revert 작업에 실패했습니다."

    # --- 3. 검증 ---
    # 3-1. revert로 생성된 최신 버전(v4)의 데이터가 v1과 동일한지 확인
    v4_df = read_table(table_dir) # version을 지정하지 않으면 최신 버전을 읽음
    v1_df = read_table(table_dir, version=1)
    
    pd.testing.assert_frame_equal(v4_df, v1_df, "Revert된 데이터(v4)가 원본(v1)과 일치하지 않습니다.")

    # 3-2. list_snapshots (로그) 기능이 모든 이력을 정확히 보여주는지 확인
    snapshots = list_snapshots(table_dir)
    
    assert len(snapshots) == 4, "Revert 후 총 버전 개수가 4개가 아닙니다."
    
    # 각 버전의 상세 정보 검증
    v1_info, v2_info, v3_info, v4_info = snapshots

    assert v1_info['version_id'] == 1
    assert "Initial commit" in v1_info['message']
    assert v1_info['is_latest'] is False

    assert v3_info['version_id'] == 3
    assert "Add feature 'c'" in v3_info['message']
    assert v3_info['is_latest'] is False

    assert v4_info['version_id'] == 4
    assert v4_info['message'] == revert_message, "Revert 커밋 메시지가 올바르지 않습니다."
    assert v4_info['is_latest'] is True, "Revert로 생성된 버전이 최신 버전으로 표시되어야 합니다."

def test_log_with_tags(table_dir):
    """
    버전에 태그를 지정하고, list_snapshots가 태그 정보를 정확히 보여주는지 테스트합니다.
    """
    # --- 1. 테스트를 위한 버전 생성 ---
    write_snapshot(pd.DataFrame({'data': [100]}), table_dir, message="Initial data") # v1
    write_snapshot(pd.DataFrame({'data': [200]}), table_dir, message="Update data")  # v2

    # --- 2. 태그 지정 ---
    assert tag_version(table_dir, version_id=1, tag_name="stable-release"), "v1에 태그 지정 실패"
    assert tag_version(table_dir, version_id=2, tag_name="latest-dev"), "v2에 태그 지정 실패"
    
    # 이미 존재하는 태그를 다른 버전에 재지정
    assert tag_version(table_dir, version_id=1, tag_name="latest-dev"), "태그 재지정 실패"

    # --- 3. 검증 ---
    snapshots = list_snapshots(table_dir)
    assert len(snapshots) == 2

    v1_info, v2_info = snapshots

    # v1은 'stable-release'와 'latest-dev' 두 태그를 가져야 함 (알파벳 순 정렬)
    assert v1_info['tags'] == ['latest-dev', 'stable-release'], "v1의 태그 정보가 올바르지 않습니다."
    
    # v2는 이제 아무 태그도 없어야 함
    assert v2_info['tags'] == [], "v2의 태그 정보가 올바르지 않습니다."
    
    # --- 4. 태그로 버전 읽기 테스트 ---
    df_tagged = read_table(table_dir, version="stable-release")
    df_v1 = read_table(table_dir, version=1)
    pd.testing.assert_frame_equal(df_tagged, df_v1, "태그로 읽은 데이터가 버전 ID로 읽은 데이터와 다릅니다.")
# #!/usr/bin/env python3

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

# """
# 다양한 오류 상황에서 성능 진단 로깅 테스트 (개선된 버전)
# """

# import pandas as pd
# import numpy as np
# import os
# import time
# import pytest
# import threading
# import signal
# from unittest.mock import patch
# from atio import write

# # --- 성공적인 쓰기 테스트를 위한 기본 데이터 ---
# @pytest.fixture
# def sample_df():
#     """테스트용 샘플 데이터프레임을 생성하는 Fixture"""
#     return pd.DataFrame({'A': [1, 2], 'B': [3, 4]})

# # --- 각 오류 상황에 대한 테스트 함수 ---

# def test_unsupported_format(sample_df, tmp_path):
#     """지원하지 않는 형식에 대해 ValueError가 발생하는지 테스트"""
#     print("\n=== 지원하지 않는 형식 오류 테스트 ===")
    
#     # 'xyz'라는 지원하지 않는 형식을 사용할 때 ValueError가 발생하는지 확인
#     with pytest.raises(ValueError, match="Unsupported format: 'xyz'"):
#         write(sample_df, tmp_path / 'test.xyz', format='xyz', verbose=True)
#     print("✅ 지원하지 않는 형식에 대한 예외 발생 확인")

# def test_permission_error(sample_df, tmp_path):
#     """권한 없는 디렉토리에 쓸 때 PermissionError가 발생하는지 테스트"""
#     print("\n=== 권한 오류 테스트 ===")
    
#     # 쓰기 권한이 없는 디렉토리 생성
#     read_only_dir = tmp_path / "read_only"
#     read_only_dir.mkdir()
#     os.chmod(read_only_dir, 0o444)  # 읽기 전용으로 권한 변경

#     # 권한 오류(PermissionError)가 발생하는지 확인
#     with pytest.raises(PermissionError):
#         write(sample_df, read_only_dir / 'test.parquet', format='parquet', verbose=True)
    
#     # 테스트 후 권한 복원 (필수는 아니지만 좋은 습관)
#     os.chmod(read_only_dir, 0o755)
#     print("✅ 권한 없는 경로에 대한 예외 발생 확인")

# @patch('pandas.DataFrame.to_parquet')
# def test_disk_full_error(mock_to_parquet, sample_df, tmp_path):
#     """디스크 공간 부족(OSError) 상황을 모킹하여 테스트"""
#     print("\n=== 디스크 공간 부족 오류 테스트 (모킹) ===")

#     # to_parquet 함수가 OSError를 발생시키도록 설정
#     mock_to_parquet.side_effect = OSError(28, "No space left on device")
    
#     with pytest.raises(OSError, match="No space left on device"):
#         write(sample_df, tmp_path / 'disk_full_test.parquet', format='parquet', verbose=True)
#     print("✅ 디스크 공간 부족 예외 발생 확인")

# @patch('atio.core.write_pandas')
# def test_memory_error(mock_write_pandas, sample_df, tmp_path):
#     """메모리 부족(MemoryError) 상황을 모킹하여 테스트"""
#     print("\n=== 메모리 부족 오류 테스트 (모킹) ===")
    
#     # 내부 쓰기 함수가 MemoryError를 발생시키도록 설정
#     mock_write_pandas.side_effect = MemoryError("Unable to allocate array with shape")

#     with pytest.raises(MemoryError):
#         write(sample_df, tmp_path / 'memory_error_test.parquet', format='parquet', verbose=True)
#     print("✅ 메모리 부족 예외 발생 확인")

# def test_concurrent_access_error(tmp_path):
#     """여러 스레드가 동일한 파일에 동시 접근 시 오류 테스트"""
#     print("\n=== 동시 접근 오류 테스트 ===")
    
#     target_file = tmp_path / "concurrent_test.parquet"
#     errors = []

#     def write_job():
#         try:
#             df = pd.DataFrame({'A': np.random.randn(100), 'B': np.random.randn(100)})
#             # 모든 스레드가 '같은' 파일에 쓰기 시도
#             write(df, target_file, format='parquet', verbose=False)
#         except Exception as e:
#             errors.append(e)

#     threads = [threading.Thread(target=write_job) for _ in range(5)]
#     for t in threads:
#         t.start()
#     for t in threads:
#         t.join()

#     # atio 라이브러리가 동시 쓰기를 어떻게 처리하는지에 따라 검증 내용이 달라짐
#     # 예: 파일 잠금(lock) 메커니즘이 없다면 오류가 발생할 수 있음
#     # 예: 메커니즘이 있다면 오류는 없어야 하고 파일은 하나만 정상적으로 생성되어야 함
#     assert target_file.exists()
#     print(f"✅ 동시 접근 테스트 완료 (발생한 오류 수: {len(errors)})")


# def test_keyboard_interrupt_cleanup(tmp_path):
#     """Ctrl+C(KeyboardInterrupt) 발생 시 임시 파일이 정리되는지 테스트"""
#     print("\n=== 키보드 인터럽트 오류 테스트 ===")
    
#     # 대용량 데이터를 준비하여 쓰기 작업이 오래 걸리도록 함
#     large_df = pd.DataFrame(np.random.rand(500000, 10))
#     target_file = tmp_path / "interrupt_test.parquet"

#     def simulate_interrupt():
#         # 쓰기 작업이 시작될 시간을 벌어줌
#         time.sleep(0.1)
#         os.kill(os.getpid(), signal.SIGINT)

#     # 별도 스레드에서 인터럽트 발생
#     interrupt_thread = threading.Thread(target=simulate_interrupt)
#     interrupt_thread.start()

#     try:
#         write(large_df, target_file, format='parquet', verbose=True)
#     except KeyboardInterrupt:
#         print("✔️ KeyboardInterrupt 정상적으로 발생")
    
#     interrupt_thread.join()
    
#     # 인터럽트 발생 후, atio의 원자적 쓰기 기능 덕분에
#     # 불완전한 파일이 남아있지 않아야 함
#     assert not target_file.exists()
#     print("✅ 인터럽트 후 대상 파일이 존재하지 않음을 확인 (정리 성공)")
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
# 라이브러리 기본 진단 로깅 테스트
# 테스트 파일에서는 출력하지 않고, 오직 라이브러리 자체의 로그만 확인
# """

# import pandas as pd
# import numpy as np
# import logging
# import io
# import sys
# from atio import write

# def capture_logs(func):
#     """로그를 캡처하는 데코레이터"""
#     def wrapper(*args, **kwargs):
#         # 로그 캡처 설정
#         log_capture = io.StringIO()
#         handler = logging.StreamHandler(log_capture)
#         handler.setLevel(logging.DEBUG)
        
#         # atomicwriter 로거에 핸들러 추가
#         logger = logging.getLogger('atomicwriter')
#         logger.addHandler(handler)
#         logger.setLevel(logging.DEBUG)
        
#         # 기존 핸들러 제거 (중복 방지)
#         for hdlr in logger.handlers[:-1]:
#             logger.removeHandler(hdlr)
        
#         try:
#             result = func(*args, **kwargs)
#             logs = log_capture.getvalue()
#             # 로그 분석 및 검증
#             analyze_logs(logs, func.__name__)
#             return result
#         finally:
#             # 핸들러 정리
#             logger.removeHandler(handler)
#             log_capture.close()
    
#     return wrapper

# @capture_logs
# def test_basic_success(tmp_path):
#     """기본 사용법에서 성공 시 진단 정보 확인"""
#     df = pd.DataFrame({
#         'A': np.random.randn(10000),
#         'B': np.random.randn(10000),
#     })

#     write(df, tmp_path/'test_basic_success.parquet', format='parquet')

# @capture_logs
# def test_verbose_success(tmp_path):
#     """verbose 모드에서 성공 시 진단 정보 확인"""
#     df = pd.DataFrame({
#         'A': np.random.randn(10000),
#         'B': np.random.randn(10000),
#     })

#     write(df, tmp_path/'test_verbose_success.parquet', format='parquet', verbose=True)

# @capture_logs
# def test_basic_error(tmp_path):
#     """기본 사용법에서 오류 시 진단 정보 확인"""
#     df = pd.DataFrame({
#         'A': np.random.randn(1000),
#         'B': np.random.randn(1000),
#     })
    
#     try:
#         write(df, tmp_path/'test_basic_error.xyz', format='xyz')
#     except Exception:
#         pass

# @capture_logs
# def test_verbose_error(tmp_path):
#     """verbose 모드에서 오류 시 진단 정보 확인"""
#     df = pd.DataFrame({
#         'A': np.random.randn(1000),
#         'B': np.random.randn(1000),
#     })
    
#     try:
#         write(df, tmp_path/'test_verbose_error.xyz', format='xyz', verbose=True)
#     except Exception:
#         pass

# @capture_logs
# def test_large_data_success(tmp_path):
#     """대용량 데이터에서 성공 시 진단 정보 확인"""
#     df = pd.DataFrame({
#         'A': np.random.randn(100000),  # 10만 행
#         'B': np.random.randn(100000),
#         'C': np.random.randn(100000),
#         'D': np.random.randn(100000),
#     })

#     write(df, tmp_path/'test_large_data_success.parquet', format='parquet')

# @capture_logs
# def test_large_data_verbose(tmp_path):
#     """대용량 데이터에서 verbose 모드 진단 정보 확인"""
#     df = pd.DataFrame({
#         'A': np.random.randn(100000),  # 10만 행
#         'B': np.random.randn(100000),
#         'C': np.random.randn(100000),
#         'D': np.random.randn(100000),
#     })

#     write(df, tmp_path/'test_large_data_verbose.parquet', format='parquet', verbose=True)

# def analyze_logs(logs, test_name):
#     """로그 분석 및 진단 정보 확인"""
#     print(f"\n=== {test_name} ===")
    
#     # 기본 진단 정보 확인
#     if "Atomic write completed successfully" in logs:
#         print("✅ 기본 성공 진단 정보 제공됨")
#     elif "Atomic write failed" in logs:
#         print("✅ 기본 실패 진단 정보 제공됨")
#     else:
#         print("❌ 기본 진단 정보 없음")
    
#     # 상세 진단 정보 확인
#     if "Atomic write step timings" in logs:
#         print("✅ 상세 성능 진단 정보 제공됨")
#     else:
#         print("❌ 상세 성능 진단 정보 없음")
    
#     # 시간 정보 확인
#     if "took" in logs:
#         print("✅ 소요 시간 정보 제공됨")
#     else:
#         print("❌ 소요 시간 정보 없음")
    
#     # 오류 유형 정보 확인
#     if "error:" in logs:
#         print("✅ 오류 유형 정보 제공됨")
    
#     # 단계별 정보 확인
#     if "setup=" in logs or "write_call=" in logs or "replace=" in logs:
#         print("✅ 단계별 성능 정보 제공됨")
    
#     return {
#         'basic_diagnostic': "Atomic write completed successfully" in logs or "Atomic write failed" in logs,
#         'detailed_diagnostic': "Atomic write step timings" in logs,
#         'time_info': "took" in logs,
#         'error_info': "error:" in logs,
#         'step_info': "setup=" in logs or "write_call=" in logs or "replace=" in logs
#     }


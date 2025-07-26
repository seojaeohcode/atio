#!/usr/bin/env python3
"""
라이브러리 기본 진단 로깅 테스트
테스트 파일에서는 출력하지 않고, 오직 라이브러리 자체의 로그만 확인
"""

import pandas as pd
import numpy as np
import logging
import io
import sys
from atomicwriter import write

def capture_logs(func):
    """로그를 캡처하는 데코레이터"""
    def wrapper(*args, **kwargs):
        # 로그 캡처 설정
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        
        # atomicwriter 로거에 핸들러 추가
        logger = logging.getLogger('atomicwriter')
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        
        # 기존 핸들러 제거 (중복 방지)
        for hdlr in logger.handlers[:-1]:
            logger.removeHandler(hdlr)
        
        try:
            result = func(*args, **kwargs)
            return result, log_capture.getvalue()
        finally:
            # 핸들러 정리
            logger.removeHandler(handler)
            log_capture.close()
    
    return wrapper

@capture_logs
def test_basic_success():
    """기본 사용법에서 성공 시 진단 정보 확인"""
    df = pd.DataFrame({
        'A': np.random.randn(10000),
        'B': np.random.randn(10000),
    })
    
    write(df, 'test_basic_success.parquet', format='parquet')
    return None

@capture_logs
def test_verbose_success():
    """verbose 모드에서 성공 시 진단 정보 확인"""
    df = pd.DataFrame({
        'A': np.random.randn(10000),
        'B': np.random.randn(10000),
    })
    
    write(df, 'test_verbose_success.parquet', format='parquet', verbose=True)
    return None

@capture_logs
def test_basic_error():
    """기본 사용법에서 오류 시 진단 정보 확인"""
    df = pd.DataFrame({
        'A': np.random.randn(1000),
        'B': np.random.randn(1000),
    })
    
    try:
        write(df, 'test_basic_error.xyz', format='xyz')
    except Exception:
        pass
    return None

@capture_logs
def test_verbose_error():
    """verbose 모드에서 오류 시 진단 정보 확인"""
    df = pd.DataFrame({
        'A': np.random.randn(1000),
        'B': np.random.randn(1000),
    })
    
    try:
        write(df, 'test_verbose_error.xyz', format='xyz', verbose=True)
    except Exception:
        pass
    return None

@capture_logs
def test_large_data_success():
    """대용량 데이터에서 성공 시 진단 정보 확인"""
    df = pd.DataFrame({
        'A': np.random.randn(100000),  # 10만 행
        'B': np.random.randn(100000),
        'C': np.random.randn(100000),
        'D': np.random.randn(100000),
    })
    
    write(df, 'test_large_data_success.parquet', format='parquet')
    return None

@capture_logs
def test_large_data_verbose():
    """대용량 데이터에서 verbose 모드 진단 정보 확인"""
    df = pd.DataFrame({
        'A': np.random.randn(100000),  # 10만 행
        'B': np.random.randn(100000),
        'C': np.random.randn(100000),
        'D': np.random.randn(100000),
    })
    
    write(df, 'test_large_data_verbose.parquet', format='parquet', verbose=True)
    return None

def analyze_logs(logs, test_name):
    """로그 분석 및 진단 정보 확인"""
    print(f"\n=== {test_name} ===")
    
    # 기본 진단 정보 확인
    if "Atomic write completed successfully" in logs:
        print("✅ 기본 성공 진단 정보 제공됨")
    elif "Atomic write failed" in logs:
        print("✅ 기본 실패 진단 정보 제공됨")
    else:
        print("❌ 기본 진단 정보 없음")
    
    # 상세 진단 정보 확인
    if "Atomic write step timings" in logs:
        print("✅ 상세 성능 진단 정보 제공됨")
    else:
        print("❌ 상세 성능 진단 정보 없음")
    
    # 시간 정보 확인
    if "took" in logs:
        print("✅ 소요 시간 정보 제공됨")
    else:
        print("❌ 소요 시간 정보 없음")
    
    # 오류 유형 정보 확인
    if "error:" in logs:
        print("✅ 오류 유형 정보 제공됨")
    
    # 단계별 정보 확인
    if "setup=" in logs or "write_call=" in logs or "replace=" in logs:
        print("✅ 단계별 성능 정보 제공됨")
    
    return {
        'basic_diagnostic': "Atomic write completed successfully" in logs or "Atomic write failed" in logs,
        'detailed_diagnostic': "Atomic write step timings" in logs,
        'time_info': "took" in logs,
        'error_info': "error:" in logs,
        'step_info': "setup=" in logs or "write_call=" in logs or "replace=" in logs
    }

def main():
    """메인 테스트 실행"""
    print("라이브러리 기본 진단 로깅 테스트 시작")
    print("=" * 60)
    
    results = {}
    
    # 1. 기본 사용법 성공 테스트
    _, logs = test_basic_success()
    results['basic_success'] = analyze_logs(logs, "기본 사용법 성공")
    
    # 2. verbose 모드 성공 테스트
    _, logs = test_verbose_success()
    results['verbose_success'] = analyze_logs(logs, "verbose 모드 성공")
    
    # 3. 기본 사용법 오류 테스트
    _, logs = test_basic_error()
    results['basic_error'] = analyze_logs(logs, "기본 사용법 오류")
    
    # 4. verbose 모드 오류 테스트
    _, logs = test_verbose_error()
    results['verbose_error'] = analyze_logs(logs, "verbose 모드 오류")
    
    # 5. 대용량 데이터 기본 테스트
    _, logs = test_large_data_success()
    results['large_basic'] = analyze_logs(logs, "대용량 데이터 기본")
    
    # 6. 대용량 데이터 verbose 테스트
    _, logs = test_large_data_verbose()
    results['large_verbose'] = analyze_logs(logs, "대용량 데이터 verbose")
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("테스트 결과 요약")
    print("=" * 60)
    
    total_tests = len(results)
    basic_diagnostic_count = sum(1 for r in results.values() if r['basic_diagnostic'])
    detailed_diagnostic_count = sum(1 for r in results.values() if r['detailed_diagnostic'])
    time_info_count = sum(1 for r in results.values() if r['time_info'])
    error_info_count = sum(1 for r in results.values() if r['error_info'])
    step_info_count = sum(1 for r in results.values() if r['step_info'])
    
    print(f"총 테스트 수: {total_tests}")
    print(f"기본 진단 정보 제공: {basic_diagnostic_count}/{total_tests}")
    print(f"상세 진단 정보 제공: {detailed_diagnostic_count}/{total_tests}")
    print(f"소요 시간 정보 제공: {time_info_count}/{total_tests}")
    print(f"오류 정보 제공: {error_info_count}/{total_tests}")
    print(f"단계별 정보 제공: {step_info_count}/{total_tests}")
    
    # 개선 아이디어 3 달성도 평가
    print("\n💡 개선 아이디어 3 달성도:")
    if basic_diagnostic_count == total_tests:
        print("✅ 기본 진단 정보: 완벽 달성")
    else:
        print(f"⚠️ 기본 진단 정보: {basic_diagnostic_count}/{total_tests} 달성")
    
    if detailed_diagnostic_count >= total_tests // 2:
        print("✅ 상세 진단 정보: 대부분 달성")
    else:
        print(f"⚠️ 상세 진단 정보: {detailed_diagnostic_count}/{total_tests} 달성")
    
    if time_info_count == total_tests:
        print("✅ 소요 시간 정보: 완벽 달성")
    else:
        print(f"⚠️ 소요 시간 정보: {time_info_count}/{total_tests} 달성")

if __name__ == "__main__":
    main() 
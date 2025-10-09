import logging
import os


def setup_logger(name="atio", debug_level=False):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    # debug_level이 True이면 DEBUG 레벨로 설정, 아니면 INFO 레벨
    if debug_level:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    return logger


def check_file_exists(path):
    return os.path.exists(path)


import time
import threading


class ProgressBar:
    """
    파일 쓰기 진행 상황을 콘솔에 표시하는 클래스.
    스피너, 처리된 용량, 처리 속도, 경과 시간을 표시합니다.
    """

    def __init__(self, filepath: str, stop_event: threading.Event, description: str = "Writing"):
        self.filepath = filepath
        self.stop_event = stop_event
        self.description = description
        
        self.spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        self.start_time = time.time()
        
    def _format_size(self, size_bytes: int) -> str:
        """바이트를 KB, MB, GB 등 읽기 좋은 형태로 변환합니다."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024**2:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024**3:
            return f"{size_bytes/1024**2:.1f} MB"
        else:
            return f"{size_bytes/1024**3:.1f} GB"

    def run(self):
        """
        진행도 막대를 실행하는 메인 루프.
        이 함수가 모니터링 스레드에서 실행됩니다.
        """
        spinner_index = 0
        while not self.stop_event.is_set():
            spinner_char = self.spinner_chars[spinner_index % len(self.spinner_chars)]
            
            try:
                current_size = os.path.getsize(self.filepath)
            except FileNotFoundError:
                current_size = 0

            elapsed_time = time.time() - self.start_time
            
            # 0으로 나누기 방지
            speed = current_size / elapsed_time if elapsed_time > 0 else 0
            
            # 시간 포맷팅 (MM:SS)
            mins, secs = divmod(int(elapsed_time), 60)
            time_str = f"{mins:02d}:{secs:02d}"

            # 최종 출력 문자열 생성
            progress_line = (
                f"\r{spinner_char} {self.description} {os.path.basename(self.filepath)}... "
                f"[ {self._format_size(current_size)} | {self._format_size(speed)}/s | {time_str} ]"
            )
            
            # 콘솔에 한 줄 출력 (덮어쓰기)
            print(progress_line, end="", flush=True)
            
            spinner_index += 1
            time.sleep(0.1)  # 0.1초마다 업데이트하여 CPU 사용 최소화
        
        # 루프가 끝나면 마지막 완료 메시지를 출력
        self._finish()

    def _finish(self):
        """작업 완료 후 깔끔한 최종 메시지를 출력합니다."""
        final_size = os.path.getsize(self.filepath)
        elapsed_time = time.time() - self.start_time
        time_str = f"{int(elapsed_time)}s"
        
        # 기존 줄을 지우기 위해 공백으로 덮어씁니다.
        clear_line = "\r" + " " * 80 + "\r"
        
        finish_message = (
            f"✔︎ Finished {self.description} {os.path.basename(self.filepath)}. "
            f"({self._format_size(final_size)} in {time_str})"
        )
        print(clear_line + finish_message, flush=True)


import json

def read_json(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def write_json(data: dict, path: str):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

import os
from concurrent.futures import ProcessPoolExecutor
import atexit

_PROCESS_POOL = None

def get_process_pool():
    """
    전역 프로세스 풀을 생성하고 반환합니다. (지연 초기화)
    풀이 이미 생성되었다면 기존 객체를 반환합니다.
    """
    global _PROCESS_POOL

    # 풀이 아직 생성되지 않았을 때만 새로 생성합니다.
    if _PROCESS_POOL is None:
        _MAX_WORKERS = os.cpu_count() or 4
        print(f"--- ATIO Global Process Pool created (workers: {_MAX_WORKERS}) ---")
        _PROCESS_POOL = ProcessPoolExecutor(max_workers=_MAX_WORKERS)
    
    return _PROCESS_POOL

def _shutdown_pool():
    """프로그램 종료 시 풀을 안전하게 종료하는 함수"""
    global _PROCESS_POOL
    if _PROCESS_POOL:
        print("--- Shutting down ATIO Global Process Pool ---")
        _PROCESS_POOL.shutdown(wait=True)
        _PROCESS_POOL = None

atexit.register(_shutdown_pool)

class FileLock:
    """
    간단한 파일 기반 락(Lock)을 구현하는 컨텍스트 매니저.
    
    with FileLock(path):
        ... # 락이 필요한 위험한 작업 수행
    """
    def __init__(self, lock_dir, timeout=10):
        """
        Args:
            lock_dir (str): .lock 파일이 생성될 디렉토리.
            timeout (int): 락을 얻기 위해 대기할 최대 시간 (초).
        """
        os.makedirs(lock_dir, exist_ok=True)
        self.lock_path = os.path.join(lock_dir, '.lock')
        self.timeout = timeout
        self._lock_file_descriptor = None

    def __enter__(self):
        start_time = time.time()
        while True:
            try:
                # O_CREAT: 파일이 없으면 생성
                # O_EXCL: 파일이 이미 있으면 에러 발생 (원자적 연산)
                # O_WRONLY: 쓰기 전용으로 열기
                self._lock_file_descriptor = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                # 락 획득 성공
                return self
            except FileExistsError:
                if time.time() - start_time >= self.timeout:
                    raise TimeoutError(f"'{self.lock_path}'에 대한 락을 {self.timeout}초 내에 얻지 못했습니다.")
                time.sleep(0.1) # 짧은 시간 대기 후 재시도

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._lock_file_descriptor is not None:
            # 파일 디스크립터를 닫고 .lock 파일을 삭제
            os.close(self._lock_file_descriptor)
            os.remove(self.lock_path)
            self._lock_file_descriptor = None
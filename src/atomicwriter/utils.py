import logging
import os

def setup_logger(name="atomicwriter"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
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
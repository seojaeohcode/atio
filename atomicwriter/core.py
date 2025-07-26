"""progress 적용 후 write 함수"""
import os
import tempfile
import threading
from queue import Queue
from .plugins import get_writer
from .utils import setup_logger
from .progress import ProgressBar

def write(obj, target_path, format=None, show_progress=False, **kwargs):
    """
    데이터 객체(obj)를 안전하게 target_path에 저장합니다.
    format: 'parquet', 'csv' 등
    kwargs: 저장 함수에 전달할 추가 인자
    """
    logger = setup_logger()

    dir_name = os.path.dirname(os.path.abspath(target_path))
    base_name = os.path.basename(target_path)
    with tempfile.TemporaryDirectory(dir=dir_name) as tmpdir:
        tmp_path = os.path.join(tmpdir, base_name)
        logger.info(f"임시 디렉토리 생성: {tmpdir}")
        logger.info(f"임시 파일 경로: {tmp_path}")
        # 플러그인 기반 저장
        writer = get_writer(obj, format)
        if writer is None:
            logger.error(f"지원하지 않는 format: {format}")
            raise ValueError(f"지원하지 않는 format: {format}")
        logger.info(f"사용할 writer: {writer} (format: {format})") # 디버깅용 로그

        try:
            # --- 이 부분이 show_progress 옵션에 따라 다르게 동작 ---
            if not show_progress:
                # [옵션 OFF] 기존 방식대로 단순 실행
                _execute_write(writer, obj, tmp_path, **kwargs)
            else:
                # [옵션 ON] 멀티스레딩으로 진행도 표시와 함께 실행
                _execute_write_with_progress(writer, obj, tmp_path, **kwargs)

            logger.info(f"데이터 임시 파일에 저장 완료: {tmp_path}")

        except Exception as e:
            logger.error(f"임시 파일 저장 중 예외 발생: {e}")
            # with 구문이 끝나면서 임시 디렉토리는 자동으로 정리됩니다.
            raise e

        # try:
        #     if callable(writer):
        #         writer(obj, tmp_path, **kwargs)
        #     else:
        #         getattr(obj, writer)(tmp_path, **kwargs)
        #     logger.info(f"데이터 임시 파일에 저장 완료: {tmp_path}")
        # except Exception as e:
        #     logger.error(f"임시 파일 저장 중 예외 발생: {e}")
        #     # 임시 파일/디렉토리 자동 정리됨
        #     raise e

        # 원자적 교체
        os.replace(tmp_path, target_path)
        logger.info(f"원자적 교체 완료: {tmp_path} -> {target_path}")
        # _SUCCESS 플래그 파일 생성
        success_path = target_path + "._SUCCESS"
        with open(success_path, "w") as f:
            f.write("OK\n")
        logger.info(f"_SUCCESS 플래그 파일 생성: {success_path}")


def _execute_write(writer, obj, path, **kwargs):
    """단순히 쓰기 작업을 실행하는 내부 함수"""
    if callable(writer):
        writer(obj, path, **kwargs)
    else:
        getattr(obj, writer)(path, **kwargs)

def _execute_write_with_progress(writer, obj, path, **kwargs):
    """멀티스레딩으로 쓰기 작업과 진행도 표시를 함께 실행하는 내부 함수"""
    stop_event = threading.Event()
    exception_queue = Queue()

    # 실제 쓰기 작업을 수행할 '작업 스레드'의 목표 함수
    def worker_task():
        try:
            _execute_write(writer, obj, path, **kwargs)
        except Exception as e:
            exception_queue.put(e)

    # 스레드 생성
    worker_thread = threading.Thread(target=worker_task)
    progress_bar = ProgressBar(filepath=path, stop_event=stop_event, description="Writing")
    monitor_thread = threading.Thread(target=progress_bar.run)

    # 스레드 시작
    worker_thread.start()
    monitor_thread.start()

    # 작업 스레드가 끝날 때까지 대기
    worker_thread.join()

    # 모니터 스레드에 중지 신호 전송 및 종료 대기
    stop_event.set()
    monitor_thread.join()

    # 작업 스레드에서 예외가 발생했는지 확인하고, 있었다면 다시 발생시킴
    if not exception_queue.empty():
        raise exception_queue.get_nowait()
"""progress 적용 후 write 함수"""
import os
import tempfile
import threading
import time
from queue import Queue
from .plugins import get_writer
from .utils import setup_logger, ProgressBar

def write(obj, target_path, format=None, show_progress=False, verbose=False, **kwargs):
    """
    데이터 객체(obj)를 안전하게 target_path에 저장합니다.
    format: 'parquet', 'csv' 등
    show_progress: 진행도 표시 여부
    verbose: 상세한 성능 진단 정보 출력 여부 (기본: False)
    kwargs: 저장 함수에 전달할 추가 인자
    """
    logger = setup_logger(debug_level=verbose)
    
    # 성능 측정 시작
    t0 = time.perf_counter()
    
    dir_name = os.path.dirname(os.path.abspath(target_path))
    base_name = os.path.basename(target_path)
    with tempfile.TemporaryDirectory(dir=dir_name) as tmpdir:
        tmp_path = os.path.join(tmpdir, base_name)
        logger.info(f"임시 디렉토리 생성: {tmpdir}")
        logger.info(f"임시 파일 경로: {tmp_path}")
        
        # 임시 폴더 생성 완료 시점
        t1 = time.perf_counter()
        
        # 플러그인 기반 저장
        writer = get_writer(obj, format)
        if writer is None:
            logger.error(f"지원하지 않는 format: {format}")
            # 실패 시 성능 진단 로그 (verbose가 아니어도 오류 시에는 항상 출력)
            if verbose:
                logger.debug(f"Atomic write step timings (FAILED at setup): "
                            f"setup={t1-t0:.4f}s, "
                            f"write_call=N/A, "
                            f"replace=N/A, "
                            f"success_flag=N/A, "
                            f"total={time.perf_counter()-t0:.4f}s")
            logger.info(f"Atomic write failed at setup stage (took {time.perf_counter()-t0:.4f}s)")
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

            # 실제 쓰기 함수 호출 완료 시점
            t2 = time.perf_counter()
            
            logger.info(f"데이터 임시 파일에 저장 완료: {tmp_path}")

        except KeyboardInterrupt:
            # 인터럽트 발생 시 성능 진단 로그
            t_interrupt = time.perf_counter()
            logger.error(f"KeyboardInterrupt 발생 - 쓰기 작업 중단")
            if verbose:
                logger.debug(f"Atomic write step timings (INTERRUPTED): "
                            f"setup={t1-t0:.4f}s, "
                            f"write_call={t_interrupt-t1:.4f}s (중단됨), "
                            f"replace=N/A, "
                            f"success_flag=N/A, "
                            f"total={t_interrupt-t0:.4f}s")
            logger.info(f"Atomic write interrupted during write stage (took {t_interrupt-t0:.4f}s)")
            raise
            
        except Exception as e:
            # 일반 예외 발생 시 성능 진단 로그
            t_error = time.perf_counter()
            logger.error(f"임시 파일 저장 중 예외 발생: {e}")
            if verbose:
                logger.debug(f"Atomic write step timings (ERROR during write): "
                            f"setup={t1-t0:.4f}s, "
                            f"write_call={t_error-t1:.4f}s (실패), "
                            f"replace=N/A, "
                            f"success_flag=N/A, "
                            f"total={t_error-t0:.4f}s, "
                            f"error_type={type(e).__name__}")
            logger.info(f"Atomic write failed during write stage (took {t_error-t0:.4f}s, error: {type(e).__name__})")
            # with 구문이 끝나면서 임시 디렉토리는 자동으로 정리됩니다.
            raise e

        try:
            # 원자적 교체
            os.replace(tmp_path, target_path)
            # 원자적 교체 완료 시점
            t3 = time.perf_counter()
            
            logger.info(f"원자적 교체 완료: {tmp_path} -> {target_path}")
            
        except Exception as e:
            # 파일 교체 중 오류 발생 시 성능 진단 로그
            t_replace_error = time.perf_counter()
            logger.error(f"원자적 교체 중 예외 발생: {e}")
            if verbose:
                logger.debug(f"Atomic write step timings (ERROR during replace): "
                            f"setup={t1-t0:.4f}s, "
                            f"write_call={t2-t1:.4f}s, "
                            f"replace={t_replace_error-t2:.4f}s (실패), "
                            f"success_flag=N/A, "
                            f"total={t_replace_error-t0:.4f}s, "
                            f"error_type={type(e).__name__}")
            logger.info(f"Atomic write failed during replace stage (took {t_replace_error-t0:.4f}s, error: {type(e).__name__})")
            raise e
        
        try:
            # _SUCCESS 플래그 파일 생성
            success_path = target_path + "._SUCCESS"
            with open(success_path, "w") as f:
                f.write("OK\n")
            
            # 전체 작업 완료 시점
            t4 = time.perf_counter()
            
            logger.info(f"_SUCCESS 플래그 파일 생성: {success_path}")
            
            # 성능 진단 로그 (verbose가 True일 때만 상세 정보)
            if verbose:
                logger.debug(f"Atomic write step timings (SUCCESS): "
                            f"setup={t1-t0:.4f}s, "
                            f"write_call={t2-t1:.4f}s, "
                            f"replace={t3-t2:.4f}s, "
                            f"success_flag={t4-t3:.4f}s, "
                            f"total={t4-t0:.4f}s")
            logger.info(f"Atomic write completed successfully (took {t4-t0:.4f}s)")
                            
        except Exception as e:
            # SUCCESS 플래그 생성 중 오류 발생 시 성능 진단 로그
            t_success_error = time.perf_counter()
            logger.error(f"_SUCCESS 플래그 파일 생성 중 예외 발생: {e}")
            if verbose:
                logger.debug(f"Atomic write step timings (ERROR during success flag): "
                            f"setup={t1-t0:.4f}s, "
                            f"write_call={t2-t1:.4f}s, "
                            f"replace={t3-t2:.4f}s, "
                            f"success_flag={t_success_error-t3:.4f}s (실패), "
                            f"total={t_success_error-t0:.4f}s, "
                            f"error_type={type(e).__name__}")
            logger.info(f"Atomic write failed during success flag stage (took {t_success_error-t0:.4f}s, error: {type(e).__name__})")
            raise e


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
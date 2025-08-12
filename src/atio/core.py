"""progress 적용 후 write 함수"""
import os
import tempfile
import threading
import time
import numpy as np
from queue import Queue
from .plugins import get_writer
from .utils import setup_logger, ProgressBar

def write(obj, target_path, format=None, show_progress=False, verbose=False, **kwargs):
    """
    데이터 객체(obj)를 안전하게 target_path에 저장합니다. (롤백 기능 추가)
    format: 'parquet', 'csv' 등
    show_progress: 진행도 표시 여부
    verbose: 상세한 성능 진단 정보 출력 여부 (기본: False)
    kwargs: 저장 함수에 전달할 추가 인자
    """
    logger = setup_logger(debug_level=verbose)
    
    t0 = time.perf_counter()
    
    dir_name = os.path.dirname(os.path.abspath(target_path))
    base_name = os.path.basename(target_path)

    # 롤백을 위한 백업 경로 설정
    backup_path = target_path + "._backup"
    original_exists = os.path.exists(target_path)
    
    with tempfile.TemporaryDirectory(dir=dir_name) as tmpdir:
        tmp_path = os.path.join(tmpdir, base_name)
        logger.info(f"임시 디렉토리 생성: {tmpdir}")
        logger.info(f"임시 파일 경로: {tmp_path}")
        
        t1 = time.perf_counter()
        
        writer = get_writer(obj, format)
        if writer is None:
            logger.error(f"지원하지 않는 format: {format}")
            if verbose:
                logger.debug(f"Atomic write step timings (FAILED at setup): "
                            f"setup={t1-t0:.4f}s, total={time.perf_counter()-t0:.4f}s")
            logger.info(f"Atomic write failed at setup stage (took {time.perf_counter()-t0:.4f}s)")
            raise ValueError(f"지원하지 않는 format: {format}")
        logger.info(f"사용할 writer: {writer} (format: {format})")

        try:
            if not show_progress:
                _execute_write(writer, obj, tmp_path, **kwargs)
            else:
                _execute_write_with_progress(writer, obj, tmp_path, **kwargs)
            
            t2 = time.perf_counter()
            logger.info(f"데이터 임시 파일에 저장 완료: {tmp_path}")

        except Exception as e:
            # 쓰기 실패 시에는 롤백할 필요가 없음 (원본 파일은 그대로 있음)
            t_error = time.perf_counter()
            logger.error(f"임시 파일 저장 중 예외 발생: {e}")
            if verbose:
                logger.debug(f"Atomic write step timings (ERROR during write): "
                            f"setup={t1-t0:.4f}s, write_call={t_error-t1:.4f}s (실패), "
                            f"total={t_error-t0:.4f}s, error_type={type(e).__name__}")
            logger.info(f"Atomic write failed during write stage (took {t_error-t0:.4f}s, error: {type(e).__name__})")
            raise e

        # [롤백 STEP 1] 기존 파일 백업
        if original_exists:
            logger.info(f"기존 파일 백업: {target_path} -> {backup_path}")
            try:
                # rename은 atomic 연산이므로 백업 과정도 안전합니다.
                os.rename(target_path, backup_path)
            except Exception as e:
                logger.error(f"백업 생성 실패. 작업을 중단합니다: {e}")
                # 백업 실패 시 더 이상 진행하면 안 되므로 예외를 발생시킵니다.
                raise IOError(f"Failed to create backup for {target_path}") from e

        try:
            # [롤백 STEP 2] 원자적 교체
            os.replace(tmp_path, target_path)
            t3 = time.perf_counter()
            logger.info(f"원자적 교체 완료: {tmp_path} -> {target_path}")
            
            # [롤백 STEP 3] _SUCCESS 플래그 생성
            success_path = target_path + "._SUCCESS"
            with open(success_path, "w") as f:
                f.write("OK\n")
            t4 = time.perf_counter()
            logger.info(f"_SUCCESS 플래그 파일 생성: {success_path}")
            
            # [롤백 STEP 4] 성공 시 백업 파일 삭제
            if original_exists:
                os.remove(backup_path)
                logger.info(f"작업 성공, 백업 파일 삭제 완료: {backup_path}")

            if verbose:
                logger.debug(f"Atomic write step timings (SUCCESS): "
                             f"setup={t1-t0:.4f}s, write_call={t2-t1:.4f}s, "
                             f"replace={t3-t2:.4f}s, success_flag={t4-t3:.4f}s, "
                             f"total={t4-t0:.4f}s")
            logger.info(f"Atomic write completed successfully (took {t4-t0:.4f}s)")

        except Exception as e:
            # [롤백 STEP 5] 교체 또는 플래그 생성 실패 시 롤백 실행
            t_final_error = time.perf_counter()
            logger.error(f"최종 저장 단계에서 오류 발생. 롤백을 시작합니다. 원인: {e}")

            if original_exists:
                try:
                    # 새로 쓴 불완전한 파일이 있다면 삭제
                    if os.path.exists(target_path):
                        os.remove(target_path)
                    
                    # 백업해둔 원본 파일을 다시 복구
                    os.rename(backup_path, target_path)
                    logger.info(f"롤백 성공: 원본 파일 복구 완료 ({backup_path} -> {target_path})")
                
                except Exception as rollback_e:
                    logger.critical(f"치명적 오류: 롤백 실패! {rollback_e}")
                    logger.critical(f"시스템이 불안정한 상태일 수 있습니다. 수동 확인이 필요합니다.")
                    logger.critical(f"남아있는 파일: (새 데이터) {target_path}, (원본 백업) {backup_path}")

            if verbose:
                 logger.debug(f"Atomic write step timings (FAILED AND ROLLED BACK): "
                             f"setup={t1-t0:.4f}s, write_call={t2-t1:.4f}s, "
                             f"final_stage_fail_time={t_final_error-t2:.4f}s, "
                             f"total={t_final_error-t0:.4f}s, error_type={type(e).__name__}")
            logger.info(f"Atomic write failed and rolled back (took {t_final_error-t0:.4f}s, error: {type(e).__name__})")
            
            # 원본 예외를 다시 발생시켜 사용자에게 알립니다.
            raise e

def _execute_write(writer, obj, path, **kwargs):
    """단순히 쓰기 작업을 실행하는 내부 함수"""
    if callable(writer):
        if writer in (np.savez, np.savez_compressed):
            # .npz 저장을 위해서는 데이터 객체(obj)가 반드시 딕셔너리여야 합니다.
            if not isinstance(obj, dict):
                raise TypeError(
                    f"To save multiple arrays with '{writer.__name__}', "
                    f"the data object must be a dictionary, not {type(obj).__name__}"
                )
            writer(path, **obj)

        # 2. np.save, np.savetxt 등 다른 모든 일반 함수 처리
        else:
            writer(path, obj, **kwargs)

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
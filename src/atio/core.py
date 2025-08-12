"""progress 적용 후 write 함수"""
import os
import tempfile
import threading
import time
import numpy as np
from queue import Queue
from .plugins import get_writer
from .utils import setup_logger, ProgressBar

def write(obj, target_path=None, format=None, show_progress=False, verbose=False, **kwargs):
    """
    데이터 객체(obj)를 안전하게 target_path 또는 데이터베이스에 저장합니다.

    - 파일 기반 쓰기 (format: 'csv', 'parquet', 'excel' 등):
      - target_path (str): 필수. 데이터가 저장될 파일 경로입니다.
      - 롤백 기능이 있는 원자적 쓰기를 수행합니다.

    - 데이터베이스 기반 쓰기 (format: 'sql', 'database'):
      - target_path: 사용되지 않습니다.
      - kwargs (dict): 데이터베이스 쓰기에 필요한 추가 인자들입니다.
        - pandas.to_sql: 'name'(테이블명), 'con'(커넥션 객체)가 필수입니다.
        - polars.write_database: 'table_name', 'connection_uri'가 필수입니다.
    
    Args:
        obj: 저장할 데이터 객체 (e.g., pandas.DataFrame, polars.DataFrame, np.ndarray).
        target_path (str, optional): 파일 저장 경로. 파일 기반 쓰기 시 필수. Defaults to None.
        format (str, optional): 저장할 포맷. Defaults to None.
        show_progress (bool): 진행도 표시 여부. Defaults to False.
        verbose (bool): 상세한 성능 진단 정보 출력 여부. Defaults to False.
        **kwargs: 각 쓰기 함수에 전달될 추가 키워드 인자.
    """
    logger = setup_logger(debug_level=verbose)
    t0 = time.perf_counter()

    # --- 1. 데이터베이스 쓰기 특별 처리 ---
    # 데이터베이스 쓰기는 파일 경로 기반의 원자적 쓰기 로직을 따르지 않습니다.
    if format in ('sql', 'database'):
        logger.info(f"데이터베이스 쓰기 모드 시작 (format: {format})")
        writer_method_name = get_writer(obj, format)
        
        if writer_method_name is None:
            err_msg = f"객체 타입 {type(obj).__name__}에 대해 지원하지 않는 format: {format}"
            logger.error(err_msg)
            raise ValueError(err_msg)

        try:
            writer_func = getattr(obj, writer_method_name)
            
            # 각 데이터베이스 쓰기 함수에 필요한 필수 인자 확인
            if format == 'sql': # Pandas
                if 'name' not in kwargs or 'con' not in kwargs:
                    raise ValueError("'name'(테이블명)과 'con'(DB 커넥션) 인자는 'sql' 포맷에 필수입니다.")
            elif format == 'database': # Polars
                if 'table_name' not in kwargs or 'connection_uri' not in kwargs:
                    raise ValueError("'table_name'과 'connection_uri' 인자는 'database' 포맷에 필수입니다.")
            
            # target_path는 무시하고 **kwargs로 받은 인자들을 사용하여 DB에 직접 씁니다.
            writer_func(**kwargs)
            
            t_end = time.perf_counter()
            logger.info(f"✅ 데이터베이스 쓰기 완료 (총 소요 시간: {t_end - t0:.4f}s)")
            return # DB 쓰기 완료 후 함수 종료

        except Exception as e:
            t_err = time.perf_counter()
            logger.error(f"데이터베이스 쓰기 중 예외 발생: {e}")
            logger.info(f"데이터베이스 쓰기 실패 (소요 시간: {t_err - t0:.4f}s, 에러: {type(e).__name__})")
            raise e

    # --- 2. 파일 기반 원자적 쓰기 ---
    if target_path is None:
        raise ValueError("파일 기반 쓰기(예: 'csv', 'parquet')에는 'target_path' 인자가 필수입니다.")

    dir_name = os.path.dirname(os.path.abspath(target_path))
    base_name = os.path.basename(target_path)
    os.makedirs(dir_name, exist_ok=True)

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
            success_path = os.path.join(os.path.dirname(target_path), f".{os.path.basename(target_path)}._SUCCESS")
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
            logger.info(f"✅ Atomic write completed successfully (took {t4-t0:.4f}s)")

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
    """
    내부 쓰기 실행 함수. 핸들러 타입에 따라 분기하여 실제 쓰기 작업을 수행합니다.
    - callable(writer): `np.save`와 같은 함수 핸들러
    - str(writer): `to_csv`와 같은 객체의 메소드 핸들러
    """
    # 1. writer가 호출 가능한 '함수'인 경우 (e.g., np.save, np.savetxt)
    if callable(writer):
        # 1a. np.savez, np.savez_compressed 특별 처리: 여러 배열을 dict로 받아 저장
        if writer in (np.savez, np.savez_compressed):
            if not isinstance(obj, dict):
                raise TypeError(
                    f"'{writer.__name__}'로 여러 배열을 저장하려면, "
                    f"데이터 객체는 dict 타입이어야 합니다. (현재: {type(obj).__name__})"
                )
            writer(path, **obj)
        # 1b. 그 외 일반적인 함수 핸들러 처리
        else:
            writer(path, obj, **kwargs)

    # 2. writer가 '메소드 이름(문자열)'인 경우 (e.g., 'to_csv', 'to_excel')
    # 이 경우, obj.to_csv(path, **kwargs) 와 같이 호출됩니다.
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

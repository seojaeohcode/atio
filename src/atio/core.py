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

import uuid
import pyarrow as pa
from .utils import read_json, write_json
import io
import pyarrow.parquet as pq
from tqdm import tqdm

def write_snapshot(obj, table_path, mode='overwrite', show_progress=False, **kwargs):
    """
    데이터 객체를 열 단위 청크로 분해하여 버전 관리(스냅샷) 방식으로 저장합니다.

    Args:
        obj: 저장할 데이터 객체 (pandas, polars, numpy, pyarrow.Table).
        table_path (str): 테이블 데이터가 저장될 최상위 디렉토리 경로.
        mode (str): 'overwrite' (기본값) 또는 'append'.
                    - 'overwrite': 테이블을 현재 데이터로 완전히 대체합니다.
                    - 'append': 기존 버전의 데이터에 현재 데이터를 추가(열 기준)합니다.
        show_progress (bool): 진행률 표시 여부. Defaults to False.
    """
    logger = setup_logger(debug_level=False)

    format='parquet'

    # 1. 경로 설정 및 폴더 생성
    data_dir = os.path.join(table_path, 'data')
    metadata_dir = os.path.join(table_path, 'metadata')
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(metadata_dir, exist_ok=True)
    
    # 2. 현재 버전 확인
    pointer_path = os.path.join(table_path, '_current_version.json')
    current_version = 0
    if os.path.exists(pointer_path):
        current_version = read_json(pointer_path)['version_id']
    new_version = current_version + 1

    # 3. Arrow Table로 표준화 (NumPy 지원 추가)
    if isinstance(obj, pa.Table):
        arrow_table = obj
    elif hasattr(obj, 'to_arrow'):  # Polars
        arrow_table = obj.to_arrow()
    elif hasattr(obj, '__arrow_array__') or hasattr(obj, '__dataframe__'): # Pandas
        arrow_table = pa.Table.from_pandas(obj)
    elif "numpy" in str(type(obj)): # NumPy 처리 부분
        # (핵심 수정) 배열의 차원(ndim)에 따라 다르게 처리
        if obj.ndim == 1:
            # 1차원 배열은 기존 방식 그대로 사용
            arrow_table = pa.Table.from_arrays([obj], names=['_col_0'])
        else:
            # 2차원 이상 배열은 "리스트의 리스트"로 변환 후 Arrow Array로 만듦
            arrow_table = pa.Table.from_arrays([pa.array(obj.tolist())], names=['_col_0'])
            
    else:
        raise TypeError(f"지원하지 않는 데이터 타입: {type(obj)}")

    # 4. 임시 디렉토리에서 열 단위 해시 계산 및 중복 없는 쓰기
    with tempfile.TemporaryDirectory() as tmpdir:
        new_snapshot_columns = []
        temp_data_files_to_commit = {}  # {임시경로: 최종경로}

        iterable = tqdm(
            arrow_table.column_names,
            desc="Saving snapshot columns",
            disable=not show_progress
        )

        for col_name in iterable:
            i = arrow_table.schema.get_field_index(col_name)
            column_array = arrow_table.column(i)
            col_hash = _get_column_hash(column_array, col_name)
            chunk_filename = f"{col_hash}.{format}"
            final_data_path = os.path.join(data_dir, chunk_filename)
            
            if not os.path.exists(final_data_path):
                tmp_data_path = os.path.join(tmpdir, chunk_filename)
                
                table_to_write = pa.Table.from_arrays([column_array], names=[col_name])

                pq.write_table(table_to_write, tmp_data_path)
                # 바깥쪽 with open() 구문이 끝나면서 파일이 확실하게 닫힙니다.
                    
                temp_data_files_to_commit[tmp_data_path] = final_data_path
            
            new_snapshot_columns.append({"name": col_name, "hash": col_hash, "format": format})

        # 5. Snapshot 생성 (overwrite/append 모드 분기 처리)
        snapshot_id = int(time.time())
        snapshot_filename = f"snapshot-{snapshot_id}-{uuid.uuid4()}.json"
        
        final_columns_for_snapshot = new_snapshot_columns

        # append 모드이고 이전 버전이 존재할 경우, 이전 스냅샷의 컬럼 목록을 가져와 병합
        if mode.lower() == 'append' and current_version > 0:
            try:
                prev_metadata_path = os.path.join(metadata_dir, f'v{current_version}.metadata.json')
                prev_metadata = read_json(prev_metadata_path)
                prev_snapshot_filename = prev_metadata['snapshot_filename']
                prev_snapshot_path = os.path.join(table_path, prev_snapshot_filename)
                prev_snapshot = read_json(prev_snapshot_path)
                
                previous_columns = prev_snapshot.get('columns', [])
                final_columns_for_snapshot = previous_columns + new_snapshot_columns
                logger.info(f"Append 모드: v{current_version}의 컬럼 {len(previous_columns)}개에 {len(new_snapshot_columns)}개를 추가합니다.")

            except (FileNotFoundError, KeyError) as e:
                logger.warning(f"Append 모드 실행 중 이전 버전 정보를 찾을 수 없어 Overwrite 모드로 동작합니다. 오류: {e}")
        
        new_snapshot = {
            'snapshot_id': snapshot_id,
            'timestamp': time.time(),
            'columns': final_columns_for_snapshot
        }
        write_json(new_snapshot, os.path.join(tmpdir, snapshot_filename))
        
        # 6. Metadata 및 포인터 생성
        new_metadata = {
            'version_id': new_version,
            'snapshot_id': snapshot_id,
            'snapshot_filename': os.path.join('metadata', snapshot_filename)
        }
        metadata_filename = f"v{new_version}.metadata.json"
        write_json(new_metadata, os.path.join(tmpdir, metadata_filename))

        new_pointer = {'version_id': new_version}
        tmp_pointer_path = os.path.join(tmpdir, '_current_version.json')
        write_json(new_pointer, tmp_pointer_path)

        # 7. 최종 커밋 (새로 쓰여진 데이터 파일과 메타데이터 파일들을 최종 위치로 이동)
        for tmp_path, final_path in temp_data_files_to_commit.items():
            os.rename(tmp_path, final_path)
        
        os.rename(os.path.join(tmpdir, snapshot_filename), os.path.join(metadata_dir, snapshot_filename))
        os.rename(os.path.join(tmpdir, metadata_filename), os.path.join(metadata_dir, metadata_filename))
        os.replace(tmp_pointer_path, pointer_path) # 원자적 연산으로 포인터 교체
        
        logger.info(f"✅ 스냅샷 저장 완료! '{table_path}'가 버전 {new_version}으로 업데이트되었습니다. (모드: {mode})")


import pyarrow as pa
import pandas as pd
import polars as pl

def read_table(table_path, version=None, output_as='pandas'):
    """
    지정된 버전의 스냅샷을 읽어 데이터 객체로 재구성합니다.

    Args:
        table_path (str): 테이블 데이터가 저장된 최상위 디렉토리 경로.
        version (int, optional): 불러올 버전 ID. None이면 최신 버전을 불러옵니다.
        output_as (str): 반환할 데이터 객체 타입 ('pandas', 'polars', 'arrow', 'numpy').
                         Defaults to 'pandas'.
    
    Returns:
        지정된 포맷의 데이터 객체 (e.g., pandas.DataFrame).
    """
    logger = setup_logger(debug_level=False)

    # --- 1. 읽을 버전의 스냅샷 파일 경로 찾기 ---
    try:
        if version is None:
            pointer_path = os.path.join(table_path, '_current_version.json')
            version_id = read_json(pointer_path)['version_id']
            logger.info(f"최신 버전(v{version_id})을 읽습니다.")
        else:
            version_id = version
            logger.info(f"지정된 버전(v{version_id})을 읽습니다.")
        
        metadata_path = os.path.join(table_path, 'metadata', f'v{version_id}.metadata.json')
        metadata = read_json(metadata_path)
        snapshot_filename = metadata['snapshot_filename']
        snapshot_path = os.path.join(table_path, snapshot_filename)
        snapshot = read_json(snapshot_path)
    
    except FileNotFoundError as e:
        logger.error(f"읽기 실패: 필요한 메타데이터 또는 스냅샷 파일을 찾을 수 없습니다. 경로: {e.filename}")
        raise e
    except (KeyError, IndexError) as e:
        logger.error(f"읽기 실패: 메타데이터 파일의 형식이 잘못되었습니다. 오류: {e}")
        raise e

    # --- 2. 스냅샷 정보를 기반으로 Arrow 컬럼들을 읽어오기 ---
    columns_to_load = snapshot.get('columns', [])
    if not columns_to_load:
        logger.warning(f"버전 {version_id}은 데이터가 비어있습니다. 빈 객체를 반환합니다.")
        if output_as == 'pandas': return pd.DataFrame()
        if output_as == 'polars': return pl.DataFrame()
        if output_as == 'arrow': return pa.Table.from_pydict({})
        if output_as == 'numpy': return np.array([])
        return None

    arrow_arrays = []
    column_names = []
    data_dir = os.path.join(table_path, 'data')

    for col_info in columns_to_load:
        col_name = col_info['name']
        col_hash = col_info['hash']
        col_format = col_info.get('format', 'arrow') # 하위 호환성을 위해 format 필드 사용
        
        chunk_path = os.path.join(data_dir, f"{col_hash}.{col_format}")
        
        # Arrow IPC(Feather V2) 포맷으로 저장된 단일 컬럼 파일 읽기
        arrow_table_chunk = pq.read_table(chunk_path)
        # 파일에는 컬럼이 하나만 들어있으므로 로직은 동일
        arrow_arrays.append(arrow_table_chunk.column(0))
        column_names.append(col_name)

    # --- 3. 읽어온 컬럼들을 하나의 Arrow Table로 조합 ---
    final_arrow_table = pa.Table.from_arrays(arrow_arrays, names=column_names)
    logger.info(f"데이터 로드 완료. 총 {final_arrow_table.num_rows}행, {final_arrow_table.num_columns}열.")

    # --- 4. 사용자가 요청한 포맷으로 변환하여 반환 ---
    if output_as.lower() == 'pandas':
        return final_arrow_table.to_pandas()
    elif output_as.lower() == 'polars':
        return pl.from_arrow(final_arrow_table)
    elif output_as.lower() == 'arrow':
        return final_arrow_table
    elif output_as.lower() == 'numpy':
        # (핵심 수정) NumPy 변환 로직 보강
        if final_arrow_table.num_columns == 1:
            column = final_arrow_table.column(0)
            # 컬럼 타입이 리스트인지(2D+ 배열이었는지) 확인
            if pa.types.is_list(column.type):
                # 리스트 컬럼이면, to_pylist()로 파이썬 리스트로 만든 후 np.array로 재조립
                return np.array(column.to_pylist())
            else:
                # 단순 1D 배열이었으면 기존 방식 사용
                return column.to_numpy()
        else:
            logger.warning("NumPy 출력은 컬럼이 하나일 때만 지원됩니다. Arrow 테이블을 반환합니다.")
            return final_arrow_table
    
    raise ValueError(f"지원하지 않는 출력 형식입니다: {output_as}")


import shutil
# (다른 import 문들은 그대로 유지)

def delete_version(table_path, version_id, dry_run=False, logger=None):
    """
    특정 버전을 삭제하고, 더 이상 참조되지 않는 데이터 파일(가비지)을 정리합니다.

    Args:
        table_path (str): 테이블 데이터가 저장된 최상위 디렉토리 경로.
        version_id (int): 삭제할 버전의 ID.
        dry_run (bool): True이면 실제로 삭제하지 않고 대상 목록만 출력합니다.
    """
    if logger is None:
        logger = setup_logger()

    # --- 1단계: 버전 메타데이터 삭제 ---
    logger.info(f"버전 {version_id} 삭제를 시작합니다...")
    
    # 안전장치: 현재 활성화된 최신 버전은 삭제할 수 없도록 방지
    pointer_path = os.path.join(table_path, '_current_version.json')
    try:
        current_version = read_json(pointer_path)['version_id']
        if version_id == current_version:
            logger.error(f"삭제 실패: 현재 활성화된 최신 버전(v{version_id})은 삭제할 수 없습니다.")
            logger.error("다른 버전으로 롤백(rollback)한 후 시도해 주세요.")
            return False
    except FileNotFoundError:
        pass

    metadata_path = os.path.join(table_path, 'metadata', f'v{version_id}.metadata.json')
    if not os.path.exists(metadata_path):
        logger.warning(f"삭제할 버전(v{version_id})을 찾을 수 없습니다.")
        return False
        
    try:
        # vX.metadata.json 파일만 먼저 삭제
        if not dry_run:
            os.remove(metadata_path)
        logger.info(f"버전 {version_id}의 메타데이터를 성공적으로 삭제했습니다.")
    except OSError as e:
        logger.error(f"버전 {version_id}의 메타데이터 삭제 중 오류 발생: {e}")
        return False

    # --- 2단계: 가비지 컬렉션 (Vacuum) 시작 ---
    logger.info("가비지 컬렉션을 시작합니다 (사용되지 않는 파일 정리)...")
    
    metadata_dir = os.path.join(table_path, 'metadata')
    data_dir = os.path.join(table_path, 'data')

    # "살아있는" 모든 객체(스냅샷, 데이터 해시)의 목록 만들기
    live_snapshot_files = set()
    live_data_hashes = set()

    for meta_filename in os.listdir(metadata_dir):
        if meta_filename.startswith('v') and meta_filename.endswith('.metadata.json'):
            try:
                meta = read_json(os.path.join(metadata_dir, meta_filename))
                snapshot_filename = os.path.basename(meta['snapshot_filename'])
                live_snapshot_files.add(snapshot_filename)
                
                snapshot = read_json(os.path.join(table_path, meta['snapshot_filename']))
                for col_info in snapshot.get('columns', []):
                    live_data_hashes.add(col_info['hash'])
            except (FileNotFoundError, KeyError):
                continue

    # "고아" 객체(삭제 대상) 식별
    files_to_delete = []
    if os.path.isdir(data_dir):
        for data_filename in os.listdir(data_dir):
            file_hash = os.path.splitext(data_filename)[0]
            if file_hash not in live_data_hashes:
                files_to_delete.append(os.path.join(data_dir, data_filename))

    for snapshot_filename in os.listdir(metadata_dir):
        if snapshot_filename.startswith('snapshot-') and snapshot_filename not in live_snapshot_files:
            files_to_delete.append(os.path.join(metadata_dir, snapshot_filename))
    
    # 최종 삭제 실행
    if not files_to_delete:
        logger.info("정리할 추가 파일이 없습니다.")
        return True

    logger.info(f"총 {len(files_to_delete)}개의 정리 대상을 찾았습니다.")
    if dry_run:
        print("\n--- [Dry Run] 아래 파일들이 삭제될 예정입니다 ---")
        for f in sorted(files_to_delete):
            print(f"  - {os.path.relpath(f, table_path)}")
    else:
        logger.info("실제 파일 삭제를 시작합니다...")
        deleted_count = 0
        for f in files_to_delete:
            try:
                os.remove(f)
                deleted_count += 1
            except OSError as e:
                logger.error(f"파일 삭제 실패: {f}, 오류: {e}")
        logger.info(f"✅ 총 {deleted_count}개의 파일 삭제 작업이 완료되었습니다.")
    
    return True

import json

def rollback(table_path, version_id, logger=None):
    """
    테이블의 현재 버전을 지정된 버전 ID로 롤백합니다.

    Args:
        table_path (str): 테이블 데이터가 저장된 최상위 디렉토리 경로.
        version_id (int): 롤백할 목표 버전의 ID.

    Returns:
        bool: 성공 시 True, 실패 시 False를 반환합니다.
    """
    if logger is None:
        # 이 부분은 라이브러리의 로거 설정에 맞게 수정하세요.
        logger = lambda msg, level="info": print(f"[{level.upper()}] {msg}")

    # 1. 롤백하려는 버전이 실제로 존재하는지 확인
    metadata_path = os.path.join(table_path, 'metadata', f'v{version_id}.metadata.json')
    if not os.path.exists(metadata_path):
        logger(f"롤백 실패: 버전 {version_id}이(가) 존재하지 않습니다.", level="error")
        return False

    # 2. _current_version.json 포인터 파일의 내용을 수정
    pointer_path = os.path.join(table_path, '_current_version.json')
    try:
        with open(pointer_path, 'w', encoding='utf-8') as f:
            json.dump({'version_id': version_id}, f)
        logger(f"✅ 롤백 성공! 현재 버전이 v{version_id}(으)로 설정되었습니다.")
        return True
    except OSError as e:
        logger(f"롤백 실패: 포인터 파일을 쓰는 중 오류 발생 - {e}", level="error")
        return False
    
def export_to_datalake(table_path, version, output_path, **kwargs):
    """지정된 버전의 atio 스냅샷을 단일 Parquet 파일로 내보냅니다."""

    # 1. atio의 read_table을 사용해 완전한 테이블을 메모리로 불러옵니다.
    full_table = read_table(table_path, version=version, output_as='arrow')

    # 2. 이 테이블을 '하나의' 표준 Parquet 파일로 저장합니다.
    import pyarrow.parquet as pq
    pq.write_table(full_table, output_path, **kwargs)

import fastcdc
from concurrent.futures import ProcessPoolExecutor, as_completed
from .utils import get_process_pool
import xxhash

def _process_chunk_from_file_task(args):
    """
    '작업 지시서'를 받아 파일을 직접 읽고 처리하는 함수
    """
    file_path, offset, length, data_dir = args
    
    # 파일을 직접 열고, 해당 위치로 이동(seek)하여 필요한 만큼만 읽음
    with open(file_path, 'rb') as f:
        f.seek(offset)
        chunk_content = f.read(length)
    
    chunk_hash = xxhash.xxh64(chunk_content).hexdigest()
    
    chunk_save_path = os.path.join(data_dir, chunk_hash)
    if not os.path.exists(chunk_save_path):
        return (chunk_hash, chunk_content)
    
    return (chunk_hash, None)


def _get_column_hash(arrow_column: pa.Array, column_name: str) -> str:
    """Arrow 컬럼(ChunkedArray)의 내용을 기반으로 sha256 해시를 계산합니다."""
    mock_sink = io.BytesIO()

    # (핵심 수정) ChunkedArray를 하나의 Array로 합칩니다.
    if isinstance(arrow_column, pa.ChunkedArray):
        array_to_write = arrow_column.combine_chunks()
    else:
        array_to_write = arrow_column

    batch = pa.RecordBatch.from_arrays([array_to_write], names=[column_name])
    
    with pa.ipc.new_stream(mock_sink, batch.schema) as writer:
        writer.write_batch(batch)

    return xxhash.xxh64(mock_sink.getvalue()).hexdigest()


def write_model_snapshot(model_path: str, table_path: str, show_progress: bool = False):
    """
    - PyTorch 또는 TensorFlow 모델의 스냅샷을 저장합니다.
    - 생산자-소비자 패턴으로 메모리 사용량을 최적화합니다.
    - 다중 파일 모델(TensorFlow) 처리를 지원합니다.
    """
    logger = setup_logger()

    # --- 1. 경로 설정 및 버전 관리 ---
    data_dir = os.path.join(table_path, 'data')
    metadata_dir = os.path.join(table_path, 'metadata')
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(metadata_dir, exist_ok=True)
    
    pointer_path = os.path.join(table_path, '_current_version.json')
    current_version = 0
    if os.path.exists(pointer_path):
        current_version = read_json(pointer_path)['version_id']
    new_version = current_version + 1
    
    logger.info(f"모델 스냅샷 v{new_version} 생성을 시작합니다...")

    # --- 2. 모델 타입 감지 및 처리할 파일 목록 생성 ---
    is_pytorch = os.path.isfile(model_path) and model_path.endswith(('.pth', '.pt'))
    is_tensorflow = os.path.isdir(model_path) and os.path.exists(os.path.join(model_path, 'saved_model.pb'))

    files_to_process = []
    if is_pytorch:
        files_to_process.append(model_path)
        model_path_base = os.path.dirname(model_path)
    elif is_tensorflow:
        for root, _, files in os.walk(model_path):
            for filename in files:
                files_to_process.append(os.path.join(root, filename))
        model_path_base = model_path
    else:
        raise ValueError(f"지원하지 않는 모델 형식입니다: {model_path}")

    # --- 3. 생산자-소비자 패턴으로 병렬 처리 ---
    all_files_info = []
    new_chunks_to_write = {}
    executor = get_process_pool()

    # 파일 목록을 순회 (TensorFlow의 경우 여러 파일, PyTorch는 1개)
    for file_path in tqdm(files_to_process, desc="Total Progress", disable=not show_progress or len(files_to_process) == 1):
        relative_path = os.path.relpath(file_path, model_path_base).replace('\\', '/')
        file_info = {"path": relative_path, "chunks": []}
    
    with open(file_path, 'rb') as f:
        # 생산자: fastcdc가 청크 '정보'를 하나씩 생성하는 제너레이터
        cdc = fastcdc.fastcdc(f, avg_size=65536, fat=True)
        
        # [핵심] 청크 정보를 만들면서 '동시에' 작업을 제출하는 future 제너레이터를 생성
        def submit_tasks_generator():
            for chunk in cdc:
                job_ticket = (file_path, chunk.offset, chunk.length, data_dir)
                yield executor.submit(_process_chunk_from_file_task, job_ticket)

        # 총 청크 수를 알 수 없으므로 파일 크기를 기준으로 진행률 표시
        file_size = os.path.getsize(file_path)
        progress_desc = os.path.basename(file_path)
        
        with tqdm(total=file_size, desc=f"Processing {progress_desc}", unit='B', unit_scale=True, disable=not show_progress, leave=False) as pbar:
            # as_completed는 future가 완료되는 대로 결과를 반환
            for future in as_completed(submit_tasks_generator()):
                chunk_hash, chunk_content = future.result()
                file_info["chunks"].append(chunk_hash)
                if chunk_content is not None:
                    new_chunks_to_write[chunk_hash] = chunk_content
                
                # 대략적인 청크 크기만큼 진행률 업데이트
                pbar.update(65536)

    all_files_info.append(file_info)

    logger.info(f"데이터 병렬 처리 완료")

    # --- 4. 최종 커밋 및 메타데이터 생성 ---
    for chunk_hash, chunk_content in tqdm(new_chunks_to_write.items(), desc="Committing new chunks", disable=not show_progress):
        with open(os.path.join(data_dir, chunk_hash), 'wb') as f:
            f.write(chunk_content)

    snapshot_id = int(time.time())
    snapshot_filename = f"snapshot-{snapshot_id}-{uuid.uuid4()}.json"
    
    new_snapshot = {'snapshot_id': snapshot_id, 'timestamp': time.time(), 'files': sorted(all_files_info, key=lambda x: x['path'])}
    write_json(new_snapshot, os.path.join(metadata_dir, snapshot_filename))
    
    new_metadata = {'version_id': new_version, 'snapshot_id': snapshot_id, 'snapshot_filename': os.path.join('metadata', snapshot_filename)}
    metadata_filename = f"v{new_version}.metadata.json"
    write_json(new_metadata, os.path.join(metadata_dir, metadata_filename))

    new_pointer = {'version_id': new_version}
    tmp_pointer_path = os.path.join(metadata_dir, f"_pointer_{uuid.uuid4()}.json")
    write_json(new_pointer, tmp_pointer_path)
    os.replace(tmp_pointer_path, pointer_path)

    end_time = time.perf_counter()
    logger.info(f"✅ 모델 스냅샷 v{new_version} 생성이 완료되었습니다.")

from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import torch
    _PYTORCH_AVAILABLE = True
except ImportError:
    _PYTORCH_AVAILABLE = False

try:
    import tensorflow as tf
    _TENSORFLOW_AVAILABLE = True
except ImportError:
    _TENSORFLOW_AVAILABLE = False

def _read_chunk(chunk_path: str) -> bytes:
    """단일 청크 파일을 읽어 내용을 반환하는 간단한 함수 (스레드에서 실행될 작업)"""
    with open(chunk_path, 'rb') as f:
        return f.read()

def _reassemble_from_chunks_threaded(table_path, snapshot, destination_path=None, max_workers=None, show_progress=False):
    """
    스냅샷 정보를 바탕으로 청크들을 'ThreadPoolExecutor'를 사용해 병렬로 조합하여 모델을 복원합니다.
    max_workers: 사용할 스레드의 최대 개수 (None이면 기본값 사용)
    """
    data_dir = os.path.join(table_path, 'data')
    files_info = snapshot.get('files', [])

    # ThreadPoolExecutor를 with문과 함께 사용해 안전하게 스레드 풀을 관리합니다.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # --- PyTorch 단일 파일 처리 ---
        if len(files_info) == 1 and not os.path.dirname(files_info[0]['path']):
            file_info = files_info[0]
            chunk_paths = [os.path.join(data_dir, h) for h in file_info['chunks']]
            
            chunk_iterator = executor.map(_read_chunk, chunk_paths)
            
            all_chunk_data_iterator = tqdm(
                chunk_iterator,
                total=len(chunk_paths),
                desc=f"Reassembling {os.path.basename(file_info['path'])}",
                disable=not show_progress,
                unit=' chunks'
            )

            # 1. 인메모리 복원
            if destination_path is None:
                in_memory_file = io.BytesIO(b"".join(all_chunk_data_iterator))
                in_memory_file.seek(0)
                return in_memory_file
            
            # 2. 디스크 파일로 복원
            else:
                if os.path.isdir(destination_path):
                    output_path = os.path.join(destination_path, file_info['path'])
                else:
                    output_path = destination_path
                
                os.makedirs(os.path.dirname(output_path), exist_ok=True)

                with open(output_path, 'wb') as f_out:
                    for chunk_data in all_chunk_data_iterator:
                        f_out.write(chunk_data)
                return output_path
                
        # --- TensorFlow 디렉토리 구조 처리 ---
        else:
            if destination_path is None:
                destination_path = tempfile.mkdtemp()
            
            os.makedirs(destination_path, exist_ok=True)

            iterable = tqdm(files_info, desc="Reassembling TensorFlow model", disable=not show_progress, unit=" file")
            for file_info in iterable:
                dir_name = os.path.dirname(file_info['path'])
                if dir_name:
                    os.makedirs(os.path.join(destination_path, dir_name), exist_ok=True)
                
                chunk_paths = [os.path.join(data_dir, h) for h in file_info['chunks']]
                
                # 각 파일의 청크를 병렬로 읽기
                all_chunk_data = executor.map(_read_chunk, chunk_paths)
                
                output_path = os.path.join(destination_path, file_info['path'])
                with open(output_path, 'wb') as f_out:
                    # 파일 쓰기 시에는 내부 tqdm 없이 바로 조합
                    f_out.write(b"".join(all_chunk_data))
            
            return destination_path

def read_model_snapshot(table_path, version=None, mode='auto', destination_path=None, max_workers=None, show_progress=False):
    """
    모델 스냅샷을 읽어옵니다. 내부적으로 멀티스레딩을 사용하여 I/O를 병렬 처리합니다.
    
    Args:
        max_workers (int, optional): 병렬 I/O에 사용할 스레드 개수. 
                                     None이면 파이썬 기본값(보통 CPU 코어 수 * 5)을 사용합니다.
        show_progress (bool): 진행률 표시 여부. Defaults to False.
    """
    logger = setup_logger()

    # (메타데이터 읽는 부분은 기존과 동일)
    try:
        if version is None:
            pointer_path = os.path.join(table_path, '_current_version.json')
            version_id = read_json(pointer_path)['version_id']
        else:
            version_id = version
        
        metadata_path = os.path.join(table_path, 'metadata', f'v{version_id}.metadata.json')
        metadata = read_json(metadata_path)
        snapshot_path = os.path.join(table_path, metadata['snapshot_filename'])
        snapshot = read_json(snapshot_path)
    except FileNotFoundError as e:
        logger.error(f"읽기 실패: 버전 {version or '(latest)'}의 메타데이터/스냅샷을 찾을 수 없습니다.")
        raise e

    # --- 2. 모드에 따라 '스레드 버전'의 재조립 함수 호출 ---
    
    # [복원 모드]
    if mode == 'restore':
        if not destination_path:
            raise ValueError("mode='restore'를 사용하려면 destination_path를 반드시 지정해야 합니다.")
        logger.info(f"v{version_id} 모델을 '{destination_path}' 경로에 병렬로 복원합니다...")
        result_path = _reassemble_from_chunks_threaded(table_path, snapshot, destination_path, max_workers, show_progress)
        logger.info(f"✅ 복원 완료: {result_path}")
        return result_path

    # [자동 인메모리 로딩 모드]
    elif mode == 'auto':
        logger.info(f"v{version_id} 모델을 메모리로 병렬 로딩합니다...")
        
        is_pytorch = len(snapshot['files']) == 1 and snapshot['files'][0]['path'].endswith(('.pth', '.pt'))
        
        if is_pytorch:
            if not _PYTORCH_AVAILABLE:
                raise ImportError("PyTorch 모델을 로드하려면 'torch' 라이브러리가 필요합니다.")
            
            in_memory_file = _reassemble_from_chunks_threaded(table_path, snapshot, None, max_workers, show_progress)
            model_obj = torch.load(in_memory_file)
            logger.info("✅ PyTorch 모델 로딩 완료.")
            return model_obj
        else: # TensorFlow
            if not _TENSORFLOW_AVAILABLE:
                raise ImportError("TensorFlow 모델을 로드하려면 'tensorflow' 라이브러리가 필요합니다.")

            temp_dir = _reassemble_from_chunks_threaded(table_path, snapshot, None, max_workers, show_progress)
            try:
                model_obj = tf.saved_model.load(temp_dir)
                logger.info("✅ TensorFlow 모델 로딩 완료.")
                return model_obj
            finally:
                shutil.rmtree(temp_dir)
    else:
        raise ValueError(f"지원하지 않는 mode입니다: '{mode}'. 'auto' 또는 'restore'를 사용하세요.")
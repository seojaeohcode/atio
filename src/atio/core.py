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

import uuid
from .utils import read_json, write_json

def write_snapshot(obj, table_path, mode='overwrite', format='parquet', **kwargs):
    logger = setup_logger(debug_level=False)

    # 1. 경로 설정 및 폴더 생성
    os.makedirs(os.path.join(table_path, 'data'), exist_ok=True)
    os.makedirs(os.path.join(table_path, 'metadata'), exist_ok=True)
    
    # 2. 현재 버전 확인
    pointer_path = os.path.join(table_path, '_current_version.json')
    current_version = 0
    if os.path.exists(pointer_path):
        current_version = read_json(pointer_path)['version_id']
    new_version = current_version + 1

    # 3. 임시 디렉토리 내에서 모든 작업 수행
    with tempfile.TemporaryDirectory() as tmpdir:
        # 3a. 새 데이터 파일 쓰기
        writer = get_writer(obj, format)
        data_filename = f"{uuid.uuid4()}.{format}"
        tmp_data_path = os.path.join(tmpdir, data_filename)
        _execute_write(writer, obj, tmp_data_path, **kwargs)

       # 3. 임시 디렉토리 내에서 모든 작업 수행
    with tempfile.TemporaryDirectory() as tmpdir:
        # 3a. 새 데이터 파일 쓰기
        writer = get_writer(obj, format)
        if writer is None:
            raise ValueError(f"지원하지 않는 format: {format} for object type {type(obj)}")
            
        data_filename = f"{uuid.uuid4()}.{format}"
        tmp_data_path = os.path.join(tmpdir, data_filename)
        _execute_write(writer, obj, tmp_data_path, **kwargs)

        # 3b. 새 manifest 생성
        new_manifest = {
            'files': [{'path': os.path.join('data', data_filename), 'format': format}]
        }
        manifest_filename = f"manifest-{uuid.uuid4()}.json"
        write_json(new_manifest, os.path.join(tmpdir, manifest_filename))

        # 3c. 새 snapshot 생성을 위한 준비
        all_manifests = [os.path.join('metadata', manifest_filename)]

        if mode.lower() == 'append' and current_version > 0:
            try:
                prev_metadata_path = os.path.join(table_path, 'metadata', f'v{current_version}.metadata.json')
                prev_metadata = read_json(prev_metadata_path)
                prev_snapshot_filename = prev_metadata['snapshot_filename']
                
                prev_snapshot_path = os.path.join(table_path, prev_snapshot_filename)
                prev_snapshot = read_json(prev_snapshot_path)
                existing_manifests = prev_snapshot['manifests']
                
                all_manifests.extend(existing_manifests)
            except (FileNotFoundError, KeyError):
                logger.warning(f"Append mode: 이전 버전(v{current_version})의 메타데이터를 찾을 수 없거나 형식이 올바르지 않습니다. Overwrite 모드로 동작합니다.")

        # 3d. 최종 manifest 목록으로 새 snapshot 생성
        snapshot_id = int(time.time())
        snapshot_filename = f"snapshot-{snapshot_id}-{uuid.uuid4()}.json"
        
        new_snapshot = {
            'snapshot_id': snapshot_id,
            'timestamp': time.time(),
            'manifests': all_manifests
        }
        write_json(new_snapshot, os.path.join(tmpdir, snapshot_filename))
        
        # 3e. 새 version metadata 생성
        new_metadata = {
            'version_id': new_version,
            'snapshot_id': snapshot_id,
            'snapshot_filename': os.path.join('metadata', snapshot_filename)
        }
        metadata_filename = f"v{new_version}.metadata.json"
        write_json(new_metadata, os.path.join(tmpdir, metadata_filename))

        # 3f. 새 포인터 파일 생성
        new_pointer = {'version_id': new_version}
        tmp_pointer_path = os.path.join(tmpdir, '_current_version.json')
        write_json(new_pointer, tmp_pointer_path)

        # 4. 최종 커밋
        os.rename(tmp_data_path, os.path.join(table_path, 'data', data_filename))
        os.rename(os.path.join(tmpdir, manifest_filename), os.path.join(table_path, 'metadata', manifest_filename))
        os.rename(os.path.join(tmpdir, snapshot_filename), os.path.join(table_path, 'metadata', snapshot_filename))
        os.rename(os.path.join(tmpdir, metadata_filename), os.path.join(table_path, 'metadata', metadata_filename))
        os.replace(tmp_pointer_path, pointer_path)
        logger.info(f"스냅샷 쓰기 완료! '{table_path}'가 버전 {new_version}으로 업데이트되었습니다.")



def read_table(table_path, version=None, output_as='pandas'):
    # 1. 읽을 버전 결정 및 진입점(metadata.json) 찾기
    pointer_path = os.path.join(table_path, '_current_version.json')
    if version is None:
        version_id = read_json(pointer_path)['version_id']
    else:
        version_id = version
    
    metadata_path = os.path.join(table_path, 'metadata', f'v{version_id}.metadata.json')
    metadata = read_json(metadata_path)
    snapshot_filepath = metadata['snapshot_filename']

    # 2. metadata -> snapshot -> manifest 순으로 파싱
    snapshot_path = os.path.join(table_path, snapshot_filepath) # 정확한 경로 사용
    snapshot = read_json(snapshot_path)
    
    # 3. 모든 manifest를 읽어 최종 데이터 파일 목록 취합
    all_data_files = []
    for manifest_ref in snapshot['manifests']:
        manifest_path = os.path.join(table_path, manifest_ref)
        manifest = read_json(manifest_path)
        for file_info in manifest['files']:
            # file_info 에는 path, format 등의 정보가 있음
            all_data_files.append(os.path.join(table_path, file_info['path']))

    # 4. output_as 옵션에 따라 최종 데이터 객체 생성
    if not all_data_files:
        # 데이터가 없는 경우 처리
        return None # 또는 빈 DataFrame

    if output_as == 'pandas':
        import pandas as pd
        return pd.read_parquet(all_data_files)
    elif output_as == 'polars':
        import polars as pl
        return pl.read_parquet(all_data_files)
    # NumPy 등의 다른 형식 처리 로직 추가
    
    raise ValueError(f"지원하지 않는 출력 형식: {output_as}")


from datetime import datetime, timedelta

def expire_snapshots(table_path, keep_for=timedelta(days=7), dry_run=True):
    """
    설정된 보관 기간(keep_for)보다 오래된 스냅샷과
    더 이상 참조되지 않는 데이터 파일을 삭제합니다.
    """
    logger = setup_logger()
    now = datetime.now()
    metadata_dir = os.path.join(table_path, 'metadata')
    
    if not os.path.isdir(metadata_dir):
        logger.info("정리할 테이블이 없거나 메타데이터 폴더를 찾을 수 없습니다.")
        return

    # --- 1. 모든 메타데이터 정보와 파일명 수집 ---
    all_versions_meta = {}      # version_id -> version_meta
    all_snapshots_meta = {}     # snapshot_id -> snapshot_meta
    all_manifest_paths = set()  # 모든 manifest 파일 경로
    
    for filename in os.listdir(metadata_dir):
        path = os.path.join(metadata_dir, filename)
        if filename.startswith('v') and filename.endswith('.metadata.json'):
            meta = read_json(path)
            all_versions_meta[meta['version_id']] = meta
        elif filename.startswith('snapshot-'):
            snap = read_json(path)
            all_snapshots_meta[snap['snapshot_id']] = snap
        elif filename.startswith('manifest-'):
            all_manifest_paths.add(os.path.join('metadata', filename))

    # --- 2. "살아있는" 객체 식별 ---
    live_snapshot_ids = set()
    live_manifests = set()
    live_data_files = set()

    # 현재 버전을 포함하여 보관 기간 내의 모든 버전을 "살아있는" 것으로 간주
    for version_meta in all_versions_meta.values():
        snapshot_id = version_meta['snapshot_id']
        snapshot = all_snapshots_meta.get(snapshot_id)
        
        if snapshot and (now - datetime.fromtimestamp(snapshot['timestamp'])) < keep_for:
            live_snapshot_ids.add(snapshot_id)
            for manifest_ref in snapshot.get('manifests', []):
                live_manifests.add(manifest_ref)
                manifest_path = os.path.join(table_path, manifest_ref)
                if os.path.exists(manifest_path):
                    manifest_data = read_json(manifest_path)
                    for file_info in manifest_data.get('files', []):
                        live_data_files.add(file_info['path'])

    # --- 3. 삭제할 "고아" 객체 식별 ---
    files_to_delete = []

    # 고아 데이터 파일 찾기
    data_dir = os.path.join(table_path, 'data')
    if os.path.isdir(data_dir):
        for data_file in os.listdir(data_dir):
            relative_path = os.path.join('data', data_file)
            if relative_path not in live_data_files:
                files_to_delete.append(os.path.join(table_path, relative_path))

    # 고아 매니페스트 파일 찾기
    manifests_to_delete = all_manifest_paths - live_manifests
    for manifest_path in manifests_to_delete:
        files_to_delete.append(os.path.join(table_path, manifest_path))

    # 고아 스냅샷 및 버전 메타데이터 파일 찾기
    for version_id, version_meta in all_versions_meta.items():
        snapshot_id = version_meta['snapshot_id']
        if snapshot_id not in live_snapshot_ids:
            # vX.metadata.json 파일 삭제 대상 추가
            files_to_delete.append(os.path.join(metadata_dir, f"v{version_id}.metadata.json"))
            # snapshot-X.json 파일 삭제 대상 추가
            snapshot_filename = version_meta.get('snapshot_filename') # 이전 단계에서 이 키를 추가했었음
            if snapshot_filename:
                 files_to_delete.append(os.path.join(table_path, snapshot_filename))
    
    # 중복 제거
    files_to_delete = sorted(list(set(files_to_delete)))

    # --- 4. 최종 삭제 실행 ---
    if not files_to_delete:
        logger.info("삭제할 오래된 파일이 없습니다.")
        return

    logger.info(f"총 {len(files_to_delete)}개의 오래된 파일을 찾았습니다.")
    if dry_run:
        logger.info("[Dry Run] 아래 파일들이 삭제될 예정입니다:")
        for f in files_to_delete:
            print(f"  - {f}")
    else:
        logger.info("오래된 파일들을 삭제합니다...")
        for f in files_to_delete:
            try:
                os.remove(f)
                logger.debug(f"  - 삭제됨: {f}")
            except OSError as e:
                logger.error(f"  - 삭제 실패: {f}, 오류: {e}")
        logger.info("삭제 작업이 완료되었습니다.")

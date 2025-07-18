import os
import tempfile
from .plugins import get_writer
import logging


def write(obj, target_path, format=None, **kwargs):
    """
    데이터 객체(obj)를 안전하게 target_path에 저장합니다.
    format: 'parquet', 'csv' 등
    kwargs: 저장 함수에 전달할 추가 인자
    """
    logger = logging.getLogger("atomicwriter")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    dir_name = os.path.dirname(os.path.abspath(target_path))
    base_name = os.path.basename(target_path)
    with tempfile.TemporaryDirectory(dir=dir_name) as tmpdir:
        tmp_path = os.path.join(tmpdir, base_name)
        logger.info(f"임시 디렉토리 생성: {tmpdir}")
        logger.info(f"임시 파일 경로: {tmp_path}")
        # 플러그인 기반 저장
        writer = get_writer(format)
        if writer is None:
            logger.error(f"지원하지 않는 format: {format}")
            raise ValueError(f"지원하지 않는 format: {format}")
        try:
            writer(obj, tmp_path, **kwargs)
            logger.info(f"데이터 임시 파일에 저장 완료: {tmp_path}")
        except Exception as e:
            logger.error(f"임시 파일 저장 중 예외 발생: {e}")
            # 임시 파일/디렉토리 자동 정리됨
            raise e
        # 원자적 교체
        os.replace(tmp_path, target_path)
        logger.info(f"원자적 교체 완료: {tmp_path} -> {target_path}")
        # _SUCCESS 플래그 파일 생성
        success_path = target_path + "._SUCCESS"
        with open(success_path, "w") as f:
            f.write("OK\n")
        logger.info(f"_SUCCESS 플래그 파일 생성: {success_path}")

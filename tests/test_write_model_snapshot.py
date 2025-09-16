import pytest
import os
import shutil
import tempfile
import torch
import tensorflow as tf
import numpy as np

# atio 라이브러리에서 테스트할 모든 함수를 가져옵니다.
from atio import (
    write_model_snapshot,
    read_model_snapshot,
    delete_version,
    rollback
)

# ------------------- 사용자 설정 -------------------
# TODO: 실제 모델로 테스트하려면 아래 경로를 채워주세요.
# 비워두면 테스트용 더미 모델이 자동으로 생성됩니다.
PATH_TO_YOUR_PYTORCH_MODEL = None  # 예: "C:/models/my_model.pth"
PATH_TO_YOUR_TENSORFLOW_MODEL = None # 예: "C:/models/my_saved_model"
# ----------------------------------------------------

@pytest.fixture(scope="module")
def pytorch_model_path():
    """테스트용 PyTorch 모델(.pth) 파일을 생성하고 경로를 반환하는 Fixture"""
    if PATH_TO_YOUR_PYTORCH_MODEL and os.path.exists(PATH_TO_YOUR_PYTORCH_MODEL):
        yield PATH_TO_YOUR_PYTORCH_MODEL
        return
        
    # 실제 모델 경로가 없으면 더미 모델 생성
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "dummy_model.pth")
        model = torch.nn.Sequential(torch.nn.Linear(10, 2), torch.nn.ReLU())
        torch.save(model.state_dict(), path)
        yield path

@pytest.fixture(scope="module")
def tensorflow_model_path():
    """테스트용 TensorFlow SavedModel을 생성하고 경로를 반환하는 Fixture"""
    if PATH_TO_YOUR_TENSORFLOW_MODEL and os.path.exists(PATH_TO_YOUR_TENSORFLOW_MODEL):
        yield PATH_TO_YOUR_TENSORFLOW_MODEL
        return

    # 실제 모델 경로가 없으면 더미 모델 생성
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "dummy_saved_model")
        model = tf.keras.Sequential([
            tf.keras.layers.Dense(10, input_shape=(10,)),
            tf.keras.layers.ReLU()
        ])
        model.save(path)
        yield path

@pytest.fixture
def table_dir():
    """각 테스트를 위한 임시 atio 테이블 디렉토리를 생성하는 Fixture"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


# --- 모델 스냅샷 기능 테스트 ---

def test_save_and_restore_pytorch(table_dir, pytorch_model_path):
    """시나리오 1: PyTorch 모델 저장 및 파일로 복원(restore) 테스트"""
    # 1. 모델 저장
    write_model_snapshot(pytorch_model_path, table_dir)
    
    # 2. 파일로 복원
    restore_path = os.path.join(table_dir, "restored.pth")
    result_path = read_model_snapshot(
        table_dir, 
        version=1, 
        mode='restore', 
        destination_path=restore_path
    )
    
    # 3. 검증
    assert result_path == restore_path
    assert os.path.exists(restore_path)
    # 원본과 복원된 파일의 크기가 같은지 비교 (간단한 검증)
    assert os.path.getsize(pytorch_model_path) == os.path.getsize(restore_path)

def test_save_and_load_pytorch_auto(table_dir, pytorch_model_path):
    """시나리오 2: PyTorch 모델 저장 및 메모리로 로딩(auto) 테스트"""
    write_model_snapshot(pytorch_model_path, table_dir)
    
    # auto 모드로 메모리에 바로 로드
    loaded_model_obj = read_model_snapshot(table_dir, version=1, mode='auto')
    
    # state_dict (OrderedDict) 타입인지 검증
    assert isinstance(loaded_model_obj, dict)

def test_save_and_restore_tensorflow(table_dir, tensorflow_model_path):
    """시나리오 3: TensorFlow 모델 저장 및 디렉토리로 복원(restore) 테스트"""
    write_model_snapshot(tensorflow_model_path, table_dir)
    
    restore_path = os.path.join(table_dir, "restored_tf_model")
    result_path = read_model_snapshot(
        table_dir, 
        version=1, 
        mode='restore', 
        destination_path=restore_path
    )
    
    assert result_path == restore_path
    assert os.path.exists(os.path.join(restore_path, "saved_model.pb"))

def test_deduplication_efficiency(table_dir, pytorch_model_path):
    """시나리오 4: 모델 일부만 변경 시 데이터 중복 제거(Deduplication) 효율성 테스트"""
    data_dir = os.path.join(table_dir, "data")

    # v1 저장
    write_model_snapshot(pytorch_model_path, table_dir)
    chunks_v1 = set(os.listdir(data_dir))
    
    # 모델 가중치 일부만 변경
    state_dict = torch.load(pytorch_model_path)
    # 한 레이어의 가중치 하나만 변경
    state_dict['0.weight'][0, 0] = 999.0 
    
    # 변경된 모델을 새 임시 파일에 저장
    with tempfile.NamedTemporaryFile(suffix=".pth", delete=False) as tmp:
        modified_model_path = tmp.name
    torch.save(state_dict, modified_model_path)

    # v2 저장
    write_model_snapshot(modified_model_path, table_dir)
    chunks_v2 = set(os.listdir(data_dir))
    
    os.remove(modified_model_path)

    # 검증: 새로 추가된 청크의 수가 매우 적어야 함
    new_chunks = chunks_v2 - chunks_v1
    print(f"v1 청크 수: {len(chunks_v1)}, v2 전체 청크 수: {len(chunks_v2)}, 새로 추가된 청크 수: {len(new_chunks)}")
    assert len(new_chunks) < 5  # 아주 작은 수의 청크만 추가되었는지 확인
    assert len(chunks_v2) < len(chunks_v1) * 1.1 # 전체 청크 수가 10% 이상 늘지 않았는지 확인

def test_full_lifecycle_management(table_dir, pytorch_model_path):
    """시나리오 5: rollback, delete 등 전체 관리 기능 테스트"""
    # 버전 2개 생성
    write_model_snapshot(pytorch_model_path, table_dir) # v1
    write_model_snapshot(pytorch_model_path, table_dir) # v2
    

    # 최신 버전(v2) 삭제 시도 -> 실패해야 함
    assert not delete_version(table_dir, version_id=2)
    
    # v1로 롤백
    assert rollback(table_dir, version_id=1)
    
    # 이제 v2 삭제 -> 성공해야 함
    assert delete_version(table_dir, version_id=2)
    
    # 삭제된 v2 읽기 시도 -> 실패해야 함
    with pytest.raises(FileNotFoundError):
        read_model_snapshot(table_dir, version=2)
        
    # 남아있는 v1은 정상적으로 읽어져야 함
    model_obj = read_model_snapshot(table_dir, version=1, mode='auto')
    assert model_obj is not None

def test_error_unsupported_format(table_dir):
    """시나리오 6: 지원하지 않는 파일 형식에 대해 오류를 내는지 테스트"""
    with tempfile.NamedTemporaryFile(mode='w', suffix=".txt", delete=False) as tmp:
        tmp.write("this is not a model")
        txt_path = tmp.name

    with pytest.raises(ValueError, match="지원하지 않는 모델 형식입니다"):
        write_model_snapshot(txt_path, table_dir)
        
    os.remove(txt_path)
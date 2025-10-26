#!/usr/bin/env python3

# Copyright 2025 Atio Developers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Atio Model Snapshot 기능 테스트 스크립트
(write_model_snapshot, read_model_snapshot, rollback, delete_version)
"""

# --- 모듈 경로 설정을 위한 코드 ---
import sys
import os
import shutil
import time
import tempfile # TensorFlow 로딩 시 임시 폴더 사용

# 현재 스크립트의 경로를 기준으로 'src' 폴더의 절대 경로를 계산합니다.
project_root = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ------------------------------------

import atio
# 데이터 관련 라이브러리(pandas, numpy 등)는 여기서 필요 없음

# --- 선택적 라이브러리 임포트 (모델 테스트용) ---
try:
    import torch
    _TORCH_AVAILABLE = True
except ImportError:
    _TORCH_AVAILABLE = False
    print("⚠️  PyTorch가 설치되지 않았습니다. 모델 스냅샷(PyTorch) 테스트를 건너뜁니다.")

try:
    import tensorflow as tf
    _TENSORFLOW_AVAILABLE = True
except ImportError:
    _TENSORFLOW_AVAILABLE = False
    print("⚠️  TensorFlow가 설치되지 않았습니다. 모델 스냅샷(TensorFlow) 테스트를 건너뜁니다.")


# --- 전역 테스트 설정 ---
TEST_OUTPUT_DIR = "atio_model_snapshot_tests" # 디렉토리 이름 변경
MODEL_SNAPSHOT_PATH = os.path.join(TEST_OUTPUT_DIR, "model_table")
MODEL_RESTORE_PATH = os.path.join(TEST_OUTPUT_DIR, "model_restore")
# 데이터 스냅샷 경로는 필요 없음

# --- 헬퍼 함수 ---
def print_test_header(title):
    print("\n" + "=" * 60)
    print(f"🧪 테스트 시작: {title}")
    print("=" * 60)

def print_test_result(success, message=""):
    if success:
        print(f"  ✅ [성공] {message}")
    else:
        print(f"  ❌ [실패] {message}")

def setup_test_environment():
    """테스트 실행 전 테스트 디렉토리를 정리하고 생성합니다."""
    print(f"🔧 테스트 환경 설정: '{TEST_OUTPUT_DIR}' 디렉토리 정리 및 생성...")
    if os.path.exists(TEST_OUTPUT_DIR):
        shutil.rmtree(TEST_OUTPUT_DIR)
    os.makedirs(TEST_OUTPUT_DIR, exist_ok=True)
    # os.makedirs(MODEL_RESTORE_PATH, exist_ok=True) # 모델 복원 경로는 필요
    print("✅ 테스트 환경 준비 완료.")

def create_dummy_models():
    """테스트에 사용할 더미 모델 파일(.pth, saved_model)을 생성합니다."""
    print("🔧 더미 모델 파일 생성 중...")
    # 1. PyTorch 더미 모델 생성
    if _TORCH_AVAILABLE:
        try:
            pth_path = os.path.join(TEST_OUTPUT_DIR, "dummy_model_v1.pth")
            state_dict = {'layer1.weight': torch.randn(10, 5), 'layer1.bias': torch.randn(10)}
            torch.save(state_dict, pth_path)
            print(f"  ✅ PyTorch 더미 모델 생성 완료: {pth_path}")
        except Exception as e:
            print(f"  ❌ PyTorch 더미 모델 생성 실패: {e}")
    # 2. TensorFlow 더미 모델 생성
    if _TENSORFLOW_AVAILABLE:
        try:
            tf_path = os.path.join(TEST_OUTPUT_DIR, "dummy_model_tf_v1")
            class SimpleModule(tf.Module):
                def __init__(self, name=None):
                    super().__init__(name=name)
                    self.v = tf.Variable(5.0, name="v1")
                @tf.function
                def __call__(self, x):
                    return self.v * x
            model = SimpleModule()
            tf.saved_model.save(model, tf_path)
            print(f"  ✅ TensorFlow 더미 모델 생성 완료: {tf_path}")
        except Exception as e:
            print(f"  ❌ TensorFlow 더미 모델 생성 실패: {e}")

# --- 모델 스냅샷 테스트 함수 ---

def test_model_snapshot_lifecycle():
    """모델 스냅샷의 전체 생명주기(생성, 읽기, 롤백, 삭제)를 테스트합니다."""
    print_test_header("모델 스냅샷 생명주기 (종합 테스트)")
    if not _TORCH_AVAILABLE and not _TENSORFLOW_AVAILABLE:
        print("  ⚠️  Torch와 TensorFlow가 모두 없어 모델 스냅샷 테스트를 건너뜁니다.")
        return
    try:
        # --- 1. write_model_snapshot (PyTorch) ---
        if _TORCH_AVAILABLE:
            print("\n  [1] write_model_snapshot (PyTorch)")
            pth_path = os.path.join(TEST_OUTPUT_DIR, "dummy_model_v1.pth")
            if not os.path.exists(pth_path):
                print_test_result(False, "PyTorch 더미 모델 파일 없음.")
                return
            atio.write_model_snapshot(pth_path, MODEL_SNAPSHOT_PATH, show_progress=True)
            print_test_result(True, "v1 (PyTorch) 생성")
        else:
            print("\n  [1] write_model_snapshot (PyTorch) - 건너뜀")

        # --- 2. write_model_snapshot (TensorFlow) ---
        if _TENSORFLOW_AVAILABLE:
            print("\n  [2] write_model_snapshot (TensorFlow)")
            tf_path = os.path.join(TEST_OUTPUT_DIR, "dummy_model_tf_v1")
            if not os.path.exists(tf_path):
                print_test_result(False, "TensorFlow 더미 모델 파일 없음.")
                return
            atio.write_model_snapshot(tf_path, MODEL_SNAPSHOT_PATH, show_progress=True)
            print_test_result(True, "v2 (TensorFlow) 생성")
        else:
            print("\n  [2] write_model_snapshot (TensorFlow) - 건너뜀")

        # --- 5. rollback() ---
        print("\n  [5] rollback() - v2 -> v1 롤백")
        if _TORCH_AVAILABLE and _TENSORFLOW_AVAILABLE:
            atio.rollback(MODEL_SNAPSHOT_PATH, version_id=1)
            model_after_rollback = atio.read_model_snapshot(MODEL_SNAPSHOT_PATH, mode='auto')
            assert isinstance(model_after_rollback, dict)
            print_test_result(True, "v1 롤백 성공 (최신 버전이 v1가 됨)")
        else:
            print("  - 롤백 테스트는 PyTorch와 TensorFlow 스냅샷이 모두 필요하므로 건너뜁니다.")

        # --- 6. delete_version() ---
        print("\n  [6] delete_version() - v2 삭제")
        if _TORCH_AVAILABLE and _TENSORFLOW_AVAILABLE:
            delete_result = atio.delete_version(MODEL_SNAPSHOT_PATH, version_id=2)
            assert delete_result is True
            print_test_result(True, "v2 (비활성 버전) 삭제 성공")
            try:
                atio.read_model_snapshot(MODEL_SNAPSHOT_PATH, version=2, mode='auto')
                print_test_result(False, "삭제된 v2 읽기 시도 (오류가 발생해야 함)")
            except FileNotFoundError:
                print_test_result(True, "삭제된 v2 읽기 시도 (예상대로 FileNotFoundError 발생)")
            except Exception:
                print_test_result(True, "삭제된 v2 읽기 시도 (예상대로 오류 발생)")
        else:
            print("  - 삭제 테스트는 PyTorch와 TensorFlow 스냅샷이 모두 필요하므로 건너뜁니다.")

    except Exception as e:
        print_test_result(False, f"모델 스냅샷 테스트 중 예기치 않은 오류: {e}")
        import traceback
        traceback.print_exc()

# --- 파일 정리 함수 ---
def cleanup_demo_files():
    """테스트 실행 후 생성된 파일들을 정리합니다."""
    print("\n" + "=" * 60)
    print("🧹 테스트 파일 정리")
    print("=" * 60)
    if not os.path.exists(TEST_OUTPUT_DIR):
        print(f"🗑️ 정리할 디렉토리가 없습니다: '{TEST_OUTPUT_DIR}'")
        return
    print(f"🗑️ 생성된 테스트 디렉토리: '{TEST_OUTPUT_DIR}'")
    print("\n❓ 테스트 디렉토리를 삭제하시겠습니까? (y/n): ", end="")
    try:
        response = input().lower().strip()
    except (EOFError, KeyboardInterrupt):
        response = 'n'
        print("\n입력 없이 종료하여 파일을 보존합니다.")
    if response == 'y':
        try:
            shutil.rmtree(TEST_OUTPUT_DIR)
            print(f"\n✅ '{TEST_OUTPUT_DIR}' 디렉토리와 모든 내용이 삭제되었습니다.")
        except Exception as e:
            print(f"\n❌ 디렉토리 삭제 중 오류 발생: {e}")
    else:
        print(f"\n📁 '{TEST_OUTPUT_DIR}' 디렉토리가 보존되었습니다.")

# --- 메인 실행 함수 ---
def main():
    """모델 스냅샷 기능 테스트 실행 함수"""
    try:
        setup_test_environment()
        create_dummy_models() # 모델 스냅샷 테스트는 더미 모델 생성이 필요
        # --- 모델 스냅샷 테스트 호출 ---
        test_model_snapshot_lifecycle()
    except Exception as e:
        print(f"\n" + "!" * 60)
        print(f" CRITICAL: 테스트 실행 중 치명적인 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        print("!" * 60)
    finally:
        cleanup_demo_files()
    print("\n" + "=" * 60)
    print("🎉 Atio Model Snapshot 기능 테스트 완료!")
    print("=" * 60)

if __name__ == "__main__":
    main()
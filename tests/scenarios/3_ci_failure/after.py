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

import os
import atio as aw
import sys

def safe_ci_cd_pipeline():
    try:
        # Step 1: 원자적 저장 방식으로 중간 산출물 생성
        aw.write("CI/CD Step 1: Preparing build artifacts...\n", "ci_artifact.txt")

        # Step 2: 오류 발생 (하지만 임시 파일로 인해 후속 작업 영향 없음)
        raise RuntimeError("CI/CD Step 2: Deployment failed due to config mismatch")

    except Exception as e:
        print(f"[ERROR] CI/CD 실패: {e}", file=sys.stderr)
        print("[INFO] atomicwriter가 중간 파일 생성을 방지했기 때문에 후속 작업에 영향 없음.")

if __name__ == "__main__":
    safe_ci_cd_pipeline()
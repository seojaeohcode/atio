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
import sys

def broken_ci_cd_pipeline():
    try:
        # Step 1: CI/CD 중간 산출물 생성
        with open("ci_artifact.txt", "w") as f:
            f.write("CI/CD Step 1: Preparing build artifacts...\n")
        
        # Step 2: 중간에 오류 발생
        raise RuntimeError("CI/CD Step 2: Deployment failed due to config mismatch")

    except Exception as e:
        print(f"[ERROR] CI/CD 실패: {e}", file=sys.stderr)
        print("[WARNING] 불완전한 파일이 저장되었을 수 있습니다. 후속 작업에 영향을 줄 수 있습니다.")

if __name__ == "__main__":
    broken_ci_cd_pipeline()
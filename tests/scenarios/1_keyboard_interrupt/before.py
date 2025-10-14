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

import pandas as pd
import time
import signal
import sys

# 시그널 핸들러 설정 (테스트 편의를 위해)
def handler(signum, frame):
    print("\n[!] KeyboardInterrupt (Ctrl+C) 감지됨. 종료 중...")
    sys.exit(1)

signal.signal(signal.SIGINT, handler)

# 예시 데이터 생성
df = pd.DataFrame({"a": range(100000000)})

print("[INFO] 5초 후 Parquet 파일 저장 시작.")
time.sleep(5)

# 파일 저장 (문제 코드)
print("[INFO] 저장 시작: data/output.parquet / 지금 Ctrl+C로 중단해 보세요...")
df.to_parquet("data/output.parquet")
print("[SUCCESS] 저장 완료")
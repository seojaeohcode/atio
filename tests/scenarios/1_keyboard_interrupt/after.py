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
import atio as aw

# Ctrl+C 감지 시 사용자 메시지 출력
def handler(signum, frame):
    print("\n[!] KeyboardInterrupt 감지됨. 안전하게 종료 중...")
    sys.exit(1)

signal.signal(signal.SIGINT, handler)

# 데이터 준비
df = pd.DataFrame({"a": range(10000000)})

print("[INFO] 5초 후 AtomicWriter를 사용한 저장 시작")
time.sleep(5)

# 핵심: atomicwriter 사용
print("[INFO] Atomic 저장 시작 / 지금 Ctrl+C로 중단해 보세요...")
aw.write(df, "data/output.parquet", format="parquet")
print("[SUCCESS] Atomic 저장 완료")

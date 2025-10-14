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
import numpy as np
import os

print("[INFO] 대용량 DataFrame 생성 중...")
# 약 8~10GB짜리 DataFrame (환경에 따라 조정 필요)
df = pd.DataFrame(np.random.rand(10_000_000, 1000))

os.makedirs("data", exist_ok=True)
print("[INFO] 저장 시작: data/oom_output.parquet")

# 💥 메모리 부족 시 실패 → 깨진 파일 남음
df.to_parquet("data/oom_output.parquet")

print("[SUCCESS] 저장 완료")
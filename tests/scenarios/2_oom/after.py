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
import atio as aw
import os

print("[INFO] 대용량 DataFrame 생성 중...")
df = pd.DataFrame(np.random.rand(10_000_000, 1000))

os.makedirs("data", exist_ok=True)
print("[INFO] atomicwriter 사용하여 저장 시작: data/oom_output.parquet")

# ✅ atomicwriter 사용: 실패 시 원본 보존, 깨진 파일 방지
aw.write(df, "data/oom_output.parquet", format="parquet")

print("[SUCCESS] atomicwriter 저장 완료")
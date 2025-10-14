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
from atio import write

# 대용량 더미 데이터 생성
print("Creating a large dummy DataFrame...")
size_mb = 2048
df = pd.DataFrame(np.random.rand(int(size_mb * 131072), 1), columns=['value'])
print(f"Dummy DataFrame created (approx. {size_mb}MB in memory).")

# 새로운 기능 테스트
write(
    df,
    "./tests/output_data/large_data.parquet",
    format="parquet",
    show_progress=True  # 이 옵션 하나로 진행도 표시 기능 활성화
)
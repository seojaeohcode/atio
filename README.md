# Atio 🛡️

안전하고 **원자적인 파일 쓰기**를 지원하는 경량 Python 라이브러리입니다.  
Pandas, Polars, NumPy 등 데이터 객체 저장 시 **파일 손상 없이**, **트랜잭션처럼 안전하게 처리**할 수 있습니다.

---

## 🌟 주요 기능

- ✅ 임시 디렉토리 스테이징 후 **원자적 파일 교체**  
- 📦 Pandas, Polars, NumPy 등 다양한 데이터 객체 지원  
- 📍 `_SUCCESS` 플래그 파일 생성 — 저장 완료 여부 표시  
- 🛠 실패 시 **원본 파일 보존**, 임시 파일 자동 정리  
- 🧩 플러그인 아키텍처로 **확장성 좋음**
- 🔍 **성능 진단 로깅** — 각 단계별 실행 시간 측정 및 병목점 분석

---

## 🔍 성능 진단 로깅 (NEW!)

Atio는 이제 **성능 진단 로깅** 기능을 제공합니다. `verbose=True` 옵션을 사용하면 각 단계별 실행 시간을 측정하여 병목점을 정확히 파악할 수 있습니다.

### 기본 사용법 (간단한 정보만):
```python
import atio as aw
import pandas as pd

df = pd.DataFrame({"a": [1, 2, 3]})

# 기본 사용법 - 간단한 성공/실패 정보만
aw.write(df, "output.parquet", format="parquet")
```

**출력 예시:**
```
[INFO] 임시 디렉토리 생성: /tmp/tmp_xxx
[INFO] 임시 파일 경로: /tmp/tmp_xxx/output.parquet
[INFO] 사용할 writer: to_parquet (format: parquet)
[INFO] 데이터 임시 파일에 저장 완료: /tmp/tmp_xxx/output.parquet
[INFO] 원자적 교체 완료: /tmp/tmp_xxx/output.parquet -> output.parquet
[INFO] _SUCCESS 플래그 파일 생성: output.parquet._SUCCESS
[INFO] Atomic write completed successfully (took 0.2359s)
```

### 상세 진단 모드 (verbose=True):
```python
# 상세한 성능 진단 정보 출력
aw.write(df, "output.parquet", format="parquet", verbose=True)
```

**출력 예시:**
```
[INFO] 임시 디렉토리 생성: /tmp/tmp_xxx
[INFO] 임시 파일 경로: /tmp/tmp_xxx/output.parquet
[INFO] 사용할 writer: to_parquet (format: parquet)
[INFO] 데이터 임시 파일에 저장 완료: /tmp/tmp_xxx/output.parquet
[INFO] 원자적 교체 완료: /tmp/tmp_xxx/output.parquet -> output.parquet
[INFO] _SUCCESS 플래그 파일 생성: output.parquet._SUCCESS
[DEBUG] Atomic write step timings (SUCCESS): setup=0.0012s, write_call=0.2345s, replace=0.0001s, success_flag=0.0001s, total=0.2359s
```

### 오류 발생 시 (기본 사용법):
```
[INFO] 임시 디렉토리 생성: /tmp/tmp_xxx
[INFO] 임시 파일 경로: /tmp/tmp_xxx/output.parquet
[INFO] 사용할 writer: to_parquet (format: parquet)
[ERROR] 임시 파일 저장 중 예외 발생: [Errno 28] No space left on device
[INFO] Atomic write failed during write stage (took 0.1246s, error: OSError)
```

### 오류 발생 시 (verbose=True):
```
[INFO] 임시 디렉토리 생성: /tmp/tmp_xxx
[INFO] 임시 파일 경로: /tmp/tmp_xxx/output.parquet
[INFO] 사용할 writer: to_parquet (format: parquet)
[ERROR] 임시 파일 저장 중 예외 발생: [Errno 28] No space left on device
[DEBUG] Atomic write step timings (ERROR during write): setup=0.0012s, write_call=0.1234s (실패), replace=N/A, success_flag=N/A, total=0.1246s, error_type=OSError
```

**측정되는 단계:**
- `setup`: 임시 폴더 생성 및 초기 설정
- `write_call`: 실제 데이터 쓰기 함수 호출 (대부분의 시간 소요)
- `replace`: 원자적 파일 교체
- `success_flag`: _SUCCESS 플래그 파일 생성
- `total`: 전체 작업 시간

**지원하는 오류 상황:**
- ✅ **KeyboardInterrupt**: 인터럽트 발생 시점과 소요 시간 표시
- ✅ **권한 오류**: 파일 시스템 권한 문제 진단
- ✅ **디스크 공간 부족**: 저장 공간 부족 상황 진단
- ✅ **메모리 부족**: 메모리 압박 상황 진단
- ✅ **네트워크 오류**: 네트워크 드라이브 접근 문제 진단
- ✅ **지원하지 않는 형식**: 잘못된 파일 형식 지정 시 진단
- ✅ **동시 접근 오류**: 멀티스레딩 환경에서의 충돌 진단

**장점:**
- 🎯 **정확한 병목점 파악**: Atio 오버헤드 vs 실제 쓰기 작업 시간 구분
- 🔧 **성능 최적화 가이드**: 어느 단계에서 시간이 많이 소요되는지 명확히 표시
- 🐛 **디버깅 시간 단축**: 문제의 원인을 빠르게 파악 가능
- 📊 **성능 모니터링**: 대용량 데이터 처리 시 성능 추적
- 🚨 **오류 진단**: 실패 상황에서도 정확한 원인과 발생 시점 파악

---

## 🧠 왜 이 도구가 정말 중요한가요?

NumPy나 Pandas는 데이터 분석에서는 최적이지만, **파일로 저장할 때는 아래와 같은 위험**이 있습니다:

1. **파일 일부만 저장되어 깨질 수 있음** — 강제 종료나 오류 시
2. **동시 쓰기 충돌** — 멀티프로세스 환경에서 파일이 엉킬 수 있음
3. **플랫폼 간 동작 차이** — Windows와 Linux/macOS에서 파일 시스템 동작이 다름

AtomicWriter는 임시 파일에 쓰고 **단일 `rename()`/`replace()` 작업으로 교체**합니다.  
이 방식은 **“완전히 저장되거나 전혀 저장되지 않는”** 원자성(atomicity)을 보장하며,  
- POSIX: `os.replace` (atomic), `fsync`  
- Windows: `MoveFileEx`, `Commit`  
를 활용하여 파일이 **항상 일관된 상태**를 유지하도록 합니다 :contentReference[oaicite:1]{index=1}.

---

## ⚙️ 설치

```bash
pip install atomicwriter

## 🛠️ 사용 예제

```python
import atomicwriter as aw
import pandas as pd

df = pd.DataFrame({"a": [1, 2, 3]})

# 기본 사용법
aw.write(df, "output.parquet", format="parquet")
# │→ 임시 파일 작성 → 원자적 교체 → _SUCCESS 생성
# │→ 실패 시 원본 보존, 임시 파일 자동 정리

# 상세 성능 진단 로깅 활성화
aw.write(df, "output_verbose.parquet", format="parquet", verbose=True)
# │→ 각 단계별 실행 시간 측정 및 로그 출력

# 진행도 표시와 함께 사용
aw.write(df, "output_progress.parquet", format="parquet", show_progress=True)
# │→ 실시간 진행도 표시

# 모든 옵션 조합
aw.write(df, "output_full.parquet", format="parquet", 
         verbose=True, show_progress=True)
# │→ 성능 진단 + 진행도 표시
```

## 💡 빅데이터 워크플로우에서 활용 시나리오

| 시나리오               | 해결 방법                   | 장점                    |
|------------------------|-----------------------------|-------------------------|
| Pandas → CSV 저장      | 임시 파일에 기록 후 교체    | CSV 파일 깨짐 방지      |
| 멀티프로세스 병렬 쓰기 | atomic replace 방식 사용    | 충돌 없는 안전 저장     |
| 데이터 파이프라인 작업 | 저장 성공 시 `_SUCCESS` 확인 | 데이터 완전성 보장      |

---

## 🔄 비교 – 유사 라이브러리 특징 정리

### [python-atomicwrites](https://github.com/untitaker/python-atomicwrites)
- 간편한 API
- Windows 지원
- 크로스 플랫폼 호환

### atomicwriter (본 프로젝트)
- ✅ 경량
- ✅ 플러그인 아키텍처
- ✅ Pandas / Polars / Numpy 등 데이터 객체 중심 저장 지원

---

## ✅ 라이선스

Apache 2.0 — 기업 및 커뮤니티 모두 자유롭게 사용 가능

---

## ✨ 요약

**AtomicWriter**는 분석만큼 중요한 **“저장” 단계를 안전하게 처리**하는 도구입니다.

특히 데이터 무결성이 중요한 환경에서  
(예: 머신러닝 배치, 멀티프로세스 분석, 중요 로그 저장 등)  
**작지만 강력한 해결책**을 제공합니다.

📘 시나리오 1: Pandas CSV 저장 중 작업 중단
문제 상황:
한 사용자가 Pandas로 대용량 분석 결과를 .csv 파일로 저장하던 중, 예상치 못한 전원 차단이나 커널 강제 종료가 발생했습니다.
결과 파일은 50MB 중 3MB만 저장된 채 손상되었고, 이후 읽기도 되지 않았습니다.

AtomicWriter로 해결:
임시 파일에 먼저 기록 후, 모든 쓰기가 성공해야만 원본과 교체됩니다.
따라서 중간에 꺼져도 기존 파일은 보존되고, 손상된 임시 파일은 자동 정리되어 안정성을 확보할 수 있습니다.

📘 시나리오 2: 멀티프로세스 환경에서 경쟁 조건(Race Condition)
문제 상황:
Python multiprocessing 기반 데이터 수집 파이프라인에서 여러 프로세스가 동시에 같은 파일을 저장하며 충돌이 발생했습니다.
결과적으로 로그 파일이 덮어쓰여 누락되거나, 일부 JSON 파일은 파싱할 수 없는 손상된 형태로 저장됐습니다.

AtomicWriter로 해결:
파일 쓰기를 atomic replace 방식으로 수행하면, 한 번에 하나의 프로세스만 최종 경로로 이동할 수 있습니다.
이로써 경쟁 조건 없이 충돌 없이 저장이 보장됩니다.

📘 시나리오 3: 데이터 파이프라인 검증 불가
문제 상황:
ETL 작업에서 .parquet 저장이 완료됐는지 여부를 자동 시스템이 판단할 수 없어, 손상되거나 미완성된 데이터를 다음 단계에서 그대로 사용했습니다.
결과적으로 모델 학습 데이터에 결측값이 포함되어 품질 저하가 발생했습니다.

AtomicWriter로 해결:
저장이 성공적으로 완료된 경우에만 _SUCCESS 플래그 파일을 함께 생성하도록 설정할 수 있습니다.
후속 단계는 _SUCCESS 유무를 기준으로 안전하게 파이프라인을 구동할 수 있습니다.

📘 시나리오 4: Polars DataFrame을 S3로 저장 중 오류 발생
문제 상황:
Polars DataFrame을 AWS S3에 직접 저장하는 중간에 ConnectionError가 발생하여 S3에는 부분적으로 깨진 .parquet 파일이 올라갔습니다.
다음 번 실행에서 이 파일을 재사용하려 했지만, S3에서 파일이 손상된 채로 존재해 오류를 유발했습니다.

AtomicWriter로 해결:
로컬 임시 파일에 완전히 저장된 후에만 S3 업로드 또는 교체가 수행됩니다.
네트워크 이슈나 디스크 오류에도 최종 파일은 항상 완전한 상태로만 존재하게 됩니다.
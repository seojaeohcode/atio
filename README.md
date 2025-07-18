# AtomicWriter

안전하고 원자적인(atomic) 파일 쓰기를 지원하는 경량 Python 라이브러리입니다. Pandas, Polars, Numpy 등 다양한 데이터 객체의 파일 저장 시 파일 손상 없이 트랜잭션처럼 안전하게 처리합니다.

## 주요 기능
- 임시 디렉토리 스테이징 후 원자적 파일 교체
- Pandas, Polars, Numpy 등 다양한 객체 지원
- _SUCCESS 플래그 파일 생성
- 실패 시 원본 파일 보존, 임시 파일 자동 정리
- 플러그인 아키텍처 및 확장성

## 설치
```bash
pip install atomicwriter
```

## 간단 사용 예제
```python
import atomicwriter as aw
import pandas as pd

df = pd.DataFrame({"a": [1,2,3]})
aw.write(df, "output.parquet", format="parquet")
```

## 라이선스
Apache 2.0 
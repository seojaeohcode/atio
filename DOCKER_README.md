# 🐳 Atio 벤치마크 Docker 환경

## 📋 개요

Atio 라이브러리의 쓰기 성능을 Docker 환경에서 테스트하기 위한 간단한 설정입니다.

## 🚀 빠른 시작

### Docker 사용
```bash
# 이미지 빌드
docker build -t atio-benchmark .

# 벤치마크 실행
docker run --rm atio-benchmark
```

### 한 번에 실행
```bash
# 빌드 + 실행을 한 번에
docker run --rm $(docker build -q .)
```

## 📁 파일 구조

```
atio/
├── Dockerfile                    # Docker 이미지 정의
├── benchmark_write_speed.py     # 벤치마크 실행 스크립트
├── requirements.txt             # Python 의존성
└── src/                        # Atio 소스 코드
```

## 🔧 Docker 환경 구성

### Dockerfile
- **Base Image**: Python 3.11-slim
- **의존성**: requirements.txt 기반 설치
- **실행**: 벤치마크 스크립트 자동 실행

## 🎯 사용법

1. **벤치마크 실행**
   ```bash
   docker build -t atio-benchmark .
   docker run --rm atio-benchmark
   ```

2. **결과 확인**
   - 콘솔에서 직접 결과 확인
   - 컨테이너가 종료되면 결과도 함께 사라짐

3. **개발 모드** (필요시)
   ```bash
   # 컨테이너 내부 접속
   docker run -it --rm atio-benchmark /bin/bash
   ```

## 💡 장점

- **표준화된 환경**: 어떤 시스템에서든 동일한 결과
- **간단한 설정**: 복잡한 환경 설정 불필요
- **빠른 실행**: 한 번의 명령으로 전체 벤치마크 실행
- **격리된 환경**: 호스트 시스템에 영향 없음

---

**💡 팁**: 이 Docker 환경을 사용하면 기능 명세서에 포함할 표준화된 벤치마크 결과를 얻을 수 있습니다!

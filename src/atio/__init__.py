"""
Atio: 안전한 원자적 파일 쓰기 라이브러리
"""

__version__ = "1.0.0"

# Public API로 노출할 함수들을 명시적으로 가져옵니다.
from .core import write
from .plugins import register_writer

# 향후 open, atomic_output 등도 여기에 추가 예정

"""
AtomicWriter: 안전한 원자적 파일 쓰기 라이브러리
"""

from .core import write
from .plugins import register_writer

# 향후 open, atomic_output 등도 여기에 추가 예정

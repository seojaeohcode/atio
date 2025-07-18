from setuptools import setup, find_packages

setup(
    name="atomicwriter",
    version="0.1.0",
    description="Atomic file writer for Pandas, Polars, Numpy, etc.",
    author="Your Name",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "pyarrow",
        # "polars",  # 선택적
        # "fsspec",  # 선택적
    ],
    python_requires=">=3.7",
    license="Apache-2.0",
)

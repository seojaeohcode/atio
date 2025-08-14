<div align="center">

<img src="https://img.shields.io/badge/ATIO-3776AB?style=for-the-badge&logo=python&logoColor=white" width="200" />

<b>Python library for safe atomic file writing and database writing</b><br>
<b>🚀 `pip install atio`</b>

[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![PyPI](https://img.shields.io/badge/PyPI-2.1.0-orange.svg)](https://pypi.org/project/atio/)
[![Documentation](https://img.shields.io/badge/Documentation-Read%20the%20Docs-blue.svg)](https://seojaeohcode.github.io/atio/)

</div>

---

## 📖 Overview

Atio is a Python library that prevents data loss and ensures safe file writing. Through atomic writing, it protects existing data even when errors occur during file writing, and supports various data formats and database connections.

### ✨ Key Features

- 🔒 **Atomic File Writing**: Safe writing using temporary files
- 📊 **Multiple Format Support**: CSV, Parquet, Excel, JSON, etc.
- 🗄️ **Database Support**: Direct SQL and Database writing
- 📈 **Progress Display**: Progress monitoring for large data processing
- 🔄 **Rollback Function**: Automatic recovery when errors occur
- 🎯 **Simple API**: Intuitive and easy-to-use interface
- 📋 **Version Management**: Snapshot-based data version management
- 🧹 **Auto Cleanup**: Automatic deletion of old data

## 🚀 Installation

```bash
pip install atio
```

## 📚 Usage

### `atio.write()` - Basic File/Database Writing

**Purpose**: Save data to a single file or database

**Key Parameters**:
- `obj`: Data to save (pandas.DataFrame, polars.DataFrame, numpy.ndarray)
- `target_path`: File save path (required for file writing)
- `format`: Save format ('csv', 'parquet', 'excel', 'json', 'sql', 'database')
- `show_progress`: Whether to display progress
- `verbose`: Whether to output detailed performance information

#### Basic File Writing

```python
import atio
import pandas as pd

df = pd.DataFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "city": ["Seoul", "Busan", "Incheon"]
})

# Save in various formats
atio.write(df, "users.parquet", format="parquet")
atio.write(df, "users.csv", format="csv", index=False)
atio.write(df, "users.xlsx", format="excel", sheet_name="Users")
```

#### Database Writing

```python
import atio
import pandas as pd
from sqlalchemy import create_engine

df = pd.DataFrame({
    "product_id": [101, 102, 103],
    "product_name": ["Laptop", "Mouse", "Keyboard"],
    "price": [1200, 25, 75]
})

# Save to SQL database
engine = create_engine('postgresql://user:password@localhost/dbname')
atio.write(df, format="sql", name="products", con=engine, if_exists="replace")
```

#### Advanced Features (Progress, Performance Monitoring)

```python
# Save with progress display
atio.write(large_df, "big_data.parquet", format="parquet", show_progress=True)

# Output detailed performance information
atio.write(df, "data.parquet", format="parquet", verbose=True)

# Use Polars DataFrame
import polars as pl
polars_df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
atio.write(polars_df, "data.parquet", format="parquet")
```

### `atio.write_snapshot()` - Version-Managed Table Storage

**Purpose**: Save data in table format with version management

**Key Parameters**:
- `obj`: Data to save
- `table_path`: Table save path
- `mode`: Save mode ('overwrite', 'append')
- `format`: Save format

#### Version Management Usage

```python
# Save with version management in table format
atio.write_snapshot(df, "my_table", mode="overwrite", format="parquet")

# Add to existing data (append mode)
new_data = pd.DataFrame({"name": ["David"], "age": [40], "city": ["Daejeon"]})
atio.write_snapshot(new_data, "my_table", mode="append", format="parquet")
```

### `atio.read_table()` - Table Data Reading

**Purpose**: Read data from table

**Key Parameters**:
- `table_path`: Table path
- `version`: Version number to read (None for latest)
- `output_as`: Output format ('pandas', 'polars')

#### Table Reading Usage

```python
# Read latest data
latest_data = atio.read_table("my_table", output_as="pandas")

# Read specific version
version_1_data = atio.read_table("my_table", version=1, output_as="pandas")

# Read in Polars format
polars_data = atio.read_table("my_table", output_as="polars")
```

### `atio.expire_snapshots()` - Old Data Cleanup

**Purpose**: Clean up old snapshots and orphaned files

**Key Parameters**:
- `table_path`: Table path
- `keep_for`: Retention period
- `dry_run`: Whether to actually delete (True for preview only)

#### Data Cleanup Usage

```python
from datetime import timedelta

# Clean up old data (preview)
atio.expire_snapshots("my_table", keep_for=timedelta(days=7), dry_run=True)

# Execute actual deletion
atio.expire_snapshots("my_table", keep_for=timedelta(days=7), dry_run=False)
```

## 📊 Supported Formats

| Format | Description | Required Parameters | Example |
|--------|-------------|-------------------|---------|
| `csv` | CSV file | `target_path` | `atio.write(df, "data.csv", format="csv")` |
| `parquet` | Parquet file | `target_path` | `atio.write(df, "data.parquet", format="parquet")` |
| `excel` | Excel file | `target_path` | `atio.write(df, "data.xlsx", format="excel")` |
| `json` | JSON file | `target_path` | `atio.write(df, "data.json", format="json")` |
| `sql` | SQL database | `name`, `con` | `atio.write(df, format="sql", name="table", con=engine)` |
| `database` | Database (Polars) | `table_name`, `connection_uri` | `atio.write(df, format="database", table_name="table", connection_uri="...")` |

## 🎯 Real-World Usage Scenarios

### Scenario 1: Large CSV File Writing Interruption

**Problem**: A user was saving large analysis results to a .csv file using Pandas when an unexpected power outage or kernel force termination occurred. The result file was corrupted with only 3MB saved out of 50MB, and could not be read afterward.

**Atio Solution**: `atio.write()` first writes to a temporary file, then only replaces the original after all writing is successful. Therefore, even if interrupted, the existing file is preserved and corrupted temporary files are automatically cleaned up, ensuring stability.

### Scenario 2: File Conflicts in Multiprocessing Environment

**Problem**: In a Python multiprocessing-based data collection pipeline, multiple processes were simultaneously saving to the same file, causing conflicts. As a result, log files were overwritten and lost, or some JSON files were saved in corrupted, unparseable forms.

**Atio Solution**: Using `atio.write()`'s atomic replacement method for file writing ensures that only one process can move to the final path at a time. This guarantees conflict-free, collision-free saving without race conditions.

### Scenario 3: Data Pipeline Validation Issues

**Problem**: In ETL operations, the automated system could not determine whether .parquet saving was completed, so corrupted or incomplete data was used in the next stage. This resulted in missing values in model training data, causing quality degradation.

**Atio Solution**: Using `atio.write_snapshot()` creates a `_SUCCESS` flag file only when saving is successfully completed. Subsequent stages can safely run the pipeline based on the presence or absence of `_SUCCESS`.

### Scenario 4: Lack of Data Version Management

**Problem**: As datasets for machine learning model training were updated multiple times, it became impossible to track which version of data was used to train which model. Experimental result reproducibility decreased and model performance comparison became difficult.

**Atio Solution**: Using `atio.write_snapshot()` and `atio.read_table()` allows automatic management of data versions. Snapshots are created for each version, allowing you to return to data from any specific point in time, ensuring experimental reproducibility.

### Scenario 5: System Interruption Due to Disk Space Shortage

**Problem**: During large data processing, the system was interrupted due to insufficient disk space. Incomplete files from processing remained, continuing to occupy disk space and requiring manual cleanup.

**Atio Solution**: Using `atio.expire_snapshots()` allows automatic cleanup of snapshots and orphaned files older than the set retention period. You can preview files to be deleted with `dry_run=True` option, then safely perform cleanup operations.

### Scenario 6: Network Error During Database Storage

**Problem**: While saving analysis results to a PostgreSQL database, the network connection was interrupted, stopping the save operation. Partially saved tables remained in the database, breaking data integrity.

**Atio Solution**: `atio.write()`'s database storage feature uses transactions to ensure all data is either successfully saved or not saved at all. When network errors occur, automatic rollback maintains data integrity.

### Scenario 7: Complexity in Experimental Data Management

**Problem**: A research team was conducting multiple experiments simultaneously, causing experimental data to mix and making it difficult to track which data was used for which experiment. Experimental result reliability decreased and reproduction became impossible.

**Atio Solution**: Using `atio.write_snapshot()` creates independent tables for each experiment, and `atio.read_table()` can read the exact data for specific experiments. Automated version management and metadata tracking for each experiment ensures research reproducibility and reliability.

### Scenario 8: Data Loss During Cloud Streaming

**Problem**: While processing real-time data collected from IoT sensors, system restart or network errors occurred. Data being processed was lost, breaking the continuity of important sensor data.

**Atio Solution**: Using `atio.write_snapshot()` buffers real-time data and saves it atomically at regular intervals. After system restart, data collection can resume from the last save point, ensuring data continuity.

### Scenario 9: Memory Shortage During Large Data Processing

**Problem**: While processing DataFrames larger than 10GB, the process was force-terminated due to memory shortage. All intermediate results being processed were lost, requiring restart from the beginning.

**Atio Solution**: Using `atio.write()`'s `show_progress=True` option along with chunk-based data processing controls memory usage. Each chunk is processed after the previous one is successfully saved, so even if it fails in the middle, already saved data is preserved.

### Scenario 10: Conflicts with Backup Systems

**Problem**: While trying to save a large file during automatic backup system execution, the backup software attempted to backup a file being written, causing file corruption. The backup file was also saved in an incomplete state.

**Atio Solution**: Using `atio.write()`'s atomic replacement method for file saving ensures that backup systems only see complete files when reading. Temporary files are excluded from backup targets, enabling conflict-free, safe backups. 

## 🔍 Performance Monitoring

```python
# Output detailed performance information
atio.write(df, "data.parquet", format="parquet", verbose=True)
```

Output example:
```
[INFO] Temporary directory created: /tmp/tmp12345
[INFO] Temporary file path: /tmp/tmp12345/data.parquet
[INFO] Writer to use: to_parquet (format: parquet)
[INFO] ✅ File writing completed (total time: 0.1234s)
```

## 📦 Dependencies

### Required Dependencies
- Python 3.7+
- pandas
- numpy

### Optional Dependencies
- `pyarrow` or `fastparquet`: Parquet format support
- `openpyxl` or `xlsxwriter`: Excel format support
- `sqlalchemy`: SQL database support
- `polars`: Polars DataFrame support

## 📄 License

This project is distributed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.

## 🐛 Bug Reports

Found a bug? Please report it on the [Issues](https://github.com/seojaeohcode/atio/issues) page.

---

<div align="center">

**Atio** - Safe and Fast Data Writing Library 🚀

</div> 
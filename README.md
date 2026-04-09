# partcsv

[![pypi](https://img.shields.io/pypi/v/partcsv.svg)](https://pypi.org/project/partcsv/)

Partition and write CSV files in parallel. Zero dependencies.

## Install

```bash
uv add partcsv
```

## Usage

### Write records to CSV with batching

```python
from pathlib import Path
from partcsv import CsvRecordWriter

with CsvRecordWriter(Path("output"), record_limit=10_000) as writer:
    for record in records:
        writer.add_to_batch(record)
```

### Partition dicts across files by key

```python
from partcsv import partition_dicts

partition_dicts(
    iterable=records,
    partition_key="category",
    num_partitions=8,
    parent_dir="output/",
)
```

import random

import pandas as pd

from partcsv import partition_dicts


def _gen(n, ks):
    for _ in range(n):
        yield {k: random.random() for k in ks}


def test_main(tmp_path):
    ks = list("abcdefghijklmnop")
    n = 1_000_000
    parts = 15
    spp = 1000
    dps = 8

    partition_dicts(
        _gen(n, ks),
        "b",
        parts,
        tmp_path,
        partition_type=float,
        slot_per_partition=spp,
        director_count=dps,
    )

    df = pd.concat(map(pd.read_csv, tmp_path.iterdir()))
    assert df.shape[0] == n


def xtest_errs(tmp_path):
    partition_dicts(
        [{"a": 10}, {"b": 10}], partition_key="a", num_partitions=2, parent_dir=tmp_path
    )

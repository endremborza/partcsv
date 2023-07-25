import random
from itertools import product

import pandas as pd
import pytest

from partcsv import partition_dicts


def _gen(n, ks):
    for _ in range(n):
        yield {k: random.random() for k in ks}


@pytest.mark.parametrize(
    ("parts", "spp", "dps", "bs"),
    list(product([3, 10, 40], [10, 500, 2000], [1, 10, 20], [1, 50, 1500]))[-1:],
)
def test_main(tmp_path, parts, spp, dps, bs):
    ks = list("abcdefghijklmnop")
    n = 1_000_000
    partition_dicts(
        _gen(n, ks),
        "b",
        parts,
        tmp_path,
        partition_type=float,
        slot_per_partition=spp,
        director_count=dps,
        batch_size=bs,
    )

    df = pd.concat(map(pd.read_csv, tmp_path.iterdir()))
    assert df.shape[0] == n


def test_errs(tmp_path):
    with pytest.raises(ChildProcessError):
        partition_dicts(
            [{"a": 10}, {"b": 10}],
            partition_key="a",
            num_partitions=2,
            parent_dir=tmp_path,
        )

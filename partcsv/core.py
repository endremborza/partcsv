import csv
import gzip
import multiprocessing as mp
from functools import partial
from hashlib import md5
from itertools import islice
from math import log10
from pathlib import Path
from typing import Callable, Iterable

POISON_PILL = None


def director(
    q_dic: dict[str, mp.Queue],
    main_queue: mp.Queue,
    get_partition: Callable[[dict], str],
):
    while True:
        elem = main_queue.get()
        if elem is POISON_PILL:
            return
        for record in elem:
            partition = get_partition(record)
            q_dic[partition].put(record)


def partition_writer(partition_name, parent_dir, queue: mp.Queue, mode: str = "wt"):
    with gzip.open(
        Path(parent_dir, f"{partition_name}.csv.gz"), mode, encoding="utf-8"
    ) as gzp:
        first_row = queue.get()
        if first_row is POISON_PILL:
            return
        writer = csv.DictWriter(gzp, fieldnames=first_row.keys())
        writer.writeheader()
        writer.writerow(first_row)
        while True:
            row = queue.get()
            if row is POISON_PILL:
                return
            writer.writerow(row)


def main_queue_filler(it: Iterable, main_queue: mp.Queue, batch_size: int):
    while True:
        o = list(islice(it, batch_size))
        if not o:
            return
        main_queue.put(o)


def partition_dicts(
    iterable: Iterable[dict],
    partition_key: str,
    num_partitions: int,
    parent_dir: str | Path = "",
    partition_type: type = str,
    slot_per_partition: int = 1000,
    director_count=2,
    batch_size=100,
    append: bool = False,
    writer_function: Callable[[str, str, mp.Queue, str], None] = partition_writer,
    main_queue_filler: Callable[[Iterable, mp.Queue, int], None] = main_queue_filler,
):
    _it = iter(iterable)
    _w = int(log10(num_partitions)) + 1
    _namer = partial(_pstring, w=_w)
    main_queue = mp.Queue(maxsize=int(num_partitions * slot_per_partition / batch_size))

    q_dic = {_namer(i): mp.Queue() for i in range(num_partitions)}

    writer_proces = [
        mp.Process(
            target=writer_function,
            args=(name, parent_dir, q, "at" if append else "wt"),
        )
        for name, q in q_dic.items()
    ]

    part_getter = partial(
        get_partition,
        key=partition_key,
        preproc=_PGET_DICT.get(partition_type, _pget_other),
        n=num_partitions,
        namer=_namer,
    )

    dir_proces = [
        mp.Process(target=director, args=(q_dic, main_queue, part_getter))
        for _ in range(director_count)
    ]
    all_proces = writer_proces + dir_proces
    try:
        for proc in all_proces:
            proc.start()
        main_queue_filler(_it, main_queue, batch_size)
        for _ in range(director_count):
            main_queue.put(POISON_PILL)
        for dp in dir_proces:
            dp.join()
        for q in q_dic.values():
            q.put(POISON_PILL)
        for wp in writer_proces:
            wp.join()
    except Exception as e:
        print("killing everything after", e)
        for p in all_proces:
            p.kill()


def get_partition(rec: dict, key: str, preproc: Callable, n: int, namer: Callable):
    return namer(_pget(preproc(rec[key]), n))


def _pget(elem: bytes, ngroups) -> int:
    return int(md5(elem).hexdigest(), base=16) % ngroups


def _pget_float(elem: float):
    return elem.hex().encode()


def _pget_str(elem: str):
    return elem.encode()


def _idn(elem):
    return elem


def _pget_other(elem):
    return str(elem).encode()


def _pstring(num: int, w: int):
    return f"{num:0{w}d}"


_PGET_DICT = {str: _pget_str, float: _pget_float, bytes: _idn}

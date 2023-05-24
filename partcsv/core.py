import csv
import gzip
import multiprocessing as mp
from functools import partial
from hashlib import md5
from math import log10
from pathlib import Path
from typing import Callable, Iterable

POISON_PILL = None


def partition_dicts(
    iterable: Iterable[dict],
    partition_key: str,
    num_partitions: int,
    parent_dir: str | Path = "",
    partition_type: type = str,
    slot_per_partition: int = 100,
    director_count=2,
    append: bool = False,
):
    Runner(
        iterable,
        partition_key=partition_key,
        num_partitions=num_partitions,
        parent_dir=parent_dir,
        partition_type=partition_type,
        slot_per_partition=slot_per_partition,
        director_count=director_count,
        append=append,
    ).run()


class Runner:
    def __init__(
        self,
        iterable: Iterable[dict],
        partition_key: str,
        num_partitions: int,
        parent_dir: str | Path = "",
        partition_type: type = str,
        slot_per_partition: int = 100,
        director_count=2,
        append: bool = False,
    ) -> None:
        self._it = iter(iterable)
        _w = int(log10(num_partitions)) + 1
        _namer = partial(_pstring, w=_w)
        self.main_queue = mp.Queue(maxsize=int(num_partitions * slot_per_partition))
        self.writers = [
            PartitionWriter(_namer(i), parent_dir, append=append)
            for i in range(num_partitions)
        ]

        q_dic = {w.name: w.q for w in self.writers}
        part_getter = partial(
            get_partition,
            key=partition_key,
            preproc=_PGET_DICT.get(partition_type, _pget_other),
            n=num_partitions,
            namer=_namer,
        )

        self.dir_proces = [
            mp.Process(target=director, args=(q_dic, self.main_queue, part_getter))
            for _ in range(director_count)
        ]

    def run(self):
        try:
            self._run()
        except Exception as e:
            print("killing everything after", e)
            for p in self.dir_proces:
                p.kill()
            for w in self.writers:
                w.proc.kill()

    def _run(self):
        for dproc in self.dir_proces:
            dproc.start()
        for elem in self._it:
            self.main_queue.put(elem)
        for _ in self.dir_proces:
            self.main_queue.put(POISON_PILL)
        for dp in self.dir_proces:
            dp.join()
        for w in self.writers:
            w.proc.join()


class PartitionWriter:
    def __init__(self, partition_name, parent_dir, append: bool = False) -> None:
        self.q = mp.Queue()
        self.name = partition_name
        self.proc = mp.Process(
            target=partition_writer,
            args=(partition_name, parent_dir, self.q, "at" if append else "wt"),
        )
        self.proc.start()
        self.add = self.q.put


def director(
    q_dic: dict[str, mp.Queue],
    main_queue: mp.Queue,
    get_partition: Callable[[dict], str],
):
    while True:
        elem = main_queue.get()
        if elem is POISON_PILL:
            for q in q_dic.values():
                q.put(POISON_PILL)
            return
        partition = get_partition(elem)
        q_dic[partition].put(elem)


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

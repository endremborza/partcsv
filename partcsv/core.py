import csv
import gzip
import io
import multiprocessing as mp
from dataclasses import dataclass, field
from functools import partial, reduce
from hashlib import md5
from itertools import islice
from math import log10
from pathlib import Path
from typing import Callable, Iterable

POISON_PILL = None


@dataclass
class CsvRecordWriter:
    rpath: Path
    record_limit: int = 1_000_000
    batch: list = field(default_factory=list)
    record_count: int = field(default=0, init=False)
    append: bool = False
    headers_written: bool = field(default=False, init=False)
    csv_writer: csv.DictWriter = field(init=False, default=None)
    f: io.TextIOWrapper = field(init=False, default=None)
    compression: bool = True

    def __post_init__(self):
        self.record_count = len(self.batch)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def add_to_batch(self, element):
        self.batch.append(self._parse_elem(element))
        self.record_count += self._rec_count_from_elem(element)
        if self.record_count >= self.record_limit:
            self._write()

    def add_multiple(self, elements):
        for e in elements:
            self.add_to_batch(e)

    def close(self):
        if self.batch:
            self._write()
        if self.f is not None:
            self.f.close()
        self.record_count = 0
        self.batch = []

    @property
    def csv_path(self):
        suff = "" if not self.compression else ".gz"
        return self.rpath.with_suffix(f".csv{suff}")

    def _write(self):
        if self.csv_writer is None:
            keys = list(reduce(lambda keys, r: keys | r.keys(), self.batch, {}.keys()))
            if self.compression:
                self.f = gzip.open(
                    self.csv_path,
                    mode=("at" if self.append else "wt"),
                    encoding="utf-8",
                )
            else:
                self.f = self.csv_path.open("a" if self.append else "w")
            self.csv_writer = csv.DictWriter(self.f, keys, extrasaction="ignore")
            self.csv_writer.writeheader()
        self.csv_writer.writerows(self.batch)
        self.batch = []
        self.record_count = 0

    def _parse_elem(self, elem):
        return elem

    def _rec_count_from_elem(self, elem):
        return 1


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


def partition_writer(
    partition_name,
    parent_dir,
    queue: mp.Queue,
    append: bool = False,
    batch_size: int = 10,
):
    with CsvRecordWriter(
        Path(parent_dir, str(partition_name)), batch_size, append=append
    ) as dwriter:
        while True:
            row = queue.get()
            if row is POISON_PILL:
                return
            dwriter.add_to_batch(row)


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
    writer_function: Callable[[str, str, mp.Queue, bool, int], None] = partition_writer,
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
            args=(name, parent_dir, q, append, slot_per_partition),
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
            if dp.exitcode != 0:
                raise ChildProcessError("")
        for q in q_dic.values():
            q.put(POISON_PILL)
        for wp in writer_proces:
            wp.join()
    except Exception as e:
        print("killing everything after", e)
        for p in all_proces:
            p.kill()
        raise e


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

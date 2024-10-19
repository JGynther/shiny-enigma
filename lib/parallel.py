from concurrent.futures import ProcessPoolExecutor
from functools import reduce
from math import ceil

from lib.naive import process_records


def chunk(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i : i + n]


# Works but can ends up with undersized batches due to chunking with ordering in mind
def process_records_parallel(records: list[str], number_of_workers: int = 4):
    if not records:
        return []

    chunk_size = ceil(len(records) / number_of_workers)
    chunks = chunk(records, chunk_size)

    with ProcessPoolExecutor(max_workers=number_of_workers) as executor:
        iterator = executor.map(process_records, chunks)

    return reduce(lambda x, y: x + y, iterator)  # ...or sum()

from functools import cache

from lib.const import MAX_BATCH_SIZE, MAX_RECORD_SIZE, MAX_RECORDS_PER_BATCH


@cache
def string_byte_size(string: str, encoding="utf-8") -> int:
    return len(string.encode(encoding))


def process_records(records: list[str]) -> list[list[str]]:
    batches = []
    current_batch = []
    current_batch_size = 0

    for record in records:
        match string_byte_size(record):
            # Drop records that are too large
            case size if size > MAX_RECORD_SIZE:
                continue

            # Flush batch on max records per batch
            case size if len(current_batch) == MAX_RECORDS_PER_BATCH:
                batches.append(current_batch)
                current_batch = [record]
                current_batch_size = size

            # Flush batch on max batch size
            case size if current_batch_size + size > MAX_BATCH_SIZE:
                batches.append(current_batch)
                current_batch = [record]
                current_batch_size = size

            case size:
                current_batch.append(record)
                current_batch_size += size

    if current_batch:
        batches.append(current_batch)

    return batches

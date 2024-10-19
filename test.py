from lib.const import MAX_BATCH_SIZE, MAX_RECORD_SIZE, ONE_MEGABYTE
from lib.naive import process_records

ONE_MEGABYTE_RECORD = "a" * ONE_MEGABYTE
OVERSIZED_RECORD = "a" * (ONE_MEGABYTE + 1)


def test_process_records():
    # Case 1: Basic case with small records (all fit into one batch)
    records = ["record1", "record2", "record3"]
    expected_batches = [records]
    assert process_records(records) == expected_batches, "Test Case 1 failed"
    print("Test Case 1 passed:  Basic case with small records.")

    # Case 2: Discard record larger than 1 MB
    records = ["small_record", OVERSIZED_RECORD, "small_record_2"]
    expected_batches = [["small_record", "small_record_2"]]
    assert process_records(records) == expected_batches, "Test Case 2 failed"
    print("Test Case 2 passed:  Discard oversized record.")

    # Case 3: Test batch size limit of 5 MB
    records = [ONE_MEGABYTE_RECORD] * 6  # 6 records, each 1 MB
    expected_batches = [records[:5], records[5:]]
    assert process_records(records) == expected_batches, "Test Case 3 failed"
    print("Test Case 3 passed:  Batch size limit of 5 MB.")

    # Case 4: Test maximum number of records per batch
    records = ["small_record"] * 550  # 550 small records
    expected_batches = [records[:500], records[500:]]
    assert process_records(records) == expected_batches, "Test Case 4 failed"
    print("Test Case 4 passed:  Maximum number of records per batch.")

    # Case 5: Mixed sizes, including oversized records
    records = [
        "small_record",
        "small_record",
        ONE_MEGABYTE_RECORD,
        "small_record",
        OVERSIZED_RECORD,
    ]
    expected_batches = [records[:-1]]
    assert process_records(records) == expected_batches, "Test Case 5 failed"
    print("Test Case 5 passed:  Mixed sizes and oversized record.")

    # Case 6: Batch split by size and number of records
    records = ["a" * (ONE_MEGABYTE // 2)] * 11  # 11 records, each 0.5 MB
    expected_batches = [records[:10], records[10:]]
    assert process_records(records) == expected_batches, "Test Case 6 failed"
    print("Test Case 6 passed:  Batch split by size and number of records.")

    # Case 7: Empty input
    records = []
    expected_batches = []
    assert process_records(records) == expected_batches, "Test Case 7 failed"
    print("Test Case 7 passed:  Empty input.")

    # Case 8: Single small record
    records = ["single_record"]
    expected_batches = [records]
    assert process_records(records) == expected_batches, "Test Case 8 failed"
    print("Test Case 8 passed:  Single small record.")

    # Case 9: All records oversized
    records = [OVERSIZED_RECORD] * 10  # All records are over 1 MB
    expected_batches = []  # All records should be discarded
    assert process_records(records) == expected_batches, "Test Case 9 failed"
    print("Test Case 9 passed:  All records oversized.")

    # Case 10: Record exactly 5 MB should be discarded
    records = ["a" * MAX_BATCH_SIZE]  # 1 record of 5 MB
    expected_batches = []  # The record should be discarded
    assert process_records(records) == expected_batches, "Test Case 10 failed"
    print("Test Case 10 passed: Record exceeding 1 MB limit discarded.")

    # Case 11: Non-ASCII characters handling
    records = ["😊" * (MAX_RECORD_SIZE // 2)]  # Each emoji is 4 bytes in UTF-8
    expected_batches = []
    assert process_records(records) == expected_batches, "Test Case 11 failed"
    print("Test Case 11 passed: Non-ASCII character handling.")

    # Case 12: Test a large number of records
    records = ["small_record"] * 550_037
    expected_batches = [records[:500]] * 1100 + [records[:37]]
    assert process_records(records) == expected_batches, "Test Case 12 failed"
    print("Test Case 12 passed:  Maximum number of records per batch.")


if __name__ == "__main__":
    test_process_records()

import csv
from utils import col_index, reset_reader, file_len, Block, Printer, estimate_block_size


def main(file1, file2, col, join_type, max_mem_mb=256):
    """
    Join two files based on a column index.

    Parameters
    ----------
    file1: str
        First file to join.
    file2: str
        Second file to join.
    col: str
        Column index to join on.
    join_type: str
        Type of join to perform (inner, left, right, outer).
    max_mem_mb: int
        Max memory to use in MB.
    """
    # right join is the same as left join with reversed file order
    if join_type == 'right':
        file1, file2 = file2, file1

    f1, f2 = open(file1, "r"), open(file2, "r")
    len1, len2 = file_len(file1)-1, file_len(file2)-1

    if join_type == 'inner' and len1 > len2:
        f1, f2 = f2, f1
        len1, len2 = len2, len1

    reader1, reader2 = csv.reader(f1), csv.reader(f2)
    header1, header2 = next(reader1), next(reader2)

    printer = Printer(header1, header2, col, reverse_files=(join_type == 'right'))
    printer.print_header()

    col_index_1, col_index_2 = col_index(header1, col), col_index(header2, col)

    max_block_size = estimate_block_size(max_mem_mb, reader1, len1)
    reset_reader(f1, reader1)

    block_sizes = [max_block_size]*(len1//max_block_size)
    if len1 % max_block_size > 0:
        block_sizes += [len1 % max_block_size]

    for size in block_sizes:
        block = Block(col_index_1)

        for _ in range(size):
            row = next(reader1)
            block.put(row)

        for row2 in reader2:
            key = row2[col_index_2]
            for row1 in block.get(key):
                printer.print(row1, row2)

        # for left, right and outer join, print the remaining rows from the first file
        if join_type in ['left', 'right', 'outer']:
            for row in block.get_unused():
                printer.print(row, None)

        reset_reader(f2, reader2)

    # outer join, print the remaining rows from the second file
    if join_type == 'outer':
        for size in block_sizes:
            reset_reader(f1, reader1)
            block = Block(col_index_2)

            for _ in range(size):
                row = next(reader2)
                block.put(row)

            for row1 in reader1:
                key = row1[col_index_1]
                block.mark_as_used(key)

            for row in block.get_unused():
                printer.print(None, row)

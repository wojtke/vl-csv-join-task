import csv
import math
from utils import col_index, reset_reader, file_len, Block, Printer

def main(file1, file2, col, join_type, block_size=10000):
    reverse_files=False
    if join_type == 'right':
        join_type = 'left'
        file1, file2 = file2, file1
        reverse_files=True

    f1, f2 = open(file1, "r"), open(file2, "r")
    len1, len2 = file_len(file1)-1, file_len(file2)-1

    if join_type == 'inner' and len1 > len2:
        f1, f2 = f2, f1
        len1, len2 = len2, len1

    reader1, reader2 = csv.reader(f1), csv.reader(f2)
    header1, header2 = next(reader1), next(reader2)

    printer = Printer(header1, header2, col, reverse_files=True)
    printer.print_header()

    col_index_1, col_index_2 = col_index(header1, col), col_index(header2, col)

    block_sizes = [block_size]*(len1//block_size)
    if len1%block_size>0:
        block_sizes += [len1%block_size]

    for size in block_sizes:
        block = Block(col_index_1)

        for _ in range(size):
            row = next(reader1)
            block.put(row)

        for row2 in reader2:
            key = row2[col_index_2]
            for row1 in block.get(key):
                printer.print(row1, row2)

        if join_type in ['left', 'outer']:
            for row in block.get_unused():
                printer.print(row, None)

        reset_reader(f2, reader2)

    if join_type=='outer':
        for size in block_sizes:
            reset_reader(f1, reader1)
            block = Block(col_index_2)

            for _ in range(size):
                row = next(reader2)
                block.put(row)

            for row1 in reader1:
                key = row1[col_index_1]
                block.check_as_used(key)

            for row in block.get_unused():
                printer.print(None, row)

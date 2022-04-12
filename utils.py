import os, psutil
import time


def col_index(col_names, col):
    """Return the index of a column name in a list of column names."""
    for i, c in enumerate(col_names):
        if c == col:
            return i
    raise ValueError(f"Column name not found: {col}")


def reset_reader(file, reader):
    """Reset a file to the beginning."""
    file.seek(0)
    next(reader)


def file_len(filename):
    """Return the number of lines in a file."""
    def _make_gen(reader):
        b = reader(2 ** 16)
        while b:
            yield b
            b = reader(2 ** 16)

    with open(filename, "rb") as f:
        count = sum(buf.count(b"\n") for buf in _make_gen(f.raw.read))

    return count


class Block:
    """A block of lines."""
    def __init__(self, join_col_index):
        """Initialize a block."""
        self.join_col_index = join_col_index

        self.rows = {}
        self.row_used = {}

    def put(self, row):
        """Put a row in the block."""
        key = row[self.join_col_index]
        if key in self.rows:
            self.rows[key].append(row)
        else:
            self.rows[key] = [row]
            self.row_used[key] = False

    def get(self, key):
        """Return the rows with the given key, and mark them as used."""
        if key in self.rows:
            self.mark_as_used(key)
            return self.rows[key]
        else:
            return []

    def mark_as_used(self, key):
        """Mark a row as used."""
        self.row_used[key] = True

    def get_unused(self):
        """Return the rows that have not been used."""
        for key in self.rows:
            if not self.row_used[key]:
                for row in self.rows[key]:
                    yield row


class Printer:
    """A printer for printing rows."""
    def __init__(self, header1, header2, col, reverse_files=False):
        """Initialize a printer."""
        self.header1 = header1
        self.header2 = header2

        self.reverse_files = reverse_files
        if reverse_files:
            header1, header2 = header2, header1

        self.col_index_1 = col_index(header1, col)
        self.col_index_2 = col_index(header2, col)

        # construct new header
        self.header = [
            col, 
            *header1[:self.col_index_1], 
            *header1[self.col_index_1+1:],
            *header2[:self.col_index_2],
            *header2[self.col_index_2+1:]
        ]

        # if column names repeat, add a suffix to each column
        n_cols = len(self.header)
        for i in range(n_cols):
            for j in range(i+1, n_cols):
                if self.header[i] == self.header[j]:
                    self.header[i] += '_x'
                    self.header[j] += '_y'

    def print_header(self):
        """Print the header."""
        print(*self.header, sep=',')

    def print(self, row1, row2):
        """Print a row."""
        if not row1 and not row2:
            return

        if self.reverse_files:
            row1, row2 = row2, row1

        key = None
        if row1:
            key = row1[self.col_index_1]
            row1 = row1[:self.col_index_1] + row1[self.col_index_1+1:]
        else: 
            row1 = ['NULL']*(len(self.header1)-1)

        if row2:
            key = row2[self.col_index_2]
            row2 = row2[:self.col_index_1] + row2[self.col_index_1+1:]
        else: 
            row2 = ['NULL']*(len(self.header2)-1)

        print(key, *row1, *row2, sep=',')


def get_memory_used():
    """Return the memory used by the process (MB)."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 ** 2)


def estimate_block_size(max_mem_mb, reader, n_lines):
    """Estimate block size."""
    for _ in range(100):
        base_mem_mb = get_memory_used()
        if base_mem_mb > 0.7*max_mem_mb:
            time.sleep(0.2)
        else:
            break
    base_mem_mb = get_memory_used()
    if base_mem_mb > 0.7 * max_mem_mb:
        raise MemoryError(f"Not enough memory to proceed (base memory used: {base_mem_mb} MB, "
                  f"max memory: {max_mem_mb} MB)")

    dic = {}
    i = 0
    while n_lines//2 - i > 0 and get_memory_used()-base_mem_mb < 0.1*max_mem_mb:
        dic[i] = next(reader)
        i += 1

    if i == n_lines//2:
        return 10**6
    else:
        estimate = (max_mem_mb*0.8 - base_mem_mb)*i / (get_memory_used() - base_mem_mb)

    if len(dic) == 0:
        pass

    return max(10, int(estimate))

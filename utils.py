def col_index(col_names, col):
    for i, c in enumerate(col_names):
        if c == col:
            return i

def reset_reader(file, reader):
    file.seek(0)
    next(reader)

def file_len(filename):
    def _make_gen(reader):
        b = reader(2 ** 16)
        while b:
            yield b
            b = reader(2 ** 16)

    with open(filename, "rb") as f:
        count = sum(buf.count(b"\n") for buf in _make_gen(f.raw.read))
    return count


class Block:
    def __init__(self, join_col_index):
        self.join_col_index = join_col_index

        self.rows = {}
        self.row_used = {}

    def put(self, row):
        key = row[self.join_col_index]
        if key in self.rows:
            self.rows[key].append(row)
        else:
            self.rows[key] = [row]
            self.row_used[key] = False

    def get(self, key):
        if key in self.rows:
            self.check_as_used(key)
            return self.rows[key]
        else:
            return []

    def check_as_used(self, key):
        self.row_used[key] = True

    def get_unused(self):
        for key in self.rows:
            if not self.row_used[key]:
                for row in self.rows[key]:
                    yield row


class Printer:
    def __init__(self, header1, header2, col, reverse_files=False):
        self.reverse_files = reverse_files
        if reverse_files:
            header1, header2 = header2, header1

        self.header1 = header1
        self.header2 = header2

        self.col_index_1 = col_index(header1, col)
        self.col_index_2 = col_index(header2, col)

        self.header = [
            col, 
            *header1[:self.col_index_1], 
            *header1[self.col_index_1+1:],
            *header2[:self.col_index_2],
            *header2[self.col_index_2+1:]
        ]

        n_cols = len(self.header)
        for i in range(n_cols):
            for j in range(i+1, n_cols):
                if self.header[i]==self.header[j]:
                    self.header[i] += '_x'
                    self.header[j] += '_y'



    def print_header(self):
        print(*self.header, sep=',')

    def print(self, row1, row2):
        if self.reverse_files:
            row1, row2 = row2, row1

        if row1:
            key = row1[self.col_index_1]
            del row1[self.col_index_1]
        else: 
            row1 = ['NULL']*(len(self.header1)-1)

        if row2:
            key = row2[self.col_index_2]
            del row2[self.col_index_2]
        else: 
            row2 = ['NULL']*(len(self.header2)-1)

        print(key,*row1,*row2, sep=',')
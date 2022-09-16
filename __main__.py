import join
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Join two files')
    parser.add_argument("file1", type=str)
    parser.add_argument("file2", type=str)
    parser.add_argument("col", type=str)
    parser.add_argument("join_type", type=str, choices=['inner', 'left', 'right', 'outer'], default='inner')
    parser.add_argument("--max_mem", type=int, default=1024)
    args = parser.parse_args()

    join.main(args.file1, args.file2, args.col, args.join_type, args.max_mem)

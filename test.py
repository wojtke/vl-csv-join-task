import pandas as pd
import numpy as np

import contextlib
import join


def generate_test_files(name, n=50, m=2, d=0.5, unique=True):
    """Generate two files with m columns. Out of n rows, for each file d*n rows
    are randomly selected and saved in random order. Column supposed to be used as join
    key is named 'index' and its values are unique if unique=True."""

    A = np.random.uniform(size=(n, 2 * m))
    df = pd.DataFrame(A)
    df.columns = ['col' + str(i) for i in df.columns]

    df1 = df.loc[:, df.columns[:m]]
    df1['which_file'] = "first"
    df1 = df1.sample(int(d * n))
    df1.reset_index(inplace=True)

    df2 = df.loc[:, df.columns[m:]]
    df2['which_file'] = "second"
    df2 = df2.sample(int(d * n))
    df2.reset_index(inplace=True)

    if not unique:
        df1 = pd.concat([df1, df1.loc[:m//4]], ignore_index=True)
        df2 = pd.concat([df2, df2.loc[:m // 4]], ignore_index=True)

    df1.to_csv(f'test_files/{name}-part1.csv', index=False)
    df2.to_csv(f'test_files/{name}-part2.csv', index=False)


def join_and_compare(name, join_type):
    """Join two files using my script and pandas join function and compare the results"""

    path = f'test_files/{name}-joined.csv'
    file1 = f'test_files/{name}-part1.csv'
    file2 = f'test_files/{name}-part2.csv'

    # join files using my script, save result to path
    with open(path, 'w') as f:
        with contextlib.redirect_stdout(f):
            join.main(file1, file2, 'index', join_type)

    # pandas join
    df1, df2 = pd.read_csv(file1), pd.read_csv(file2)
    df_pd_join = pd.merge(df1, df2, on='index', how=join_type).sort_values(by=['index']).reset_index(drop=True)

    # read file with data joined using my script
    df_my_join = pd.read_csv(path).sort_values(by=['index']).reset_index(drop=True)

    assert df_pd_join.shape == df_my_join.shape
    assert df_pd_join.equals(df_my_join)

    del df1, df2, df_pd_join, df_my_join

    print(join_type, "is ok")


if __name__ == "__main__":
    name = 'basic'
    print("Basic test")
    print("Generating test files...")
    generate_test_files(name, n=100, m=3, d=0.8, unique=True)
    print("Generating test files... Done")
    print(f"Testing join ...")
    join_and_compare(name, 'inner')
    join_and_compare(name, 'left')
    join_and_compare(name, 'right')
    join_and_compare(name, 'outer')
    print("Testing join... Done\n")

    name = 'non_unique'
    print("Non-unique test")
    print("Generating test files...")
    generate_test_files(name, n=1000, m=4, d=0.5, unique=False)
    print("Generating test files... Done")
    print(f"Testing join ...")
    join_and_compare(name, 'inner')
    join_and_compare(name, 'left')
    join_and_compare(name, 'right')
    join_and_compare(name, 'outer')
    print("Testing join... Done\n")

    name = 'big'
    print("Big files test")
    print("Generating test files...")
    generate_test_files(name, n=100000, m=10, d=0.8, unique=False)
    print("Generating test files... Done")
    print(f"Testing join ...")
    join_and_compare(name, 'inner')
    join_and_compare(name, 'left')
    join_and_compare(name, 'right')
    join_and_compare(name, 'outer')
    print("Testing join... Done\n")

    name = 'very_big'
    print("Big files test")
    print("Generating test files...")
    generate_test_files(name, n=10**6, m=20, d=0.8, unique=False)
    print("Generating test files... Done")
    print(f"Testing join ...")
    join_and_compare(name, 'inner')
    join_and_compare(name, 'left')
    join_and_compare(name, 'right')
    join_and_compare(name, 'outer')
    print("Testing join... Done\n")

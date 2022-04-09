import pandas as pd
import numpy as np

import contextlib
import join


def generate_test_files(name, n=50, m=4, fullness=0.5):
    A = np.random.uniform(size=(n, m))
    df = pd.DataFrame(A)
    df.columns = ['col' + str(i) for i in df.columns]

    df1 = df.loc[:,df.columns[:m//2]]
    df1['which_file'] = "first"
    df1 = df1.sample(int(fullness*n))
    df1.reset_index(inplace=True)

    df2 = df.loc[:,df.columns[m//2:]]
    df2['which_file'] = "second"
    df2 = df2.sample(int(fullness*n))
    df2.reset_index(inplace=True)

    df1.to_csv(f'test_files/{name}-part1.csv', index=False)
    df2.to_csv(f'test_files/{name}-part2.csv', index=False)

def join_and_compare(name, join_type):
    path = f'test_files/{name}-joined.csv'
    file1 = f'test_files/{name}-part1.csv'
    file2 = f'test_files/{name}-part2.csv'

    with open(path, 'w') as f:
        with contextlib.redirect_stdout(f):
            join.main(file1, file2, 'index', join_type)

    df1, df2 = pd.read_csv(file1), pd.read_csv(file2)
    df_pd_join = pd.merge(df1, df2, on='index', how=join_type).sort_values(by=['index']).reset_index(drop=True)

    df_my_join = pd.read_csv(path).sort_values(by=['index']).reset_index(drop=True)

    assert df_pd_join.shape == df_my_join.shape
    print(df_pd_join.columns, df_my_join.columns)
    assert len(df_pd_join.compare(df_my_join)) == 0

    print(join_type, "is ok")



if __name__ == "__main__":
    name = 'basic'
    generate_test_files(name)
    join_and_compare(name, 'inner')
    join_and_compare(name, 'left')
    join_and_compare(name, 'right')
    join_and_compare(name, 'outer')


 



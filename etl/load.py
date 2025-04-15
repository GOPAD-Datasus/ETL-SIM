import pandas as pd


def load ():
    aux = pd.DataFrame()

    for i in range (12, 23): # 12 ~ 22
        temp = pd.read_parquet(f'../temp/DO20{i}.parquet.gzip')

        aux = pd.concat([aux, temp])

    aux['index_sim'] = range(0, len(aux))
    aux.set_index('index_sim', inplace=True)

    aux.to_parquet('../temp/DO.parquet.gzip', compression='gzip')


if __name__ == '__main__':
    load()
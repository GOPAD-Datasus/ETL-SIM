import pandas as pd

def _drop(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for item in columns:
        if item in df.columns:
            df.drop(item, axis=1, inplace=True)

    return df

def _pipeline (data: pd.DataFrame, i: int) -> pd.DataFrame:
    # X adjustments

    data = _drop(data,
                 ['CONTADOR', 'ORIGEM', 'TIPOBITO'])
    data = _drop(data,
                 ['IDADEMAE', 'ESCMAE2010', 'SERIESCMAE',
                 'OCUPMAE', 'QTDFILVIVO', 'QTDFILMORT',
                 'SEMAGESTAC', 'GRAVIDEZ', 'PARTO',
                 'OBITOPARTO', 'PESO'])

    # Y adjustments

    data = data.loc[(data['TPMORTEOCO'] == 1) |
                    (data['TPMORTEOCO'] == 2) |
                    (data['TPMORTEOCO'] == 3) |
                    (data['TPMORTEOCO'] == 4) |
                    (data['TPMORTEOCO'] == 5)]

    data = data.loc[(data['IDADE'] >= 410) &
                    (data['IDADE'] <= 449)]

    # Dtyping

    if i == 22:
        data.loc[data['DTNASC'] == '27085193', 'DTNASC'] = '27081935'

    data.loc[:, 'DTNASC'] = pd.to_datetime(data['DTNASC'], dayfirst=True, format='%d%m%Y')
    data.loc[:, 'DTOBITO'] = pd.to_datetime(data['DTOBITO'], dayfirst=True, format='%d%m%Y')

    data['IDADE'] = data['IDADE'].apply(lambda x: x - 400)

    data = data.convert_dtypes()

    return data

def transform () -> None:
    for i in range (12, 24):
        print(f'read {i}')

        dtype = {
            "DTOBITO":   str,
            "HORAOBITO": str,
            "DTNASC":    str,
            "CIRCOBITO": str,
        }

        try:
            df = pd.read_csv(f'../temp/DO20{i}.csv', sep=';', dtype=dtype)
        except UnicodeDecodeError:
            df = pd.read_csv(f'../temp/DO20{i}.csv', sep=';', encoding='latin-1', dtype=dtype)

        _pipeline(df, i).to_parquet(f'../temp/DO20{i}.parquet.gzip', compression='gzip')


if __name__ == '__main__':
    transform()
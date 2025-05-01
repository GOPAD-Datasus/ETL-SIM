import pandas as pd

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2013 (YearHandler):
    def pipeline (self) -> pd.DataFrame:
        dtype = {
            'CAUSAMAT': str
        }
        encoding = 'latin1'
        sep = ';'

        self.df = pd.read_csv(self.url,
                              encoding=encoding,
                              sep=sep,
                              dtype=dtype)

        self.remove_cols(['CONTADOR', 'NUDIASOBIN'])

        return self.df
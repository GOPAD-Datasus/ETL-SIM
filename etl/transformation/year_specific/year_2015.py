import pandas as pd
import numpy as np

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2015 (YearHandler):
    def parse_tp_nivel_inv (self):
        target = 'TPNIVELINV'

        table = str.maketrans('MRE', '123')
        self.df[target] = (self.df[target]
                           .str.translate(table)
                           .astype(np.float32))

    def pipeline(self) -> pd.DataFrame:
        dtype = {
            'ESTABDESCR': str,
            'CAUSAMAT': str
        }
        encoding = 'latin1'
        sep = ';'

        self.df = pd.read_csv(self.url,
                              encoding=encoding,
                              sep=sep,
                              dtype=dtype)

        self.remove_cols(['ESTABDESCR', 'CONTADOR',
                          'CODIFICADO', 'STCODIFICA',
                          'VERSAOSIST', 'VERSAOSIST',
                          'NUMEROLOTE'])
        self.parse_tp_nivel_inv()

        return self.df
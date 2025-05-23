import pandas as pd
import numpy as np

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2022 (YearHandler):
    def parse_tp_nivel_inv(self):
        target = 'TPNIVELINV'

        table = str.maketrans('MRE', '123')
        self.df[target] = (self.df[target]
                           .str.translate(table)
                           .astype(np.float32))

    def pipeline(self) -> pd.DataFrame:
        dtype = {
            'CAUSAMAT': str
        }
        sep = ';'

        self.df = pd.read_csv(self.url,
                              sep=sep,
                              dtype=dtype)

        self.parse_tp_nivel_inv()
        self.remove_cols(['ESTABDESCR','CONTADOR',
                          'CODIFICADO', 'STCODIFICA',
                          'VERSAOSIST', 'VERSAOSIST',
                          'NUMEROLOTE'])

        return self.df
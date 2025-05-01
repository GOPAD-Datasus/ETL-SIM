import pandas as pd
import numpy as np

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2014 (YearHandler):
    def parse_hora_obito (self):
        target = 'HORAOBITO'

        self.df.loc[(self.df[target] == 'IGNOR') |
                    (self.df[target] == 'ignor'),
                    target] = np.nan

        table = str.maketrans('', '', ',hH')
        self.df[target] = (self.df[target]
                           .str.translate(table)
                           .astype(np.float32))

    def parse_tp_nivel_inv (self):
        target = 'TPNIVELINV'

        table = str.maketrans('MRE', '123')
        self.df[target] = (self.df[target]
                           .str.translate(table)
                           .astype(np.float32))

    def pipeline(self) -> pd.DataFrame:
        dtype = {
            'HORAOBITO': str,
            'ESTABDESCR': str,
            'CB_PRE': str,
            'CAUSAMAT': str
        }
        encoding = 'latin1'
        sep = ';'

        self.df = pd.read_csv(self.url,
                              encoding=encoding,
                              sep=sep,
                              dtype=dtype)

        self.parse_hora_obito()
        self.parse_tp_nivel_inv()
        self.remove_cols('ESTABDESCR')

        return self.df
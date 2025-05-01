import pandas as pd
import numpy as np

from etl.transformation.year_specific.yearHandler import YearHandler


class Handler2012 (YearHandler):
    def parse_hora_nasc (self):
        target = 'HORANASC'

        self.df.loc[self.df[target] == '0800h', target] = '0800'
        self.df[target] = self.df[target].astype(np.float32)

    def parse_circobito (self):
        target = 'CIRCOBITO'

        # C is not the initial letter of the types
        self.df.loc[self.df[target] == 'C', target] = np.nan
        self.df[target] = self.df[target].astype(np.float32)

    def pipeline(self) -> pd.DataFrame:
        dtype = {
            'HORAOBITO': str,
            'CIRCOBITO': str,
            'CAUSAMAT': str
        }
        sep = ';'

        self.df = pd.read_csv(self.url,
                              dtype=dtype,
                              sep=sep)

        self.remove_cols(['EXPDIFDATA', 'CONTADOR', 'NUDIASOBIN'])

        return self.df



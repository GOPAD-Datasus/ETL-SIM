import pandas as pd
import numpy as np


class HandlerGeneral:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df


    def remove_cols(self):
        """
        Removes columns with info about the system used to
        collect the data at the hospitals. Those columns
        are not very useful for analysis.
        All years have those columns.
        Columns are ...
        # TODO: add columns to documentation
        param:
            df (pd.DataFrame): DataFrame to remove columns
        return:
            pd.DataFrame: DataFrame without columns
        """
        list_ = ['ORIGEM', 'DTRECEBIM', 'DIFDATA',
                 'DTCADASTRO', 'TIPOBITO', 'DTCADINV']

        self.df.drop(list_, axis=1, inplace=True)


    def remove_ignored_values(self):
        """
        Removes the ignored category from columns.
        An ignored value is a value that wasn't collected,
        was out of the column's limit or was chosen to not
        be collected.
        """
        values = {
            'IDADE': 999,
            'ESTCIV': 9,

        }

        self.df.replace(values, np.nan, inplace=True)


    def optimize_dtypes(self):
        """
        CSV files, used as extension for all SIM files,
        can't hold info. about dtypes, and pandas defaults
        to 64 format. This function aims to reduce the
        memory used by converting to 32 instead
        """
        for i in self.df.columns:
            if self.df[i].dtype == np.int64:
                self.df[i] = self.df[i].astype(np.int32)
            elif self.df[i].dtype == np.float64:
                self.df[i] = self.df[i].astype(np.float32)


    def order_columns(self):
        order = [
            'DTNASC', 'IDADE', 'SEXO', 'PESO', 'RACACOR',
            'ESTCIV', 'OCUP',

            'ESC', 'ESC2010', 'ESCFALAGR1', 'SERIESCFAL',

            'CODMUNRES', 'CODMUNNATU', 'NATURAL',

            'IDADEMAE', 'ESCMAE', 'ESCMAE2010', 'ESCMAEAGR1',
            'SERIESCMAE', 'OCUPMAE',

            'QTDFILVIVO', 'QTDFILMORT', 'GESTACAO', 'GRAVIDEZ',
            'PARTO', 'OBITOGRAV', 'OBITOPARTO', 'OBITOPUERP',

            'DTOBITO', 'HORAOBITO', 'LOCOCOR', 'CODESTAB',
            'CODMUNOCOR',

            'CAUSABAS', 'CAUSABAS_O', 'CB_PRE', 'CAUSAMAT',
            'LINHAA', 'LINHAB', 'LINHAC', 'LINHAD', 'LINHAII',
            'ALTCAUSA',

            'CIRCOBITO', 'ACIDTRAB',

            'ASSISTMED', 'EXAME', 'CIRURGIA', 'NECROPSIA',

            'ATESTADO', 'ATESTANTE',

            'TPPOS', 'DTINVESTIG', 'DTCONINV', 'FONTEINV',
            'FONTES', 'TPNIVELINV', 'TPRESGINFO'
        ]

        additional = [col for col in self.df.columns
                     if col not in order]

        self.df = self.df[order + additional]


    def pipeline(self, output_file: str):
        self.remove_cols()
        self.remove_ignored_values()
        self.optimize_dtypes()
        self.order_columns()

        self.df.to_parquet(output_file,
                           compression='gzip')
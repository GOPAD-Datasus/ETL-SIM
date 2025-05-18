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
                 'DTCADASTRO']

        self.df.drop(list_, axis=1, inplace=True)


    def remove_ignored_values(self):
        """
        Removes the ignored category from columns.
        An ignored value is a value that wasn't collected,
        was out of the column's limit or was chosen to not
        be collected.
        """
        values = {}

        self.df.replace(values, np.nan, inplace=True)


    def pipeline(self, output_file: str):
        self.remove_cols()
        self.remove_ignored_values()

        self.df.to_parquet(output_file,
                           compression='gzip')
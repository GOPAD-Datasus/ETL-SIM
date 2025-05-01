import pandas as pd

from etl.extraction.utils import *
from etl.transformation.utils import *
from etl.transformation.year_specific import *


def transform (params: dict) -> None:
    initial_year = params['starting_year']
    final_year = params['ending_year'] + 1

    pro_folder = '../temp/processed'
    raw_folder = '../temp/raw'

    for year in range(initial_year, final_year):
        input_file = f'{raw_folder}/DO{year}.csv'
        output_file = f'{pro_folder}/DO{year}.parquet.gzip'

        if check_file_exists(output_file):
            continue

        df = apply_year_specific_changes(input_file, year)

        # General changes
        df = remove_cols(df)
        df = optimize_dtypes(df)

        df.to_parquet(output_file,
                      compression='gzip')

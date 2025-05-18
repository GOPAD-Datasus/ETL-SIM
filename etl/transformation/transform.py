from etl.extraction.utils import *
from etl.transformation.general_handler import *
from etl.transformation.year_specific import *


def transform (params: dict) -> None:
    initial_year = params['starting_year']
    final_year = params['ending_year'] + 1

    pro_folder = 'temp/processed'
    raw_folder = 'temp/raw'

    for year in range(initial_year, final_year):
        input_file = f'{raw_folder}/DO{year}.csv'
        output_file = f'{pro_folder}/DO{year}.parquet.gzip'

        if check_file_exists(output_file):
            continue

        HandlerGeneral(
            handle_year(input_file, year)
        ).pipeline(output_file)

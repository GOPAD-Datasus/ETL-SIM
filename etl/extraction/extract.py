from urllib.request import urlretrieve
import urllib.error

from etl.extraction.utils import *

def extract(params: dict) -> None:
    initial_year = params['starting_year']
    final_year = params['ending_year'] + 1

    dict_ = get_urls()

    make_temp_folder()
    raw_folder = '../temp/raw'

    for i in range(initial_year, final_year):
        source_link = dict_[i]
        output_file = f'{raw_folder}/DO{i}.csv'

        if not check_file_exists(output_file):
            try:
                urlretrieve(source_link,
                            output_file)
            except urllib.error.URLError:
                error_ = (f'Url corresponding to year {i}'
                          f' is wrong, try changing on'
                          f' extraction/utils.py inside'
                          f' get_urls()')
                raise RuntimeWarning(error_)


if __name__ == '__main__':
    extract()
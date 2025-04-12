from urllib.request import urlretrieve
import os
import json


def extract() -> None:
    with open('../parameters/extract_urls.json') as json_file:
        dict_ = json.load(json_file)

    temp_folder = '../temp'
    if not os.path.exists(temp_folder):
        os.mkdir(temp_folder)

    for i in range(12, 24):
        link = dict_[f'20{i}']
        output_file = f'DO20{i}.csv'

        urlretrieve(link,
                    f'{temp_folder}/{output_file}')


if __name__ == '__main__':
    extract()
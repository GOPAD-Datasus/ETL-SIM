import importlib
import pandas as pd

import etl.transformation.year_specific.year_2012


def handle_year (path: str, year: int) -> pd.DataFrame:
    package = f'etl.transformation.year_specific.year_{year}'
    handler = getattr(importlib.import_module(package),
                      f'Handler{year}')
    return handler(path).pipeline()

#! Python 3.9.8

import pandas as pd

def extractor(data):
    df = pd.read_html(data)
    return df

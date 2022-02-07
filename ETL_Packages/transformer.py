#!python3.9.8
import os
from numpy import double, float16
import pandas as pd
import csv
import dateutil
import requests
from datetime import datetime
from sqlalchemy import create_engine
from ETL_Packages import extractor
from ETL_Packages import transformer
from ETL_Packages import loader
import scrapy.selector as Selector
from scrapy.http import HtmlResponse   

def transformer(data, link, tahun, initial):
    #Make empty list
    df = pd.DataFrame(columns=['Nomor Urut', 'Nama', 'Kekayaan Bersih (Juta US$)', 'Usia', 'Kebangsaan', 'Sumber Kekayaan'])
    link = link
    tahun = tahun
    data = data
    table_shape = data[1].shape
    table_rows = table_shape[0]
    initial = initial
    index_tahun = {'Legenda':0, 2018:initial+1, 2017:initial+2, 2016:initial+3, 2015:initial+4, 2014:initial+5, 2013:initial+6}
    index_tahun_pilihan = [index_tahun[tahun]][0]
    df = df.append(data[index_tahun_pilihan])
    body = requests.get(link).content
    response = HtmlResponse(url=link, body=body)
    for i in list(range(table_rows)):
        no_body = f'//*[@id="mw-content-text"]/div[1]/table[2]/tbody/tr[{i+2}]/td[1]/text()'
        kekayaan_body = f'//*[@id="mw-content-text"]/div[1]/table[2]/tbody/tr[{i+2}]/td[3]/text()'
        sumber_kekayaan_body = f'//*[@id="mw-content-text"]/div[1]/table[2]/tbody/tr[{i+2}]/td[6]/a/text()'
        forbes_milliarder_no_body = response.xpath(no_body).get()
        forbes_milliarder_kekayaan_body = response.xpath(kekayaan_body).get()
        forbes_milliarder_kekayaan_body = (str(forbes_milliarder_kekayaan_body).replace('$', '')).replace('miliar', '')
        forbes_milliarder_kekayaan_body = float(forbes_milliarder_kekayaan_body)*1000
        forbes_milliarder_sumber_kekayaan_body = response.xpath(sumber_kekayaan_body).get()
        df.at[i, 'Nomor Urut'] = forbes_milliarder_no_body
        df.at[i, 'Kekayaan Bersih (Juta US$)'] = forbes_milliarder_kekayaan_body
        df.at[i, 'Sumber Kekayaan'] = forbes_milliarder_sumber_kekayaan_body
    df.drop(df.columns[[6,7,8]], axis=1, inplace=True)
    
    print(f"List orang-orang terkaya pada tahun {tahun} menurut majalah Forbes adalah sebagai berikut: \n \n", df)

    return df
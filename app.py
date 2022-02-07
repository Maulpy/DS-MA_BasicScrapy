#!python3.9.8

from ETL_Packages import extractor
from ETL_Packages import transformer
from ETL_Packages import loader

if __name__=="__main__":

    #ETL - EXTRACT
    link = 'https://id.wikipedia.org/wiki/Daftar_miliarder_Forbes'
    data = extractor.extractor(link)

    #ETL - TRANSFORMATION
    tahun = 2018
    initial = 0
    nama_peserta_orang_terkaya_forbes = transformer.transformer(data, link, tahun, initial)

    #ETL - LOADER
    loader.loader(nama_peserta_orang_terkaya_forbes)
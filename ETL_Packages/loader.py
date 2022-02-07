#!Python 3.9.8

import os
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2

#ETL - Load.
def loader(table):
    table = table
    path_config = os.getcwd()
    # print(path_config)
    with open(path_config + '\config\config.conf') as file:
        conf = file.readlines()

    print(conf)
    db = {}
    for line in conf:
        line = line.replace('\n', '').split('=')
        # print(line)
        if len(line)==2:
            db[line[0]] = line[1]
        
    # print(db)

    # Connection DB
    conn_string = f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
    engine = create_engine(conn_string)
    # print(engine)

    #Create table
    the_table = table.to_sql(name='nama_peserta_orang_terkaya_forbes', con=engine, if_exists='replace', index=False)

    return the_table
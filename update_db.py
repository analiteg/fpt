#!/home/analiteg/.virtualenvs/forexpairtrading/bin/python
import pandas as pd
import numpy as np
import mysql.connector
import psycopg2
import sys
import time

param_dic = {
    "host"      : "207.180.239.158",
    "database"  : "forex",
    "user"      : "postgres",
    "password"  : "mfocHkQ33#"
}

connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
    param_dic['user'],
    param_dic['password'],
    param_dic['host'],
    param_dic['database']
)

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    return conn

# Функция устанавливет подключение к MySQL
def conn_to_mysql ():
    """ Connect to the MySQL database server """
    mysql_conn = None
    try:
        mysql_conn = mysql.connector.connect(
        host="185.182.9.198",
        user="analiteg88",
        password="analiteg88#1312357",
        port = 3306,
        database='forex')
    except (Exception) as error:
        print(error)
        sys.exit(1) 
    return mysql_conn

def execute_mogrify(conn, df, table):
    """
    Using cursor.mogrify() to build the bulk insert query
    then cursor.execute() to execute the query
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    cursor = conn.cursor()
    values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
    query  = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)
    try:
        cursor.execute(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print('{} done'.format(table))
    cursor.close()



while True:
    try:
        conn = connect(param_dic) # connect to the database
        query="select dt from ticks.minutes order by dt desc LIMIT 1;"
        df = pd.read_sql_query(query,conn)

        if len(df.index) ==0:
            x = '2021-12-28'
        else: 
            x = df.dt[0]

        mysql_connect = conn_to_mysql ()
        query = "select * from forex.minutes where dt > '{}';".format(x)
        df = pd.read_sql_query(query,mysql_connect)
        mysql_connect.close()
        df.rename({'close_audcad': 'audcad', 'close_audchf': 'audchf', 'close_audjpy': 'audjpy', 'close_audnzd': 'audnzd', 'close_audusd': 'audusd', 'close_cadchf': 'cadchf', 'close_cadjpy': 'cadjpy', 'close_chfjpy': 'chfjpy', 'close_euraud': 'euraud', 'close_eurchf': 'eurchf', 'close_eurcad': 'eurcad','close_eurgbp': 'eurgbp','close_eurjpy': 'eurjpy','close_eurnzd': 'eurnzd','close_eurusd': 'eurusd','close_gbpaud': 'gbpaud','close_gbpchf': 'gbpchf','close_gbpusd': 'gbpusd','close_gbpjpy': 'gbpjpy','close_gbpcad': 'gbpcad','close_gbpnzd': 'gbpnzd','close_nzdcad': 'nzdcad','close_nzdchf': 'nzdchf','close_nzdjpy': 'nzdjpy','close_nzdusd': 'nzdusd','close_usdcad': 'usdcad','close_usdchf': 'usdchf','close_usdjpy': 'usdjpy','close_xauusd': 'xauusd','close_xagusd': 'xagusd','close_brent': 'brent','close_wti': 'wti',}, axis=1, inplace=True)
        data = df.iloc[:,1:]

        table = 'ticks.minutes'
        execute_mogrify(conn, data, table)
        conn.close()

        time.sleep(60)

    except (Exception) as error:
        print(error)
        sys.exit(1)

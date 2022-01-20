#!/home/analiteg/.virtualenvs/forexpairtrading/bin/python
import pandas as pd
import numpy as np
import psycopg2
import sys
import time
from datetime import date,timezone, datetime,timedelta
import pytz
import statsmodels
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint, adfuller,ccf
import requests

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
    values = [cursor.mogrify("(%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
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

#Отпаравляет сообщения в Telegram Bot
def send_msg(text):
   token = "2092288888:AAHZbw3rTu0YKE0h2ZZAetOe9vgn_eqWLS4"
   chat_id = "146151553"
   url_req = "https://api.telegram.org/bot" + token + "/sendMessage" + "?chat_id=" + chat_id + "&text=" + text
   results = requests.get(url_req)

if __name__ == "__main__":

    while True:

        date_range = 20
        start_date= (date.today() - timedelta(date_range)).strftime("%Y-%m-%d")
        end_date= date.today().strftime("%Y-%m-%d")
        business_day = np.is_busday(end_date)

        if business_day == True:
            
            time_now = datetime.now()

            if (time_now.hour > 1 and time_now.hour < 3):
               

                #Проверяем есть ли уже в базе данные за сегодняшний день
                conn = connect(param_dic) # connect to the database
                query="select dt from ticks.data_stats WHERE dt > date('{}') order by dt DESC LIMIT 1;".format(start_date)
                data_1 = pd.read_sql_query(query,conn)
                conn.close()
                last_update_date = data_1["dt"][0].strftime("%Y-%m-%d")

                if last_update_date < end_date:

                    #Get tickers
                    tickers = ['audcad','audchf','audjpy','audnzd','audusd','cadchf','cadjpy', 'chfjpy','euraud', 'eurchf', 'eurcad','eurgbp','eurjpy','eurnzd','eurusd','gbpaud','gbpchf','gbpusd','gbpjpy','gbpcad','gbpnzd','nzdcad','nzdchf','nzdjpy','nzdusd','usdcad','usdchf','usdjpy','xauusd']

                    conn = connect(param_dic) # connect to the database
                    query="select * from ticks.minutes WHERE dt between '{}' AND  date('{}') + 1 order by dt;".format(start_date,end_date)
                    data = pd.read_sql_query(query,conn)
                    conn.close()
                    data["Date"] = pd.to_datetime(data["dt"], format="%Y-%m-%d")
                    df = data.iloc[:, :29].copy()

                    n = df.shape[1]
                    keys = df.keys()
                    pairs = []
                    pairs_df = []

                    for i in range(n):
                            for j in range(n):
                                if keys[i]==keys[j]:
                                    continue
                                else:
                                    S1 = df[keys[i]]
                                    S2 = df[keys[j]]
                                    pvalue_result = coint(S1, S2)
                                    pvalue =float('%.5f'%(pvalue_result[1]))
                                    corel_pearson = ccf(S1, S2, adjusted=True,fft=True)
                                    corelation_result = float('%.5f'%(corel_pearson[0]))
                                    print((keys[i], keys[j],pvalue,corelation_result))
                                    
                                    pairs.append( "{},{},{},{}".format(keys[i], keys[j],pvalue,corelation_result))
                                    pairs_df.append((keys[i],keys[j],pvalue,corelation_result,end_date))

                    file_name = "/home/analiteg/cointegration/{}.csv".format(end_date)
                    with open (file_name,"w")as fp:
                        for line in pairs:
                            fp.write(line+"\n") 
                    
                    
                    try:

                        data_stats_df = pd.DataFrame(pairs_df, columns =['pair1', 'pair2', 'cointegration','correlation','dt'])
                        table = 'ticks.data_stats'
                        conn = connect(param_dic) # connect to the database
                        execute_mogrify(conn, data_stats_df, table)
                        conn.close()

                        message = "Data updated"
                        send_msg(message)
 

                    except (Exception) as error:
                        print(error)
                        sys.exit(1)

        
        time.sleep(300)

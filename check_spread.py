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
from statsmodels.tsa.stattools import coint, adfuller
import requests
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
    
#Z-score
def zscore(series):
    return (series - series.mean()) / np.std(series)

#Отпаравляет сообщения в Telegram Bot
def send_msg(text,chat_id):
   token = "2092288888:AAHZbw3rTu0YKE0h2ZZAetOe9vgn_eqWLS4"
   #chat_id = "146151553"
   url_req = "https://api.telegram.org/bot" + token + "/sendMessage" + "?chat_id=" + chat_id + "&text=" + text
   results = requests.get(url_req)

chat_id_1 ='-1001395140536' #Парный Трейдинг
chat_id_2 ='-1001485624593' #FOREX PAIR TRADING

if __name__ == "__main__":


    
    while True:

        date_range = 20
        spread_value_list=[]
        start_date= (date.today() - timedelta(date_range)).strftime("%Y-%m-%d")
        end_date= date.today().strftime("%Y-%m-%d")
        today= date.today()
        business_day = np.is_busday(end_date)

        if business_day == True:

            try:
                #Select tickers from db
                try:
                    conn = connect(param_dic) # connect to the database
                    cointegration = 0.06
                    correlation = 0.8
                    query= "SELECT a.pair1,a.pair2,a.correlation,a.cointegration,a.dt,a.send_signal, a.go_signal FROM ticks.data_stats a, ticks.data_stats b WHERE a.cointegration < b.cointegration AND a.correlation = b.correlation AND a.correlation > {} AND a.cointegration < {} AND a.dt >= date('{}') ORDER BY a.dt DESC; ".format(correlation,cointegration,today)
                    tickers = pd.read_sql_query(query,conn)
                    conn.close()
                except (Exception) as error:
                    print(error)
                    sys.exit(1)



                conn = connect(param_dic) # connect to the database
                query="select * from ticks.minutes WHERE dt between '{}' AND  date('{}') + 1 order by dt;".format(start_date,end_date)
                data = pd.read_sql_query(query,conn)
                conn.close()
                data["Date"] = pd.to_datetime(data["dt"], format="%Y-%m-%d")
                df = data.iloc[:, :29].copy()


                for i in range(0,tickers.shape[0]):
                    pair1 = tickers['pair1'].iloc[i]
                    pair2 = tickers['pair2'].iloc[i]
                    send_signal = tickers['send_signal'].iloc[i]
                    go_signal = tickers['go_signal'].iloc[i]

                    
                    #Ready signal
                    if send_signal == 0:

                        #Spread data
                        spread_max = 2.3
                        spread_min = -2.3
                        fuler_max = 0.03

                        S1 = np.log(df[pair1])
                        S2 = np.log(df[pair2])
                        S1 = sm.add_constant(S1)
                        results = sm.OLS(S2, S1).fit()
                        S1 = S1[pair1]
                        b = results.params[pair1]
                        spread = S2 - b * S1
                        z_score = zscore(spread)
                        

                        if z_score.iloc[-1] > spread_max  or z_score.iloc[-1] < spread_min:

                            ad_fuller = float('%.5f'%(adfuller(spread)[1]))

                            if ad_fuller < fuler_max:

                                if float('%.2f'%(z_score.iloc[-1])) > 0:
                                    spread_value = "READY to BUY {} - SELL {} Spread deviation =  {}\n".format(pair1,pair2,float('%.2f'%(z_score.iloc[-1])))
                                else:
                                    spread_value = "READY to SELL {} - BUY {} Spread deviation =  {}\n".format(pair1,pair2,float('%.2f'%(z_score.iloc[-1])))

                                conn = connect(param_dic) # connect to the database
                                cursor = conn.cursor()
                                query="UPDATE ticks.data_stats SET send_signal = 1 WHERE pair1 LIKE('{}') AND pair2 LIKE('{}') AND dt >='{}';".format(pair1,pair2,end_date)
                                cursor.execute(query)
                                conn.commit()
                                conn.close()

                                if len(spread_value) > 0:
                                    str1 = ''.join(map(str,spread_value))
                                    message = str1
                                    send_msg(message,chat_id_1)
                                    send_msg(message,chat_id_2)
                    #GO signal
                    if send_signal == 1 and go_signal == 0:

                        #Spread data
                        spread_max = 2
                        spread_min = -2
                        fuler_max = 0.03

                        S1 = np.log(df[pair1])
                        S2 = np.log(df[pair2])
                        S1 = sm.add_constant(S1)
                        results = sm.OLS(S2, S1).fit()
                        S1 = S1[pair1]
                        b = results.params[pair1]
                        spread = S2 - b * S1
                        z_score = zscore(spread)
                        

                        if (z_score.iloc[-1] > 0 and z_score.iloc[-1] < spread_max)  or (z_score.iloc[-1] > spread_min and z_score.iloc[-1] < 0):

                            ad_fuller = float('%.5f'%(adfuller(spread)[1]))

                            if ad_fuller < fuler_max:

                                if float('%.2f'%(z_score.iloc[-1])) > 0:
                                    spread_value = "GO to BUY {} - SELL {} Spread deviation =  {}\n".format(pair1,pair2,float('%.2f'%(z_score.iloc[-1])))
                                else:
                                    spread_value = "GO to SELL {} - BUY {} Spread deviation =  {}\n".format(pair1,pair2,float('%.2f'%(z_score.iloc[-1])))

                                conn = connect(param_dic) # connect to the database
                                cursor = conn.cursor()
                                query="UPDATE ticks.data_stats SET go_signal = 1 WHERE pair1 LIKE('{}') AND pair2 LIKE('{}') AND dt >='{}';".format(pair1,pair2,end_date)
                                cursor.execute(query)
                                conn.commit()
                                conn.close()

                                if len(spread_value) > 0:
                                    str1 = ''.join(map(str,spread_value))
                                    message = str1
                                    send_msg(message,chat_id_1)
                                    send_msg(message,chat_id_2)


            except (Exception) as error:
                print(error)
                sys.exit(1)
        
        
        time.sleep(120)
        
        


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


#Отпаравляет сообщения в Telegram Bot
def send_msg(text,chat_id):
   token = "2092288888:AAHZbw3rTu0YKE0h2ZZAetOe9vgn_eqWLS4"
   #chat_id = "-1001395140536"
   url_req = "https://api.telegram.org/bot" + token + "/sendMessage" + "?chat_id=" + chat_id + "&text=" + text
   results = requests.get(url_req)

chat_id_1 ='-1001395140536'
chat_id_2 ='-1001485624593'


date_range = 20
start_date= (date.today() - timedelta(date_range)).strftime("%Y-%m-%d")
end_date= date.today().strftime("%Y-%m-%d")
bd= np.is_busday(end_date)
today= date.today()
time_now = datetime.now()


              

#Проверяем есть ли уже в базе данные за сегодняшний день
conn = connect(param_dic) # connect to the database
query="SELECT a.pair1,a.pair2,a.correlation,a.cointegration,a.dt,a.send_signal FROM ticks.data_stats a, ticks.data_stats b WHERE a.cointegration < b.cointegration AND a.correlation = b.correlation AND a.correlation >0.8 AND a.cointegration <0.06 AND a.dt >= date('{}') ORDER BY a.dt DESC; ".format(today)
data_1 = pd.read_sql_query(query,conn)
   



message = "Test"
send_msg(message,chat_id_1)
send_msg(message,chat_id_2)
 

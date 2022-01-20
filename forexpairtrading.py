import dash
from dash import dcc
from dash import html
from dash.dependencies import Output, Input, State
import pandas as pd
import numpy as np
from datetime import date,timezone, datetime,timedelta
import statsmodels
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint, adfuller,ccf
import sys
import psycopg2
import dash_table

# Variables
date_range = 20

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


external_stylesheets = [
    {
        "href": "https://fonts.googleapis.com/css2?"
        "family=Lato:wght@400;700&display=swap",
        "rel": "stylesheet",
    },
]

external_scripts = [
    {'async src': 'https://www.googletagmanager.com/gtag/js?id=G-7DRKWMTL8F'}
]

app = dash.Dash(__name__,external_scripts=external_scripts,external_stylesheets=external_stylesheets)
app.title = "Forex Pair Trading"
server = app.server

def serve_layout():

    pairs_list = ['audcad','audchf','audjpy','audnzd','audusd','cadchf','cadjpy', 'chfjpy','euraud', 'eurchf', 'eurcad','eurgbp','eurjpy','eurnzd','eurusd','gbpaud','gbpchf','gbpusd','gbpjpy','gbpcad','gbpnzd','nzdcad','nzdchf','nzdjpy','nzdusd','usdcad','usdchf','usdjpy','xauusd','xagusd','brent','wti']
    return html.Div(
    children=[
        html.Div(
            children=[
                html.H1(
                    children="FOREXPAIRTRADING.COM", className="header-title"
                ),
            ],
            className="header",
        ),
        html.Div(
            children=[
                html.Div(
                    children=[
                        html.Div(children="Pair-1", className="menu-title"),
                        dcc.Dropdown(
                            id="region-filter",
                            options=[
                                {"label": pair_1, "value": pair_1}
                                for pair_1 in pairs_list
                            ],
                            value="eurjpy",
                            clearable=False,
                            searchable=False,
                            className="dropdown",
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.Div(children="Pair-2", className="menu-title"),
                        dcc.Dropdown(
                            id="type-filter",
                            options=[
                                {"label": pair_2, "value": pair_2}
                                for pair_2 in pairs_list
                            ],
                            value="gbpjpy",
                            clearable=False,
                            searchable=False,
                            className="dropdown",
                        ),
                    ],
                ),
                html.Div(
                    children=[
                        html.Div(
                            children="Date Range",
                            className="menu-title"
                            ),
                        dcc.DatePickerRange(
                            id="date-range",
                            min_date_allowed=(date.today() - timedelta(date_range)).strftime("%Y-%m-%d"),
                            max_date_allowed=date.today().strftime("%Y-%m-%d"),
                            start_date=(date.today() - timedelta(date_range)).strftime("%Y-%m-%d"),
                            end_date=date.today().strftime("%Y-%m-%d"),
                        ),
                    ]
                ),

            ],
            className="menu",
        ),

        html.Div(
            children=[

                html.Div(
                    children=dcc.Graph(
                        id="price-chart1", config={"displayModeBar": True}, style={'max-width': '1600px', 'height': '750px'},
                    ),
                    className="card",
                ),

                html.Div(
                    children=dcc.Graph(
                        id="volume-chart1", config={"displayModeBar": True}, style={'max-width': '1600px', 'height': '750px'},
                    ),
                    className="card",
                ),                 
            ],

            className="wrapper",
        ),
        
        html.Footer(
            children=[
            html.P('© 2021-2022 forexpairtarding.com'),
            html.A("Telegram", href='https://t.me/analiteg88', target="_blank")
            ],
            className="footer",
        ),
    ]
)



app.layout = serve_layout


@app.callback(
    [ Output("price-chart1", "figure"), Output("volume-chart1", "figure")],
    [   
        Input("region-filter", "value"),
        Input("type-filter", "value"),
        Input("date-range", "start_date"),
        Input("date-range", "end_date"),    
    ],
    
)

def update_charts(pair1, pair2, start_date, end_date):

    conn = connect(param_dic) # connect to the database
    query="select {},{},dt from ticks.minutes WHERE dt between '{}' AND  date('{}') + 1 order by dt;".format(pair1,pair2,start_date,end_date)
    data = pd.read_sql_query(query,conn)
    conn.close()
    data["Date"] = pd.to_datetime(data["dt"], format="%Y-%m-%d")
    
    #Spread 
    S1 = np.log(data[pair1])
    S2 = np.log(data[pair2])

    #Cointegration 
    pvalue_result = coint(S1, S2)
    pvalue =float('%.5f'%( pvalue_result[1]))
    corel_pearson = ccf(S1, S2, adjusted=True,fft=True)
    corelation_result = float('%.5f'%(corel_pearson[0]))

    S1 = sm.add_constant(S1)
    results = sm.OLS(S2, S1).fit()
    S1 = S1[pair1]
    b = results.params[pair1]
    spread = S2 - b * S1
    spread_m = spread.rolling(20).mean()

    #Augmented Dickey Fuller test spread for stationarity
    ad_fuller = float('%.5f'%(adfuller(spread)[1]))

    #Z-score
    def zscore(series):
        return (series - series.mean()) / np.std(series)
    z_score = zscore(spread)
    z_score_m = zscore(spread_m)

    #Text
    chart_text = "Percent Change | {}-{} | cointegration = {} | correlation = {} | spread stationarity = {}".format(pair1,pair2,pvalue,corelation_result,ad_fuller)
    spread_z_text = "Spread Deviation | {}-{} | {}".format(pair1,pair2, float('%.5f'%(z_score.iloc[-1])))
    
    

    Pair1 = ((data[pair1] - data[pair1].iloc[0])/data[pair1].iloc[0])*100
    Pair2 = ((data[pair2] - data[pair2].iloc[0])/data[pair2].iloc[0])*100

    volume_chart1_figure = {
        "data": [
            {
                "x": data.index,
                "y": z_score,
                "type": "lines",
                'name': 'Chart'
            },

            {
                "x": [0,data.index.max()],
                "y": [2,2],
                "type": "lines",
                "name": "+ 2 SD",
                'marker' : { "color" : "green"}
                
            },

            {
                "x": [0,data.index.max()],
                "y": [-2,-2],
                "type": "lines",
                "name": "- 2 SD",
                'marker' : { "color" : "red"}
                
            },
        ],
        "layout": {
            "title": {"text": spread_z_text, "x": 0.05, "xanchor": "left"},
            "xaxis": {"fixedrange": False},
            "yaxis": {"fixedrange": True},
            "colorway": ["#0661ED"],
        },
    }


    price_chart1_figure = {
        "data": [
            {
                "x": data.index,
                "y": Pair1,
                "type": "lines",
                "name": pair1,
                'marker' : { "color" : "#0661ED"}
            },

            {
                "x": data.index,
                "y": Pair2,
                "type": "lines",
                "name": pair2,
                'marker' : { "color" : "orange"}
            },
        ],
        "layout": {
            "title": {"text": chart_text, "x": 0.05, "xanchor": "left"},
            "xaxis": {"fixedrange": False},
            "yaxis": {"fixedrange": True},
            "colorway": ["#0661ED"],
        },
    }
    return price_chart1_figure,volume_chart1_figure




if __name__ == "__main__":
    app.run_server (port=5000,host='0.0.0.0',debug=True)

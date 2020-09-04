#%%
globals().clear()
import json
import requests
from pandas.io.json import json_normalize
import pandas as pd
from datetime import datetime, timedelta
import os


import dateutil.relativedelta

#%%
body_hasauthor={
    "rsid": "spiegel.ng.spieg.main",
    "globalFilters": [
        {
            "type": "dateRange",
            "dateRange": ""
        },
        {
            "type": "segment",
            "segmentDefinition": {
                "container": {
                    "func": "container",
                    "context": "hits",
                    "pred": {
                        "func": "streq",
                        "str": "has_author",
                        "val": {
                            "func": "attr",
                            "name": "variables/evar67"
                        },
                        "description": "Editorial has Author"
                    }
                },
                "func": "segment",
                "version": [
                    1,
                    0,
                    0
                ]
            }
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "0",
                "id": "metrics/visitors",
                "sort": "desc"
            }
        ]
    },
    "dimension": "variables/page",
    "settings": {
        "countRepeatInstances": True,
        "limit": 50000,
        "page": '',
        "nonesBehavior": "exclude-nones"
    },
    "statistics": {
        "functions": [
            "col-max",
            "col-min"
        ]
    }
}

body_noauthor={
    "rsid": "spiegel.ng.spieg.main",
    "globalFilters": [
        {
            "type": "dateRange",
            "dateRange": ""
        },
        {
            "type": "segment",
            "segmentDefinition": {
                "container": {
                    "func": "container",
                    "context": "hits",
                    "pred": {
                        "func": "streq",
                        "str": "no_author",
                        "val": {
                            "func": "attr",
                            "name": "variables/evar67"
                        },
                        "description": "Editorial has Author"
                    }
                },
                "func": "segment",
                "version": [
                    1,
                    0,
                    0
                ]
            }
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "0",
                "id": "metrics/visitors",
                "sort": "desc"
            }
        ]
    },
    "dimension": "variables/page",
    "settings": {
        "countRepeatInstances": True,
        "limit": 50000,
        "page": '',
        "nonesBehavior": "exclude-nones"
    },
    "statistics": {
        "functions": [
            "col-max",
            "col-min"
        ]
    }
}

#%%
api_key='b126a48b6e704dbba3107de1d4cb3187'

f = open("../data/processed/token.txt", "r")
aa_token=f.read()
#%%

request_url = "https://analytics.adobe.io/api/spiege2/reports/"
url = "https://analytics.adobe.io/api/spiege2/reports/"
company_id = "spiege2"

headers = {'Content-type': 'application/json;charset=utf-8',
           'Accept': 'application/json',
           "x-api-key": api_key,
            "x-proxy-global-company-id": company_id,
           "Authorization": "Bearer "+aa_token}
#%%
start_date=datetime(2020, 3, 7)
end_date=datetime.now().replace(hour=0, minute=0,second=0, microsecond=0)

#%%
body_hasauthor['globalFilters'][0]['dateRange'] = '{:%Y-%m-%dT%H:%M:%S.%f}/{:%Y-%m-%dT%H:%M:%S.%f}'.format(start_date, end_date)
body_noauthor['globalFilters'][0]['dateRange'] = '{:%Y-%m-%dT%H:%M:%S.%f}/{:%Y-%m-%dT%H:%M:%S.%f}'.format(start_date, end_date)
#%%



r = requests.post(url, json=body_hasauthor, headers=headers)
d = json.loads(r.content)
num_pages=d['totalPages']
df_response = pd.DataFrame(d.get('rows'))
df_r2 = []
df_api_final=pd.DataFrame()
for page in range(0, num_pages):
    print(page)
    body_hasauthor['settings']['page'] = page
    r = requests.post(url, json=body_hasauthor, headers=headers)
    d = json.loads(r.content)
    df_response = pd.DataFrame(d.get('rows'))
    df_r2.append(df_response)
df_api_final = pd.concat(df_r2)
df_hasauthor = pd.DataFrame()
df_hasauthor['Page'] = df_api_final['value']
df_hasauthor['UniqueVisitors'] = pd.DataFrame(df_api_final.data.tolist())
df_hasauthor['author']='has_author'

#%%


r = requests.post(url, json=body_noauthor, headers=headers)
d = json.loads(r.content)
num_pages=d['totalPages']
df_response = pd.DataFrame(d.get('rows'))
df_r2 = []
df_api_final=pd.DataFrame()
for page in range(0, num_pages):
    print(page)
    body_noauthor['settings']['page'] = page
    r = requests.post(url, json=body_noauthor, headers=headers)
    d = json.loads(r.content)
    df_response = pd.DataFrame(d.get('rows'))
    df_r2.append(df_response)
df_api_final = pd.concat(df_r2)
df_noauthor = pd.DataFrame()
df_noauthor['Page'] = df_api_final['value']
df_noauthor['UniqueVisitors'] = pd.DataFrame(df_api_final.data.tolist())
df_noauthor['author']='no_author'

#%%
df_author=pd.concat([df_hasauthor,df_noauthor])

path = "../data/input/article_hasauthor.csv"

df_author.to_csv(path)
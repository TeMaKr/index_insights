#!/usr/bin/env python
# coding: utf-8
globals().clear()
# In[43]:


import numpy as np
import pandas as pd


# In[44]:


#import scipy
#import itables.interactive
#from itables import show
import locale
locale.setlocale(locale.LC_TIME,'de_DE')
#import datetime
#from datetime import timedelta
#import itables.options as opt
#opt.lengthMenu = [ 5, 10, 20, 50, 100]


# In[45]:


#df_PIX=pd.read_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/input/PIX_data_article_OLD_20200724.csv", encoding='utf-8', sep=",")
#df_AIX=pd.read_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/input/AIX_data_article_OLD_20200724.csv", encoding='utf-8', sep=",")

df_PIX=pd.read_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/input/PIX_data_article_20200821.csv", encoding='utf-8', sep=",")
df_AIX=pd.read_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/input/AIX_data_article_20200821.csv", encoding='utf-8', sep=",")


# In[46]:


df_PIX['Minuten']=df_PIX['ZeitintervallPIX']/60
df_PIX['Stunden']=df_PIX['Minuten']/60
df_AIX['Minuten']=df_AIX['Zeitintervall']/60
df_AIX['Stunden']=df_AIX['Minuten']/60


# In[47]:

col_AIX1=['ArticleId',
           'MaxPublishDate',
           'Title',
          'Heading',
          'Intro',
           'WordCount',
           'IndexGesamt',
           'Lesedauer',
           'IndexRW',
           'IndexLD',
           'Zeitintervall',
           'Minuten',
           'Stunden',
           'Channel',
           'Subchannel',
           'isPlus',
           'Type',
           'Style',
           'Genre',
           'ContainsVideo',
           'ContainsSlideshow',
           'ContainsPodcast',
           'IsEvergreen',
           '_Version']

col_AIX2=['ArticleId',
           'MaxPublishDate',
           'Title','Heading',
          'Intro',
           'WordCount',
           'IndexGesamt',
           'Lesedauer',
           'IndexRW',
           'IndexLD',
           'Zeitintervall',
           'Minuten',
           'Stunden',
           'Channel',
           'Subchannel',
           'isPlus',
           'Type',
           'Style',
           'Genre',
           'ContainsVideo',
           'ContainsSlideshow',
           'ContainsPodcast',
           'IsEvergreen']

col_PIX1=['ArticleId',
           'MaxPublishDate',
           'Title','Heading',
          'Intro',
           'WordCount',
           'IndexGesamtPIX',
           'LesedauerPIX',
           'IndexRWPIX',
           'IndexLDPIX',
           'ZeitintervallPIX',
           'Minuten',
           'Stunden',
           'Channel',
           'Subchannel',
           'isPlus',
           'Type',
           'Style',
           'Genre',
           'ContainsVideo',
           'ContainsSlideshow',
           'ContainsPodcast',
           'IsEvergreen',
           '_Version']

col_PIX2= ['ArticleId',
           'MaxPublishDate',
           'Title','Heading',
          'Intro',
           'WordCount',
           'IndexGesamtPIX',
           'IndexLDPIX',
           'IndexRWPIX',
           'ZeitintervallPIX',
           'Minuten',
           'Stunden',
           'Channel',
           'Subchannel',
           'isPlus',
           'Type',
           'Style',
           'Genre',
           'ContainsVideo',
           'ContainsSlideshow',
           'ContainsPodcast',
           'IsEvergreen']
# In[48]:
import datetime

year=2020
month=5
day=31

maxdate=datetime.datetime(year, month,day,0, 0, 0)

#%%

df_A=df_AIX[col_AIX1].round(1)
df_A['_Version']=pd.to_datetime(df_A['_Version'])
df_A['MaxPublishDate']=pd.to_datetime(df_A['MaxPublishDate'])

df_A.drop(df_A[(df_A['Channel']=='Backstage')
               | (df_A['Channel']=='Services')
               | (df_A['Channel']=='Tests')
               | (df_A['Channel']=='International')
               | (df_A['Channel']=='Dein SPIEGEL')
               | (df_A['Heading'].str.contains('Hausmitteilung', regex=False , na=False)) ].index, inplace = True)
# In[49]:

df_P=df_PIX[col_PIX1].round(1)
df_P['_Version']=pd.to_datetime(df_P['_Version'])
df_P['MaxPublishDate']=pd.to_datetime(df_P['MaxPublishDate'])

df_P.drop(df_P[(df_P['Channel']=='Backstage')
               | (df_P['Channel']=='Services')
               | (df_P['Channel']=='Tests')
               | (df_P['Channel']=='International')
               | (df_P['Channel']=='Dein SPIEGEL')
               | (df_P['Heading'].str.contains('Hausmitteilung', regex=False , na=False)) ].index, inplace = True)

# In[50]:

list_gb1=['ArticleId', 'MaxPublishDate','Stunden']

idxa = df_A.groupby(list_gb1)['_Version'].transform(max) == df_A['_Version']
dfAIX=df_A[idxa][col_AIX2].rename(columns={"MaxPublishDate": "PublishDate","IndexGesamt": "Index"}, errors="raise")
dfAIX['LD_RW']=dfAIX['IndexLD']-dfAIX['IndexRW']

idxp = df_P.groupby(list_gb1)['_Version'].transform(max) == df_P['_Version']
dfPIX=df_P[idxp][col_PIX2].rename(columns={"MaxPublishDate": "PublishDate","IndexGesamtPIX": "IndexPIX"}, errors="raise")
dfPIX['LD_RW']=dfPIX['IndexLDPIX']-dfPIX['IndexRWPIX']



#%%

idya = dfAIX.groupby('ArticleId')['Stunden'].transform(max) == dfAIX['Stunden']
dfAIX_max=dfAIX[idya][dfAIX['Stunden']>=120]

idyp = dfPIX.groupby('ArticleId')['Stunden'].transform(max) == dfPIX['Stunden']
dfPIX_max=dfPIX[idyp][dfPIX['Stunden']>=120]




# In[52]:

#dfP_7T_OLD=dfPIX[(dfPIX['Stunden']==168) & (dfPIX['PublishDate']<=maxdate ) ].drop(columns=['Stunden']).set_index('ArticleId')
#dfA_7T_OLD=dfAIX[(dfAIX['Stunden']==168) & (dfAIX['PublishDate']<=maxdate )].drop(columns=['Stunden']).set_index('ArticleId')

dfP_7T2=dfPIX_max[(dfPIX_max['PublishDate']>maxdate )].drop(columns=['Stunden']).set_index('ArticleId')
dfA_7T2=dfAIX_max[(dfAIX_max['PublishDate']>maxdate )].drop(columns=['Stunden']).set_index('ArticleId')

#dfP_7T2_OLD=dfPIX_max[(dfPIX_max['PublishDate']<=maxdate )].drop(columns=['Stunden']).set_index('ArticleId')
#dfA_7T2_OLD=dfAIX_max[(dfAIX_max['PublishDate']<=maxdate )].drop(columns=['Stunden']).set_index('ArticleId')


#%%
#dfP_7T=dfPIX[(dfPIX['Stunden']==168) & (dfPIX['PublishDate']>maxdate )].drop(columns=['Stunden']).set_index('ArticleId')
#dfA_7T=dfAIX[(dfAIX['Stunden']==168) & (dfAIX['PublishDate']>maxdate )].drop(columns=['Stunden']).set_index('ArticleId')

# In[53]:
#dfP_1T_OLD=dfPIX[(dfPIX['Stunden']==24) & (dfPIX['PublishDate']<=maxdate ) ].drop(columns=['Stunden']).set_index('ArticleId')
#dfA_1T_OLD=dfAIX[(dfAIX['Stunden']==24) & (dfAIX['PublishDate']<=maxdate )].drop(columns=['Stunden']).set_index('ArticleId')

#dfP_1T=dfPIX[(dfPIX['Stunden']==24) & (dfPIX['PublishDate']>=maxdate )].drop(columns=['Stunden']).set_index('ArticleId')
#dfA_1T=dfAIX[(dfAIX['Stunden']==24) & (dfAIX['PublishDate']>=maxdate )].drop(columns=['Stunden']).set_index('ArticleId')

# In[54]:


#dfP.groupby(['ArticleId']).nunique().sort_values('IndexPIX')


# In[55]:


#show(dfP, maxBytes=0)

# In[56]:
#dfP_7T2_OLD.to_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/processed/PIX_Report_7T2_OLD.csv")
#dfA_7T2_OLD.to_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/processed/AIX_Report_7T2_OLD.csv")

#dfP_1T_OLD.to_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/processed/PIX_Report_1T_OLD.csv")
#dfA_1T_OLD.to_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/processed/AIX_Report_1T_OLD.csv")
# In[56]:
#dfP_7T.to_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/processed/PIX_Report_7T.csv")
#dfA_7T.to_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/processed/AIX_Report_7T.csv")

# In[57]:
#dfP_1T.to_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/processed/PIX_Report_1T.csv")
#dfA_1T.to_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/processed/AIX_Report_1T.csv")


#%%
# In[56]:
dfP_7T2.to_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/processed/PIX_Report_7T2.csv")
dfA_7T2.to_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/processed/AIX_Report_7T2.csv")

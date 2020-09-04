
import numpy as np
import pandas as pd
import locale
locale.setlocale(locale.LC_TIME,'de_DE')
import datetime
from datetime import timedelta

#%%
df_freiumfrage=pd.read_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/input/frei_umfrage.csv", encoding='utf-8', sep=";")
df_plusumfrage=pd.read_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/input/plus_umfrage.csv", encoding='utf-8', sep=";")
df_test=pd.read_csv("C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/input/1000er_Pay_Datenabzug.csv", encoding='utf-8', sep=";")

df_umfrage=pd.concat([df_plusumfrage,df_freiumfrage])

#%%
df_f1=df_test[['c_0001','c_0002','v_238_Rel','v_239_Exkl', 'session_id' ]]
df_f2=df_test[['c_0003','c_0004','v_216_Rel','v_217_Exkl', 'session_id' ]]
df_f3=df_test[['c_0005','c_0006','v_218_Rel','v_219_Exkl', 'session_id' ]]
df_f4=df_test[['c_0007','c_0008','v_220_Rel','v_221_Exkl', 'session_id' ]]
df_f5=df_test[['c_0009','c_0010','v_222_Rel','v_223_Exkl', 'session_id' ]]
df_f6=df_test[['c_0011','c_0012','v_224_Rel','v_225_Exkl', 'session_id' ]]
df_f7=df_test[['c_0013','c_0014','v_226_Rel','v_227_Exkl' , 'session_id']]
df_f8=df_test[['c_0015','c_0016','v_228_Rel','v_229_Exkl' , 'session_id']]
df_f9=df_test[['c_0017','c_0018','v_230_Rel','v_231_Exkl' , 'session_id']]
df_f10=df_test[['c_0019','c_0020','v_232_Rel','v_233_Exkl' ,'session_id']]

#%%
new_columns = df_f1.columns.values
new_columns[0] = 'filename'
new_columns[1] = 'filenr'
new_columns[2] = 'rel'
new_columns[3] = 'exkl'
new_columns[4] = 'session_id'

#%%
df_f1.columns = new_columns
df_f2.columns = new_columns
df_f3.columns = new_columns
df_f4.columns = new_columns
df_f5.columns = new_columns
df_f6.columns = new_columns
df_f7.columns = new_columns
df_f8.columns = new_columns
df_f9.columns = new_columns
df_f10.columns= new_columns

#%%
df_all=pd.concat([df_f1, df_f2, df_f3, df_f4, df_f5, df_f6, df_f7, df_f8, df_f9, df_f10])

df_teilnehmer=df_test[['session_id', 'v_6','v_294_Abo_H','v_295_Abo_P','v_296_Kein',"v_2","v_3", 'duration', 'datetime', 'date_of_last_access', 'v_247']]\
.rename(columns={"v_6": "Nutzungsfrequenz",
                "v_294_Abo_H": "Printabo",
                "v_295_Abo_P": "Plusabo",
                "v_296_Kein": "KeinAbo",
                "v_2": "Geschlecht",
                "v_3": "Alter"}, errors="raise")
#%%
df_um=df_all.join(df_umfrage.set_index('filename'), on='filename').join(df_teilnehmer.set_index('session_id'), on='session_id')

df_um['count_rel']=df_um[['session_id', 'Title', 'rel']].groupby(['session_id', 'rel'])['Title'].transform('count')
df_um['count_exkl']=df_um[['session_id', 'Title','exkl']].groupby(['session_id', 'exkl'])['Title'].transform('count')
df_u=df_um[~((df_um['count_rel']>=9) & (df_um['count_exkl']>=9))]

#df_u=df_um[df_um['duration']>0]
#%%
filename = "C:/Users/kroesent/OneDrive - SPIEGEL-Gruppe/Projekte/PM/DSProjects/index_insights/data/processed/umfrage1000teaser.csv"

df_u.to_csv(filename, index=False, sep=';')
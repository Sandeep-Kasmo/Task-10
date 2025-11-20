from extract import *

def customer_transform(dataframe):
    dataframe.drop(columns=['_id'],inplace=True)
    dataframe=dataframe.drop_duplicates()
    dataframe['registration_date']=pd.to_datetime(dataframe['registration_date'],format='mixed',errors='coerce')
    dataframe['email_flag']=dataframe['email'].astype(str).str.match(r'^[a-zA-Z0-9_.]+@[a-z]@\.[a-z]{2,}$',na=False)
    dataframe['email']=dataframe.apply(lambda row:f"{row['name'].lower().strip().replace(' ','_')}@email.com" if pd.isnull(row['email']) or row['email_flag']==False else row['email'], axis=1)
    dataframe.drop(columns=['email_flag'],inplace=True)
    dataframe['phone']=dataframe['phone'].astype(str).str.replace(r'()a-z','',regex=True)
    dataframe['phone_flag']=dataframe['phone'].astype(str).str.match(r'^\d{3}-\d{3}-\d{4}$',na=False)
    dataframe['phone']=dataframe.apply(lambda row:'000-000-0000' if pd.isnull(row['phone']) or row['phone_flag']==False else row['phone'],axis=1)
    dataframe.drop(columns=['phone_flag'],inplace=True)
    dataframe['name']=dataframe['name'].astype(str).str.title()
    return dataframe

def orders_transform(dataframe):
    dataframe.drop(columns=['_id'],inplace=True)
    dataframe=dataframe.drop_duplicates()
    dataframe['order_date']=pd.to_datetime(dataframe['order_date'],format='mixed',errors='coerce')
    dataframe=dataframe.dropna()
    dataframe['order_amount']=pd.to_numeric(dataframe['order_amount'],errors='coerce')
    return dataframe


def merge_data(dataframe1,dataframe2,column,join):
    dataframe=dataframe1.merge(dataframe2,on=f'{column}',how=f'{join}')
    return dataframe

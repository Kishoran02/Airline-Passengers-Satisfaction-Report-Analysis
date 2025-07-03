import pandas as pd
import sqlite3
import logging
import time
import warnings
warnings.filterwarnings('ignore')
from ingestion_data import ingest_db

logging.basicConfig(
    filename = "logs/get_airline_df.log",
    level = logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)
def get_data(conn):
    airlines_df = pd.read_sql_query('select * from train',conn)
    return airlines_df

def data_cleaning(df):
    df.drop(columns=['Unnamed: 0'],axis=1,inplace=True)
    df.columns = df.columns.str.lower()
    df = df.loc[~df['arrival delay in minutes'].isnull()]
    #feature engineering
    df['is_frequently_flyer'] = (df['customer type'] == 'Loyal Customer' ).astype(int)
    df['is_business_traveler'] = (df['type of travel'] == 'Business travel').astype(int)
    df['age_group'] = pd.cut(df['age'], bins=[0, 20, 40, 60, 100], labels=['Teen', 'Adult', 'Middle Age', 'Senior'])
    df['delay_total'] = df['departure delay in minutes'] + df['arrival delay in minutes']
    df['is_delayed'] = (df['delay_total'] > 15).astype(int)
    inflight_cols = ['inflight wifi service', 'inflight entertainment', 'inflight service', 'on-board service', 'leg room service']
    df['inflight_score_avg'] = df[inflight_cols].mean(axis=1)
    ground_cols = ['ease of online booking', 'baggage handling', 'checkin service', 'departure/arrival time convenient']
    df['ground_score_avg'] = df[ground_cols].mean(axis=1)
    df['is_long_haul'] = (df['flight distance'] > 2000).astype(int)
    df['business_long_haul'] = df['is_business_traveler'] & df['is_long_haul']
    df['vip_delay_penalty'] = df['is_frequently_flyer'] * df['delay_total']
    df['comfort_gap'] = df['leg room service'] - df['on-board service']
    
    return df



if __name__ == '__main__':
    conn = sqlite3.connect('airlines.db')
    logging.info('Creating data Table......')
    summary_df = get_data(conn)
    logging.info(summary_df.head())
    logging.info('Cleaning Data........')
    clean_df = data_cleaning(summary_df)
    logging.info(clean_df.head())
    logging.info('Ingestion data......')
    ingest_db(clean_df,'airlines_df',conn)
    logging.info('Completed')


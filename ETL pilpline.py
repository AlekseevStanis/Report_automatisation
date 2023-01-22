# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# main DB
connection = {'host' : ,
                  'database' : '',
                  'user' : '',
                  'password' : '',
             }

# analitics DB
connection_t = {'host': '',
                      'database':'',
                      'user':'', 
                      'password':''
                     }

#Default parameters
default_args = {
    'owner': '',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 10),
}

# Dag lanch period
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def ass_dag_etl_1657742(): # dag name should be unique

    @task()
    def extract_actions():
        q = """
        SELECT 
            toDate(time) as event_date,
            user_id,
            sum(action = 'like') as likes,
            sum(action = 'view') as views
        FROM {db}.feed_actions 
        WHERE toDate(time) = yesterday()
        GROUP BY event_date, user_id
        """
        df_actions=ph.read_clickhouse(q, connection=connection)
        return df_actions
    @task()
    def extract_messeges():
        q = """
        SELECT *
        FROM 
            (SELECT toDate(time) as event_date,
                reciever_id  as user_id,
                count(user_id) as messages_received,
                count(distinct(user_id)) as users_received
                FROM {db}.message_actions
                WHERE toDate(time)= yesterday()
                GROUP BY user_id, event_date) t1
            FULL OUTER  JOIN
            (SELECT toDate(time) as event_date,
                user_id,
                count(user_id) as messages_sent,
                count(distinct(user_id)) as users_sent
                FROM {db}.message_actions
                WHERE toDate(time)= yesterday()
                GROUP BY  user_id, event_date) t2
            USING user_id, event_date
        

        """
        df_messeges=ph.read_clickhouse(q, connection=connection)
        return df_messeges
    @task()
    def extract_desc():
        q = """
        SELECT 
        user_id,
        gender, 
        age,
        os
        FROM {db}.feed_actions
        GROUP BY user_id, gender, age, os
        UNION ALL
        SELECT  
        user_id,
        gender, 
        age,
        os
        FROM {db}.message_actions
        GROUP BY user_id, gender, age, os    
        """
        dft=ph.read_clickhouse(q, connection=connection)
        return dft
        
    @task
    def transfrom_summary(df_messeges, df_actions):
        df_summary=df_messeges.merge(df_actions,on=['user_id','event_date'], how='outer', sort=True).fillna(0)
        return df_summary

    @task
    def transfrom_gender(df_summary, dft):
        df_gender=df_summary.merge(dft[['user_id','gender']],on='user_id', how='left').drop_duplicates()
        df_gender.insert(0,'dimention', 'gender')
        df_gender.rename(columns={'gender':'dimention_value'}, inplace=True)
        df_gender=df_gender.groupby(['event_date', 'dimention','dimention_value']).sum().reset_index()
        df_gender.drop(columns=['user_id'], inplace=True)
        return df_gender

    @task
    def transfrom_os(df_summary, dft):
        df_os=df_summary.merge(dft[['user_id','os']],on='user_id', how='left').drop_duplicates()
        df_os.insert(0,'dimention', 'os')
        df_os.rename(columns={'os':'dimention_value'}, inplace=True)
        df_os=df_os.groupby(['event_date', 'dimention','dimention_value']).sum().reset_index()
        df_os.drop(columns=['user_id'], inplace=True)
        return df_os
    
    @task
    def transfrom_age(df_summary, dft):
        df_age=df_summary.merge(dft[['age', 'user_id']],on='user_id', how='left').drop_duplicates()
        df_age.insert(0,'dimention', 'age')
        df_age.rename(columns={'age':'dimention_value'}, inplace=True)
        df_age=df_age.groupby(['event_date', 'dimention','dimention_value']).sum().reset_index()
        df_age.drop(columns=['user_id'], inplace=True)
        return df_age
    @task
    def merge_in_all(df_gender, df_os, df_age):
        frames = [df_gender, df_os, df_age]

        united = pd.concat(frames).reset_index(drop=True)
        united=united.astype({'event_date' : 'datetime64',
                                            'dimention' : 'str',
                                            'dimention_value' : 'str',
                                            'views' : 'int64',
                                            'likes' : 'int64',
                                            'messages_sent' : 'int64',
                                            'users_sent' : 'int64',
                                            'messages_received' : 'int64',
                                            'users_received' : 'int64'})
        return united


    @task
    def load(united):
        create = '''CREATE TABLE IF NOT EXISTS table_name
        (event_date Date,
         dimention String,
         dimention_value String,
         views Int64,
         likes Int64,
         messages_sent Int64,
         users_sent Int64,
         messages_received Int64,
         users_received Int64
         ) ENGINE = MergeTree()
         ORDER BY event_date
         '''
        ph.execute(query=create, connection=connection_t)
        ph.to_clickhouse(df=united, table='s_alekseev_14', connection=connection_t, index=False)
#####

    df_actions = extract_actions()
    df_messeges = extract_messeges()
    dft=extract_desc()
    df_summary=transfrom_summary(df_messeges, df_actions)
    df_gender = transfrom_gender(df_summary, dft)
    df_os = transfrom_os(df_summary, dft)
    df_age = transfrom_age(df_summary, dft)
    united=merge_in_all(df_gender, df_os, df_age)
    load(united)

ass_dag_etl_1657742 = ass_dag_etl_1657742()

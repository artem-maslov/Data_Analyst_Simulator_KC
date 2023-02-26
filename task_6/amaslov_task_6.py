import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from io import StringIO
import requests 
from airflow import DAG
from airflow.operators.python import PythonOperator # Так как мы пишет таски в питоне
from airflow.operators.python import get_current_context 



# подлючение к кликхаусу
connection = {
                'host': 'https://clickhouse.lab.karpov.courses',
                'password': 'dpo_python_2020',
                'user': 'student',
                'database': 'simulator_20230120'
              }



# данные для подлючения к базе test
connection_to_test = {
                        'host': 'https://clickhouse.lab.karpov.courses',
                        'database': 'test',
                        'user': 'student-rw',
                        'password': '656e2b0c9c'
                      }



# стандартные параметры, которые прокидываются в таски
default_args = {
                'owner': 'amaslov',
                'depends_on_past': False,
                'retries': 2,                        # количество попыток выполнить DAG
                'retry_delay': timedelta(minutes=5), # промежуток между перезапусками
                'start_date': datetime(2023, 2, 2)
                }



# интервал запуска DAG
schedule_interval = '0 23 * * *' # каждый день в 23 часа



# главный dag
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def amaslov_task_6_dag():
    
    
    
    # вытаскиваю нужные данные из фида
    @task
    def extract_feed():
        q = '''
                SELECT 
                    user_id,
                    toDate(time) AS event_date,
                    COUNT(action = 'like') AS likes,
                    COUNT(action = 'view') AS views,
                    MAX(gender) AS gender,
                    MAX(age) AS age,
                    MAX(os) As os
                FROM simulator_20230120.feed_actions
                WHERE toDate(time) = yesterday()
                GROUP BY user_id,  toDate(time)
            '''
        df_feed = ph.read_clickhouse(q, connection=connection)
        
    return df_feed 



    # вытаскиваю нужные данные из мессенджера
    @task
    def extract_messenger():
        q = '''
                   WITH 
                   t1 AS
                        (SELECT 
                            user_id,
                            toDate(time) AS event_date,
                            COUNT(reciever_id) AS messages_sent,
                            COUNT(DISTINCT reciever_id) AS users_sent,
                            MAX(gender) AS gender,
                            MAX(age) AS age, 
                            MAX(os) AS os
                        FROM simulator_20230120.message_actions
                        WHERE toDate(time) = yesterday()
                        GROUP BY user_id, toDate(time)),

                    t2 AS        
                        (SELECT 
                            reciever_id,
                            toDate(time) AS event_date,
                            COUNT(user_id) AS messages_received,
                            COUNT(DISTINCT user_id) AS users_received,
                            MAX(gender) AS gender,
                            MAX(age) AS age, 
                            MAX(os) AS os
                        FROM simulator_20230120.message_actions
                        WHERE toDate(time) = yesterday()
                        GROUP BY reciever_id, toDate(time))

                SELECT 
                    CASE
                        WHEN t1.user_id = 0 THEN t2.reciever_id
                        ELSE user_id
                    END AS user_id,
                    CASE
                        WHEN t1.user_id = 0 THEN t2.event_date
                        ELSE t1.event_date
                    END AS event_date,
                    messages_sent,
                    users_sent,
                    messages_received,
                    users_received,
                    CASE
                        WHEN t1.user_id = 0 THEN t2.age
                        ELSE t1.age
                    END AS age,
                    CASE
                        WHEN t1.user_id = 0 THEN t2.gender
                        ELSE t1.gender
                    END AS gender,
                    CASE
                        WHEN t1.user_id = 0 THEN t2.os
                        ELSE t1.os
                    END AS os
                FROM t1 
                    FULL OUTER JOIN t2
                    ON t1.user_id = t2.reciever_id 
           '''
        df_messanger = ph.read_clickhouse(q, connection=connection)
        
    return df_messanger 



    # джойним две таблицы 
    @task
    def join_feed_messanger(df_feed, df_messanger):
        
        # делаю внешний джойн таблиц без копий
        df = df_feed.merge(df_messanger, how='outer', on='user_id', copy=False)
        
        # создаю единые колонки 
        df['event_date'] = df['event_date_x'].fillna(df['event_date_y'])
        df['gender'] = df['gender_x'].fillna(df['gender_y'])
        df['gender'] = df['gender'].map({1: 'male', 0: 'female'}) # заменяю значения 0 и 1 в поле gender
        df['age'] = df['age_x'].fillna(df['age_y'])
        df['os'] = df['os_x'].fillna(df['os_y'])
        
        # удаляю ненужные колонки
        df.drop(['event_date_x', 'event_date_y', 'gender_x', 'gender_y', 'age_x', 'age_y', 'os_x', 'os_y'], axis=1, inplace=True)
           
        # все пропуски заменяю на 0
        df = df.fillna(0)
        
    return df



    # разрез по полу
    @task 
    def gender(df):
        
        df_gender = df.drop(['user_id', 'os', 'age'], axis=1).groupby(['event_date','gender']).sum().reset_index()
        df_gender.insert(loc = 1, column = 'dimension', value = 'gender')
        df_gender.rename(columns={'gender':'dimension_value'}, inplace=True)
        
    return df_gender


    # разрез по возрасту
    @task 
    def age(df):
        
        df_age = df.drop(['user_id', 'os', 'gender'], axis=1).groupby(['event_date','age']).sum().reset_index()
        df_age.insert(loc = 1, column = 'dimension', value = 'age')
        df_age.rename(columns = {'age':'dimension_value'}, inplace = True)
        
    return df_age


    # разрез по os
    @task 
    def os(df):
        
        df_os = df.drop(['user_id', 'gender', 'age'], axis=1).groupby(['event_date','os']).sum().reset_index()
        df_os.insert(loc = 1, column = 'dimension', value = 'os')
        df_os.rename(columns={'os':'dimension_value'}, inplace=True)
        
    return df_os



    # объединенияю три дф в финальный
    @task    
    def df_final(df_gender, df_age, df_os):
        
        df_final = pd.concat([df_gender, df_os, df_age])
        df_final = df_final.astype({
                 'dimension' : 'string',
                 'dimension_value' : 'string',
                 'event_date' : 'datetime64',
                 'views': 'int32',
                 'likes': 'int32',
                 'messages_sent': 'int32',
                 'users_sent': 'int32',
                 'messages_received' : 'int32',
                 'users_received' : 'int32'
                                   })
    return df_final


    # загруаю df_final в базу test
    @task
    def load(df_final):
        
        # создаю таблицу в базе test, если её нет
        q = '''
                CREATE TABLE IF NOT EXISTS test.amaslov_task_6
                (
                    event_date Date,
                    dimension String,
                    dimension_value String,
                    views Int64,
                    likes Int64,
                    messages_recieved Int64,
                    messages_sent Int64,
                    users_recieved Int64,
                    users_sent Int64
                )
                ENGINE = MergeTree()
                ORDER BY event_date
                '''
        
            ph.execute(q, connection = connection_to_test)
            ph.to_clickhouse(df = df_final, table='amaslov_task_6', connection = connection_to_test, index = False)
        
        
    # выполняю таски
    df_feed = extract_feed()
    df_messanger = extract_messenger()
    df = join_feed_messanger(df_feed, df_messanger)
    df_gender = gender(df)
    df_age = age(df)
    df_os = os(df)
    df_final = df_final(df_gender, df_age, df_os)
    load(df_final)

amaslov_task_6_dag = amaslov_task_6_dag()

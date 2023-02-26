import pandas as pd
import pandahouse as ph
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pylab # совмещаю 4 графика на одной картинке

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator # Так как мы пишет таски в питоне
from datetime import datetime, timedelta

import telegram
import io # сохраняет в буфер данные и из буфера отправляет данные




# получение доступа к новому боту
my_token = '6069175291:AAHvb59xf5Plnm5H41TXKdFq-imtJqU7gTc' # токен получается при создании бота
bot = telegram.Bot(token=my_token) # получаю доступ


# чтобы получить chat_id, использую ссылкой https://api.telegram.org/bot6069175291:AAHvb59xf5Plnm5H41TXKdFq-imtJqU7gTc/getUpdates
chat_id = -677113209 # чат группы (минус всегда ставится перед групповыми каналами)
#chat_id = 5128718260 # личный чат


# подключение к бд
connection = {
                'host': 'https://clickhouse.lab.karpov.courses',
                'password': 'dpo_python_2020',
                'user': 'student',
                'database': 'simulator_20230120'
              }


# стандартные параметры, которые прокидываются в таски
default_args = {
                'owner': 'amaslov',
                'depends_on_past': False,            # зависимость от прошлых запусков
                'retries': 1,                        # количество попыток выполнить DAG
                'retry_delay': timedelta(minutes=1), # промежуток между перезапусками
                'start_date': datetime(2023, 2, 8)
                }


# интервал запуска DAG
schedule_interval = '0 11 * * *' # каждый день в 11 часа (синтаксис крона: минуты, часы, дни, месяцы, дни недели)




# dag
@dag(default_args=default_args, schedule_interval=schedule_interval, tags=['ar-maslov'], catchup=False)
def amaslov_task_7_1_dag():
    
    
    # вытаскиваю DAU, views, likes, CTR в df_message за вчерашний день
    @task()
    def extract_yesterday():
        query_yesterday = '''
                                SELECT 
                                    toDate(time) AS date,
                                    COUNT(DISTINCT user_id) AS DAU,
                                    COUNT(user_id) FILTER(WHERE action = 'view') AS views,
                                    COUNT(user_id) FILTER(WHERE action = 'like') AS likes,
                                    ROUND(likes/views, 2) AS CTR
                                FROM simulator_20230120.feed_actions
                                WHERE toDate(time) = yesterday()
                                GROUP BY toDate(time)
                            '''
        
        df_message = ph.read_clickhouse(query = query_yesterday, connection=connection)
        return df_message
    
    
    # вытаскиваю DAU, views, likes, CTR в df_plot за 7 последних дней
    @task()
    def extract_7_days():
        query_7_days = '''
                            SELECT
                                toDate(time) AS date,
                                COUNT(DISTINCT user_id) AS DAU,
                                COUNT(user_id) FILTER(WHERE action = 'view') AS views,
                                COUNT(user_id) FILTER(WHERE action = 'like') AS likes,
                                ROUND(likes/views, 4) AS CTR 
                            FROM simulator_20230120.feed_actions
                            WHERE toDate(time) <= yesterday() AND toDate(time) >= today() - 7
                            GROUP BY toDate(time) 
                            ORDER BY toDate(time)
                        '''
        
        df_plot = ph.read_clickhouse(query = query_7_days, connection=connection)
        return df_plot
    
    
    # создаю сообщение отчет для тг 
    @task()
    def create_message(df_message, chat_id):
        DAU = df_message['DAU'].loc[0]
        views = df_message['views'].loc[0]
        likes = df_message['likes'].loc[0]
        CTR = df_message['CTR'].loc[0]
        date = df_message['date'].loc[0].strftime('%Y-%m-%d') # .strftime('%Y-%m-%d') - обрезает до дня
        
        message = f'Привет! Это отчет за вчерашний день {date}: \
                    \nDAU = {DAU} \
                    \nviews = {views} \
                    \nlikes = {likes} \
                    \nCTR = {CTR}'
        
        bot.sendMessage(chat_id=chat_id, text=message)
    
    
    # создаю графики для тг
    @task()
    def create_plot(df_plot, chat_id):
        
        #задаю необходимые размеры графика  
        sns.set(rc={'figure.figsize':(22, 11)})

        # Две строки, два столбца. Текущая ячейка - 1
        pylab.subplot (2, 2, 1)
        sns.lineplot(x='date', y='DAU', data = df_plot)
        plt.title('DAU_за_7_дней')

        # Две строки, два столбца. Текущая ячейка - 3
        pylab.subplot (2, 2, 3)
        sns.lineplot(x='date', y='views', data = df_plot)
        plt.title('views_за_7_дней')

        # Две строки, два столбца. Текущая ячейка - 2
        pylab.subplot (2, 2, 2)
        sns.lineplot(x='date', y='likes', data = df_plot)
        plt.title('likes_за_7_дней')

        # Две строки, два столбца. Текущая ячейка - 4
        pylab.subplot (2, 2, 4)
        sns.lineplot(x='date', y='CTR', data = df_plot)
        plt.title('CTR_за_7_дней')
        
        # отправляю график
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0) 
        plot_object.name = 'метрики_за_7_дней.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        
        
    # выполняю таски
    df_message = extract_yesterday()
    df_plot = extract_7_days()
    create_message(df_message, chat_id)
    create_plot(df_plot, chat_id)
    
    
    
amaslov_task_7_1_dag = amaslov_task_7_1_dag()
    
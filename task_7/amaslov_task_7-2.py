import pandas as pd
import pandahouse as ph
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

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
chat_id = -677113209  # чат группы (минус всегда ставится перед групповыми каналами)
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
                'start_date': datetime(2023, 2, 10)
                }



# интервал запуска DAG
schedule_interval = '0 11 * * *' # каждый день в 11 часа (синтаксис крона: минуты, часы, дни, месяцы, дни недели)



# dag
@dag(default_args=default_args, schedule_interval=schedule_interval, tags=['ar-maslov'], catchup=False)
def amaslov_task_7_2_dag():
    
    # 1. Cообщения
    # 1.1 DAU
    # подключаюсь к бд и вывожу дф со всеми DAU на ленту и мессенджер за вчера
    @task()
    def exctarct_DAU_yesterday():
        query_DAU_yesterday = '''
                                    WITH 
                                    t1 AS   (
                                            SELECT toDate(time) AS date, COUNT(DISTINCT user_id) AS DAU_feed
                                            FROM simulator_20230120.feed_actions
                                            WHERE toDate(time) = yesterday()
                                            GROUP BY toDate(time)
                                            ),

                                    t2 AS   (
                                            SELECT toDate(time) AS date, COUNT(DISTINCT user_id) AS DAU_messenger
                                            FROM simulator_20230120.message_actions
                                            WHERE toDate(time) = yesterday()
                                            GROUP BY toDate(time)
                                            ),

                                    t3 AS   (
                                            SELECT date, COUNT(DISTINCT user_id) AS DAU_app
                                            FROM
                                                (
                                                SELECT user_id, toDate(time) AS date
                                                FROM simulator_20230120.feed_actions
                                                WHERE toDate(time) = yesterday()

                                                UNION ALL

                                                SELECT user_id, toDate(time) AS date
                                                FROM simulator_20230120.message_actions
                                                WHERE toDate(time) = yesterday()
                                                )
                                            GROUP BY date
                                            )

                                    SELECT t3.date AS date, DAU_app, DAU_messenger, DAU_feed
                                    FROM t3
                                        JOIN t2
                                        ON t3.date = t2.date
                                        JOIN t1
                                        ON t3.date = t1.date
                                    '''
        DAU_yesterday = ph.read_clickhouse(query = query_DAU_yesterday, connection=connection)
        return DAU_yesterday
    
    
    
    # 1.2 actions
    # подключаюсь к бд и вывожу дф со всеми лайками, просмотрами и сообщениями за вчера
    @task()
    def extract_actions_yesterday():
        query_actions = '''
                                    SELECT 
                                        date,
                                        COUNT(user_id) FILTER(WHERE action = 'like') AS likes,
                                        COUNT(user_id) FILTER(WHERE action = 'view') AS views,
                                        COUNT(user_id) FILTER(WHERE action = 'message') AS messages
                                    FROM 
                                        (
                                        SELECT toDate(time) AS date, user_id, action
                                        FROM simulator_20230120.feed_actions
                                        WHERE toDate(time) = yesterday()

                                        UNION ALL

                                        SELECT toDate(time) AS date, user_id, 'message' AS action
                                        FROM simulator_20230120.message_actions
                                        WHERE toDate(time) = yesterday()
                                        )
                                    GROUP BY date
                                '''
        actions = ph.read_clickhouse(query = query_actions, connection=connection)  
        return actions
    
    
    
    
    # 2. Графики
    # 2.1 DAU_plot
    # подключаюсь к бд и вывожу дф со всеми DAU на ленту и мессенджер
    @task()
    def extract_query_DAU():
        query_DAU = '''
                        WITH 
                        t1 AS   (
                                SELECT toDate(time) AS date, COUNT(DISTINCT user_id) AS DAU_feed
                                FROM simulator_20230120.feed_actions
                                WHERE toDate(time) >= today() - 7
                                GROUP BY toDate(time)
                                ),

                        t2 AS   (
                                SELECT toDate(time) AS date, COUNT(DISTINCT user_id) AS DAU_messenger
                                FROM simulator_20230120.message_actions
                                WHERE toDate(time) >= today() - 7
                                GROUP BY toDate(time)
                                ),

                        t3 AS   (
                                SELECT date, COUNT(DISTINCT user_id) AS DAU_app
                                FROM
                                    (
                                    SELECT user_id, toDate(time) AS date
                                    FROM simulator_20230120.feed_actions
                                    WHERE toDate(time) >= today() - 7

                                    UNION ALL

                                    SELECT user_id, toDate(time) AS date
                                    FROM simulator_20230120.message_actions
                                    WHERE toDate(time) >= today() - 7
                                    )
                                GROUP BY date
                                )

                        SELECT t3.date AS date, DAU_app, DAU_messenger, DAU_feed
                        FROM t3
                            JOIN t2
                            ON t3.date = t2.date
                            JOIN t1
                            ON t3.date = t1.date
                    '''
        DAU = ph.read_clickhouse(query = query_DAU, connection=connection)
        return DAU
    
    
    # 2.2 actions_plot
    # подключаюсь к бд и вывожу дф со лайками, просмотрами и сообщениями на каждого пользователю за 7 дней
    @task()
    def extract_actions_plot():
        query_actions_per_user = '''
                                    SELECT 
                                        date,
                                        COUNT(user_id) FILTER(WHERE action = 'like') / COUNT(DISTINCT user_id) FILTER(WHERE action = 'like') AS likes_per_user,
                                        COUNT(user_id) FILTER(WHERE action = 'view') / COUNT(DISTINCT user_id) FILTER(WHERE action = 'view') AS views_per_user,
                                        COUNT(user_id) FILTER(WHERE action = 'message') / COUNT(DISTINCT user_id) FILTER(WHERE action = 'message') AS messages_per_user
                                    FROM 
                                        (
                                        SELECT toDate(time) AS date, user_id, action
                                        FROM simulator_20230120.feed_actions
                                        WHERE toDate(time) >= today() - 7

                                        UNION ALL

                                        SELECT toDate(time) AS date, user_id, 'message' AS action
                                        FROM simulator_20230120.message_actions
                                        WHERE toDate(time) >= today() - 7
                                        )
                                    GROUP BY date
                                '''
        actions_per_user = ph.read_clickhouse(query = query_actions_per_user, connection=connection)   
        return actions_per_user
    
    
    
    # 2.3 retention_feed_plot
    # подключаюсь к бд и вывожу дф с retention для ленты
    @task()
    def extract_retention_feed():
        query_retention_feed = '''
                                    WITH t1 AS 
                                        (SELECT user_id, MIN(toDate(time)) AS start_date
                                        FROM simulator_20230120.feed_actions
                                        GROUP BY user_id
                                        HAVING start_date >= today() - 21),

                                        t2 AS
                                        (SELECT DISTINCT user_id, toDate(time) AS date 
                                        FROM simulator_20230120.feed_actions)

                                    SELECT COUNT(user_id) AS active_users, toString(date) AS date, toString(start_date) AS start_date
                                    FROM t1
                                        JOIN t2
                                        USING user_id
                                    GROUP BY date, start_date
                               '''
        retention_feed = ph.read_clickhouse(query = query_retention_feed, connection=connection)
        cohort_table_feed =  retention_feed.pivot(index = 'start_date', columns = 'date', values = 'active_users')
        return cohort_table_feed
    
    
    
    # 2.4 retention_messenger_plot
    # подключаюсь к бд и вывожу дф с retention для мессенджера
    @task()
    def extract_retention_messenger():
        query_retention_messenger = '''
                                        WITH t1 AS 
                                            (SELECT user_id, MIN(toDate(time)) AS start_date
                                            FROM simulator_20230120.message_actions
                                            GROUP BY user_id
                                            HAVING start_date >= today() - 21),

                                            t2 AS
                                            (SELECT DISTINCT user_id, toDate(time) AS date 
                                            FROM simulator_20230120.message_actions)

                                        SELECT COUNT(user_id) AS active_users, toString(date) AS date, toString(start_date) AS start_date
                                        FROM t1
                                            JOIN t2
                                            USING user_id
                                        GROUP BY date, start_date
                                    '''
        retention_messenger = ph.read_clickhouse(query = query_retention_messenger, connection=connection)
        cohort_table_messenger =  retention_messenger.pivot(index = 'start_date', columns = 'date', values = 'active_users')
        return cohort_table_messenger
    
    
    
    # создаю сообщение отчет для тг 
    @task()
    def create_message(DAU_yesterday, actions, chat_id):
        
        date = DAU_yesterday['date'].iloc[0].strftime('%Y-%m-%d') # .strftime('%Y-%m-%d') - обрезает до дня
        
        DAU_app = DAU_yesterday['DAU_app'].iloc[0]
        DAU_messenger = DAU_yesterday['DAU_messenger'].iloc[0]
        DAU_feed = DAU_yesterday['DAU_feed'].iloc[0]
        
        likes = actions['likes'].iloc[0]
        views = actions['views'].iloc[0]
        messages = actions['messages'].iloc[0]
        
        message = f'Привет! Это отчет за вчерашний день {date}. \
                    \n\
                    \nПоказатели DAU:\
                    \nDAU_app = {DAU_app} \
                    \nDAU_messenger = {DAU_messenger} \
                    \nDAU_feed = {DAU_feed} \
                    \n\
                    \nАктивность пользователей:\
                    \nlikes = {likes} \
                    \nviews = {views} \
                    \nmessages = {messages}'
        
        bot.sendMessage(chat_id=chat_id, text=message)
        
        
        
    # создаю графики для тг 
    @task()
    def create_plots(DAU, actions_per_user, cohort_table_feed, cohort_table_messenger, chat_id):
        
        sns.set(rc={'figure.figsize':(11, 7)}) #размеры, ширина и высота
        
        # рисую график для трех видов DAU за неделю
        sns.lineplot(x = 'date', y = 'value', hue='variable', data=pd.melt(DAU, ['date']), markers = True) #pd.melt - помогает разместить три прямые на графике
        plt.title('DAU_за_7_дней')
        # отправляю график DAU
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0) 
        plot_object.name = 'график для трех видов DAU за неделю.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        # рисую график для трех видов action за неделю
        sns.lineplot(x = 'date', y = 'value', hue='variable', data=pd.melt(actions_per_user, ['date']), markers = True) #pd.melt - помогает разместить три прямые на графике
        plt.title('actions_per_user_за_7_дней')
        # отправляю график action
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0) 
        plot_object.name = 'график для трех видов action за неделю.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        # рисую heatmap по retention ленты за 21 день
        sns.heatmap(data = cohort_table_feed, cmap='Blues')
        plt.title('retention ленты за 21 день')
        # отправляю heatmap
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0) 
        plot_object.name = 'heatmap по retention ленты за 21 день.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        # рисую heatmap по retention мессендежра за 21 день
        sns.heatmap(data = cohort_table_messenger, cmap='Blues')
        plt.title('retention мессендежра за 21 день')
        # отправляю heatmap
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0) 
        plot_object.name = 'heatmap по retention мессендежра за 21 день.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        
        
    # выполняю таски
    # сообщение
    DAU_yesterday = exctarct_DAU_yesterday()
    actions = extract_actions_yesterday()
    # графики
    DAU = extract_query_DAU()
    actions_per_user = extract_actions_plot()
    cohort_table_feed = extract_retention_feed()
    cohort_table_messenger = extract_retention_messenger()
    # отчеты
    create_message(DAU_yesterday, actions, chat_id)
    create_plots(DAU, actions_per_user, cohort_table_feed, cohort_table_messenger, chat_id)
     
    
    
amaslov_task_7_2_dag = amaslov_task_7_2_dag()    



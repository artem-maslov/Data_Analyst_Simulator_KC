import pandas as pd
import pandahouse as ph
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
# import pylab # совмещаю 4 графика на одной картинке

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator # Так как мы пишет таски в питоне
from datetime import datetime, timedelta

import telegram
import io # сохраняет в буфер данные и из буфера отправляет данные




# подключение к бд
connection = {
                'host': 'https://clickhouse.lab.karpov.courses',
                'password': 'dpo_python_2020',
                'user': 'student',
                'database': 'simulator_20230120'
              }

# получение доступа к боту
my_token = '6069175291:AAHvb59xf5Plnm5H41TXKdFq-imtJqU7gTc' 
bot = telegram.Bot(token=my_token) # получаем доступ
#chat_id = 5128718260
chat_id = -520311152




# стандартные параметры, которые прокидываются в таски
default_args = {
                'owner': 'amaslov',
                'depends_on_past': False,            # зависимость от прошлых запусков
                'retries': 1,                        # количество попыток выполнить DAG
                'retry_delay': timedelta(minutes=1), # промежуток между перезапусками
                'start_date': datetime(2023, 2, 10)
                }

# интервал запуска DAG
schedule_interval = '*/15 * * * *' # кажде 15 минут (синтаксис крона: минуты, часы, дни, месяцы, дни недели)




# dag
@dag(default_args=default_args, schedule_interval=schedule_interval, tags=['ar-maslov'], catchup=False)
def amaslov_task_8_dag():
    
    # вытаскивая данные с бд:
    @task
    def extract_data():
        data_query ='''
                    SELECT
                      toStartOfFifteenMinutes(time) AS ts,
                      toDate(time) AS date,
                      formatDateTime(ts, '%R') AS hm,
                      COUNT(DISTINCT user_id) FILTER(WHERE action ='view') AS users_feed,
                      COUNT(user_id) FILTER(WHERE action ='view') AS views,
                      COUNT(user_id) FILTER(WHERE action ='like') AS likes,
                      likes / views AS CTR,
                      COUNT(DISTINCT user_id) FILTER(WHERE action ='message') AS users_messenger,
                      COUNT(user_id) FILTER(WHERE action ='message') AS messages
                    FROM
                        (SELECT 
                            user_id,
                            time,
                            action,
                            age,
                            os,
                            country,
                            city,
                            gender
                        FROM simulator_20230120.feed_actions
                        UNION ALL
                        SELECT
                            user_id,
                            time,
                            'message' AS action,
                            age,
                            os,
                            country,
                            city,
                            gender
                        FROM simulator_20230120.message_actions
                        )
                    WHERE time >= today() -1 and time < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts
                '''
        data = ph.read_clickhouse(query = data_query, connection=connection)
        return data
    
    def check_anomaly(df, metric, a=3, n=5): # n - сколько у нас 15 минуток
        # фнкция предлагает алгоритм поиска аномалий в данных (межквартильный размах)
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)   # 25 квантиль
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)   # 75 квантиль
        df['iqr'] = df['q75'] - df['q25']
        # считаю границы
        df['up'] = df['q75'] + a * df['iqr']
        df['low'] = df['q25'] - a * df['iqr']
        # сглаживаю границы
        df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean() # делаю окно посередине
        df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

        if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] >  df['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0

        return is_alert, df
    
    @task()
    def run_alerts(data, chat_id):
        metrics_list = ['users_feed', 'views', 'likes', 'CTR', 'users_messenger', 'messages']
        for metric in metrics_list:
            print(metric)
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df, metric)

            if is_alert == 1: # or True для отправки отчета (без аномилии)
                msg = '''Метрика *{metric}*: \nтекущее значение {current_val:.2f}\nотклонение от предидущего значения {last_val_diff:.2%}\n[Ссылка на дашборд](http://superset.lab.karpov.courses/r/3060)'''.format(metric = metric, current_val = df[metric].iloc[-1], last_val_diff = abs(1 - (df[metric].iloc[-1]/df[metric].iloc[-2])))

                # параметры картинки
                sns.set(rc={'figure.figsize': (16, 10)})
                plt.tight_layout()
                
                # распределение самой метрики
                ax = sns.lineplot(x=df['ts'], y=df[metric], label = 'metric')
                ax = sns.lineplot(x=df['ts'], y=df['up'], label = 'up')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label = 'low')

                # отображаю только каждую третью подпись по оси х
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                # задаю подписи для осей
                ax.set(xlabel = 'time')
                ax.set(ylabel = 'metric')

                # задаю заголовок графика
                ax.set_title(metric)

                # указываю, что нижная граница у = 0
                ax.set(ylim=(0, None))

                # формирую файловый объект
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()

                # отправляю алерт
                bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='Markdown')
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
   
    data = extract_data()
    run_alerts(data, chat_id)
    
amaslov_task_8_dag = amaslov_task_8_dag()
    
    
### task_4-1

В наших данных использования ленты новостей есть два типа юзеров: те, кто пришел через платный трафик source = 'ads', и те, кто пришел через органические каналы source = 'organic'.

Ваша задача — проанализировать и сравнить Retention этих двух групп пользователей. Решением этой задачи будет ответ на вопрос: отличается ли характер использования приложения у этих групп пользователей. 

### solution_4-1

Составив отдельные heat map для каждой группы пользователей по source, я определил, что характер использования приложения за последние 3 недели у этих групп отличается. На дашборде видно, что у группы source = 'ads' больше темных ячеек, что говорит о более высоком показателе retention. Можно сделать вывод, что пользователи, которые пришли по рекламе остаются в нашем приложении более длительный срок. Для увереннсоти я дополнительно для каждого источника рассчитал средний показатель retention по дня и за весь период, который явно показывает, что у source = 'ads' retention больше.

![S_retention_ads_4-1.png](https://github.com/artem-maslov/data_analyst_simulator_KC/blob/main/4_analysis_of_product_metrics/S_retention_ads_4-1.png?raw=true)

![S_retention_organic_4-1.png](https://github.com/artem-maslov/data_analyst_simulator_KC/blob/main/4_analysis_of_product_metrics/S_retention_organic_4-1.png?raw=true)

### task_4-2

Маркетологи запустили массивную рекламную кампанию, в результате в приложение пришло довольно много новых пользователей, вы можете видеть всплеск на графике активной аудитории.

![T_4-2.jpg](https://github.com/artem-maslov/data_analyst_simulator_KC/blob/main/4_analysis_of_product_metrics/T_4-2.jpg?raw=true)

### solution_4-2

Изучив график DAU видно, что 14 января был резкий скачок активных пользователей с 16,4к до 19,1к о чем свидетельствует запуск рекламной кампании.

![S_DAU_4-2.png](https://github.com/artem-maslov/data_analyst_simulator_KC/blob/main/4_analysis_of_product_metrics/S_DAU_4-2.png?raw=true)

Однако, на дашборте с retention source = 'ads' видно, что это самый низкий retention из всех. Уже на второй день из 3,69 новых пользователей зашло только 106, По цвету ячеек видно, что и далее retention пользователей с рекламной кампании только падает. Можно сделать вывод, что рекламная кампания оказалось не эффективной. 


![S_retention_ads_4-2.png](https://github.com/artem-maslov/data_analyst_simulator_KC/blob/main/4_analysis_of_product_metrics/S_retention_ads_4-2.png?raw=true)



### task_4-3

Мы наблюдаем внезапное падение активной аудитории! Нужно разобраться, какие пользователи не смогли зайти в приложение, что их объединяет? 

![T_4-3.jpg](https://github.com/artem-maslov/data_analyst_simulator_KC/blob/main/4_analysis_of_product_metrics/T_4-3.jpg?raw=true)

### solution_4-3

В процессе исследования, сначала я обратил внимание на DAY и даты, когда было падение. Я заметил, что 22 января число пользователей было нормальным 17.1к, однако 23 января показатель резко упал до 14.1к. 

В ходе эксперимента я сравнивал разные метрики в день падения и день до него. Обнаружил, что в день падения процентное соотношение пользователей из РФ меньше, чем до. Потом группировал по городам и смотрел, где аномалии. Оказалось, что 23 января не смогли зайти пользователи из Москвы, Санкт-Петербурга, Новосибирска и Екатеринбурга. Суммарно эти 4 города за день до аномалии дают 3,5к (23%) пользователей, что как раз и укладывается в разрыв. 

![S_search_anomalies_4-3.png](https://github.com/artem-maslov/data_analyst_simulator_KC/blob/main/4_analysis_of_product_metrics/S_search_anomalies_4-3.png?raw=true)


### task_4-4

В лекции я показывал график, который позволяет взглянуть на активную аудиторию с точки зрения новых, старых и ушедших пользователей. Попробуйте самостоятельно построить такой график:

![task_4-4.jpg](https://github.com/artem-maslov/data_analyst_simulator_KC/blob/main/4_analysis_of_product_metrics/T_4-4.jpg?raw=true)


### solution_4-4

![S_weekly_audience_4-4.png](https://github.com/artem-maslov/data_analyst_simulator_KC/blob/main/4_analysis_of_product_metrics/S_weekly_audience_4-4.png?raw=true)

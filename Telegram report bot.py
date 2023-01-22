# coding=utf-8
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import telegram
import pandahouse as ph




connection = {'host' : '',
                  'database' : '',
                  'user' : '',
                  'password' : '',
             }


my_token = '' # bot token
chat_id= #chat for reports ID
bot = telegram.Bot(token=my_token)

#Default parameters
default_args = {
    'owner': '',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}

# Иag lanch period
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def ass_dag_bot_report_telegramm1224():

    @task()
    def extract_metrics():
        q = """

                  SELECT t1.event_date as event_date,
                          t1.DAU as DAU,
                          t2.DAU_mes as DAU_mes,
                          t3.DAU_feed as DAU_feed
                   FROM
                     (SELECT event_date,
                             COUNT(DISTINCT(user_id)) as DAU
                      FROM
                        (SELECT toDate(time) as event_date,
                                user_id
                         FROM simulator_20221220.message_actions
                         GROUP BY event_date,
                                  user_id
                         UNION ALL SELECT toDate(time) as event_date,
                                          user_id
                         FROM simulator_20221220.feed_actions
                         GROUP BY event_date,
                                  user_id)
                      GROUP BY event_date) t1
                   INNER JOIN
                     (SELECT toDate(time) as event_date,
                             COUNT(DISTINCT(user_id)) as DAU_mes
                      FROM simulator_20221220.message_actions
                      GROUP BY event_date) t2 ON t1.event_date=t2.event_date
                   INNER JOIN
                     (SELECT toDate(time) as event_date,
                             COUNT(DISTINCT(user_id)) as DAU_feed
                      FROM simulator_20221220.feed_actions
                      GROUP BY event_date) t3 ON t3.event_date=t2.event_date
                    WHERE event_date BETWEEN yesterday()-6 and yesterday()
                    ORDER BY event_date
                """
        df_metrics=ph.read_clickhouse(q, connection=connection)
        df_metrics["event_date"]=df_metrics.event_date.dt.strftime('%d.%m')
        return df_metrics
    @task
    def extract_message():
        q = """
            select 
            toDate(time) as event_date, 
            count(user_id) as messages,
            messages/COUNT(DISTINCT(user_id)) AS msg_per_user
            from simulator_20221220.message_actions
            WHERE event_date between yesterday()-6 and yesterday()
            GROUP BY event_date
            ORDER BY event_date
            """
        df_messenger=ph.read_clickhouse(q, connection=connection)
        df_messenger["event_date"]=df_messenger.event_date.dt.strftime('%d.%m')
        return df_messenger
    @task
    def extract_feed():
        q = """
                SELECT 
                    toDate(time) as event_date,

                    sum(action = 'like') as likes,
                    sum(action = 'view') as views,
                    likes/views as CTR
                FROM simulator_20221220.feed_actions 
                WHERE toDate(time) between (yesterday()-6) and yesterday() 
                GROUP BY event_date
                order by event_date asc
                """
        df_feed=ph.read_clickhouse(q, connection=connection)
        df_feed["event_date"]=df_feed.event_date.dt.strftime('%d.%m')
        return df_feed
    @task
    def merge(df_metrics,df_messenger,df_feed):
        df_combined=df_metrics.join(df_messenger.set_index("event_date"), on="event_date")
        df_combined=df_combined.join(df_feed.set_index("event_date"), on="event_date")
        return df_combined

       
    @task
    def send_DAU(df_metrics):
        DAU= "Данные за вчера({}): \n DAU по приложению: {} \n DAU Мессенджер: {} \n DAU ленты: {}".format(df_metrics.event_date[6], df_metrics.DAU[6], df_metrics.DAU_mes[6], df_metrics.DAU_feed[6])
        sns.lineplot(data=df_metrics.set_index("event_date"))
        plt.title('DAU за неделю')
        dau_graph=io.BytesIO()
        plt.savefig(dau_graph)
        dau_graph.seek(0)
        dau_graph.name='dau_plot.png'
        plt.close()
        bot.sendMessage(chat_id=chat_id, text=DAU)
        bot.sendPhoto(chat_id=chat_id, photo=dau_graph)

    @task
    def send_views(df_combined):
        views= "Просмотры за вчера({}): {}".format(df_combined.event_date[6], df_combined.views[6])
        sns.lineplot(x=df_combined.event_date,y=df_combined.views)
        plt.title('Просмотры за неделю')
        views_graph=io.BytesIO()
        plt.savefig(views_graph)
        views_graph.seek(0)
        views_graph.name='views_plot.png'
        plt.close()
        bot.sendMessage(chat_id=chat_id, text=views)
        bot.sendPhoto(chat_id=chat_id, photo=views_graph)

    @task
    def send_likes(df_combined):
        likes= "Лайки за вчера({}): {}".format(df_combined.event_date[6],df_combined.likes[6])
        sns.lineplot(x=df_combined.event_date,y=df_combined.likes)
        plt.title('Лайки за неделю')
        likes_graph=io.BytesIO()
        plt.savefig(likes_graph)
        likes_graph.seek(0)
        likes_graph.name='likes_plot.png'
        plt.close()
        bot.sendMessage(chat_id=chat_id, text=likes)
        bot.sendPhoto(chat_id=chat_id, photo=likes_graph)
    
    @task
    def send_CTR(df_combined):
        CTR= "CTR за вчера({}): {:.3f}".format(df_combined.event_date[6], df_combined.CTR[6])
        sns.lineplot(x=df_combined.event_date,y=df_combined.CTR)
        plt.title('CTR за неделю')
        CTR_graph=io.BytesIO()
        plt.savefig(CTR_graph)
        CTR_graph.seek(0)
        CTR_graph.name='CTR_plot.png'
        plt.close()
        bot.sendMessage(chat_id=chat_id, text=CTR)
        bot.sendPhoto(chat_id=chat_id, photo=CTR_graph)
    @task
    def send_messages(df_combined):
        mess= "Сообщения за вчера({}): {}".format(df_combined.event_date[6], df_combined.messages[6])
        sns.lineplot(x=df_combined.event_date,y=df_combined.messages)
        plt.title('Сообщения за неделю')
        messages_graph=io.BytesIO()
        plt.savefig(messages_graph)
        messages_graph.seek(0)
        messages_graph.name='messages_graph.png'
        plt.close()
        bot.sendMessage(chat_id=chat_id, text=mess)
        bot.sendPhoto(chat_id=chat_id, photo=messages_graph)
    

#####

    df_metrics = extract_metrics()
    df_messenger=extract_message()
    df_feed=extract_feed()
    df_combined=merge(df_metrics,df_messenger,df_feed)
    send_DAU(df_metrics)
    send_views(df_combined)
    send_likes(df_combined)
    send_CTR(df_combined)
    send_messages(df_combined)

ass_dag_bot_report_telegramm1224 = ass_dag_bot_report_telegramm1224()

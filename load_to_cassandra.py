

import os
import time
import uuid
from datetime import datetime
from collections import defaultdict 

import mysql.connector
from mysql.connector import Error
import pandas as pd # Для обробки даних з MySQL

from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import BatchStatement
from cassandra.io.asyncio import AsyncioConnection 

# --- КОНФІГУРАЦІЯ ---
MYSQL_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_DATABASE', 'my_ad_data'),
    'user': os.getenv('DB_USER', 'me'),
    'password': os.getenv('DB_PASSWORD', 'artem228')
}

CASSANDRA_CONFIG = {
    'hosts': ['127.0.0.1'], 
    'port': 9042,
    'keyspace': 'ad_tracking'}

def connect_mysql():
    """Встановлює з'єднання з базою даних MySQL."""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        if conn.is_connected():
            print("Успішно підключено до MySQL.")
            return conn
    except Error as e:
        print(f"Помилка при підключенні до MySQL: {e}")
        return None

def connect_cassandra():
    """Встановлює з'єднання з Cassandra."""
    retries = 10
    while retries > 0:
        try:

            cluster = Cluster(
                CASSANDRA_CONFIG['hosts'],
                port=CASSANDRA_CONFIG['port'],
                connection_class=AsyncioConnection 
            )
            session = cluster.connect()
   
            session.execute(f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_CONFIG['keyspace']} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
            session.execute(f"USE {CASSANDRA_CONFIG['keyspace']}")

            session.execute("""
                CREATE TABLE IF NOT EXISTS campaign_performance_by_day (
                    campaign_id uuid,
                    day date,
                    campaign_name text,
                    advertiser_name text,
                    impressions counter,
                    clicks counter,
                    PRIMARY KEY ((campaign_id), day)
                ) WITH CLUSTERING ORDER BY (day ASC);
            """)
            session.execute("""
                CREATE TABLE IF NOT EXISTS user_engagement_history (
                    user_id uuid,
                    event_time timestamp,
                    impression_id uuid,
                    campaign_name text,
                    was_clicked boolean,
                    PRIMARY KEY ((user_id), event_time, impression_id)
                ) WITH CLUSTERING ORDER BY (event_time DESC, impression_id DESC);
            """)
            session.execute("""
                CREATE TABLE IF NOT EXISTS advertiser_spend_by_month (
                    year_month text,
                    advertiser_id uuid,
                    advertiser_name text,
                    total_spend decimal,
                    PRIMARY KEY ((year_month), advertiser_id)
                ) WITH CLUSTERING ORDER BY (advertiser_id ASC);
            """)
            session.execute("""
                CREATE TABLE IF NOT EXISTS advertiser_spend_by_region_and_month (
                    region text,
                    year_month text,
                    advertiser_id uuid,
                    advertiser_name text,
                    total_spend decimal,
                    PRIMARY KEY ((region), year_month, advertiser_id)
                ) WITH CLUSTERING ORDER BY (year_month ASC, advertiser_id ASC);
            """)
            session.execute("""
                CREATE TABLE IF NOT EXISTS user_clicks_by_month (
                    year_month text,
                    user_id uuid,
                    total_clicks counter,
                    PRIMARY KEY ((year_month), user_id)
                ) WITH CLUSTERING ORDER BY (user_id ASC);
            """)

            print("Успішно підключено до Cassandra.")
            return cluster, session
        except Exception as e:
            print(f"Не вдалося підключитися до Cassandra. Спроба через 5 секунд... Помилка: {e}")
            retries -= 1
            time.sleep(5)
    return None, None

def fetch_data_from_mysql(conn):
    """Витягує та об'єднує дані з MySQL."""
    print("Витяг даних з MySQL...")
    query = """
    SELECT
        imp.ImpressionID, imp.UserID, imp.Timestamp AS EventTime, imp.Device, imp.Location AS Region, imp.AdCost,
        camp.CampaignID, camp.CampaignName,
        adv.AdvertiserID, adv.AdvertiserName,
        c.ClickID, c.ClickTimestamp
    FROM
        Impressions imp
    JOIN Campaigns camp ON imp.CampaignID = camp.CampaignID
    JOIN Advertisers adv ON camp.AdvertiserID = adv.AdvertiserID
    LEFT JOIN Clicks c ON imp.ImpressionID = c.ImpressionID;
    """
    try:
        df = pd.read_sql(query, conn)
        print(f"Отримано {len(df)} записів з MySQL.")
 
        df['ImpressionID'] = df['ImpressionID'].apply(lambda x: uuid.UUID(bytes=x) if isinstance(x, bytes) else uuid.UUID(x))
        df['UserID'] = df['UserID'].apply(lambda x: uuid.UUID(bytes=x) if isinstance(x, bytes) else uuid.UUID(x))
        df['CampaignID'] = df['CampaignID'].apply(lambda x: uuid.UUID(bytes=x) if isinstance(x, bytes) else uuid.UUID(x))
        df['AdvertiserID'] = df['AdvertiserID'].apply(lambda x: uuid.UUID(bytes=x) if isinstance(x, bytes) else uuid.UUID(x))

        return df
    except Exception as e:
        print(f"Помилка при виконанні SQL-запиту або перетворенні UUID: {e}")
        return pd.DataFrame()

def load_data_to_cassandra(session, df):
    """Завантажує дані в різні таблиці Cassandra відповідно до запитів."""
    print("Підготовка запитів для Cassandra...")
    
    # Підготовлені запити для таблиць Cassandra
    update_campaign_performance = session.prepare(
        "UPDATE campaign_performance_by_day SET impressions = impressions + 1, clicks = clicks + ?, campaign_name = ?, advertiser_name = ? WHERE campaign_id = ? AND day = ?"
    )
    insert_user_history = session.prepare(
        "INSERT INTO user_engagement_history (user_id, event_time, impression_id, campaign_name, was_clicked) VALUES (?, ?, ?, ?, ?)"
    )
    # Таблиці, які не є лічильниками, оновлюються через INSERT,
    # оскільки їх ключі включають дані, які можуть бути оновлені.
    insert_advertiser_spend_month = session.prepare(
        "INSERT INTO advertiser_spend_by_month (year_month, advertiser_id, advertiser_name, total_spend) VALUES (?, ?, ?, ?)"
    )
    insert_advertiser_spend_region = session.prepare(
        "INSERT INTO advertiser_spend_by_region_and_month (region, year_month, advertiser_id, advertiser_name, total_spend) VALUES (?, ?, ?, ?, ?)"
    )
    insert_user_clicks_month = session.prepare(
        "UPDATE user_clicks_by_month SET total_clicks = total_clicks + ? WHERE year_month = ? AND user_id = ?" 
    )
    
    print("Агрегація даних перед завантаженням...")
    # Створюємо словники для агрегації даних
    # Використовуємо defaultdict для зручності агрегації
    advertiser_spend_month = defaultdict(float)
    advertiser_spend_region = defaultdict(float)
    user_clicks_month = defaultdict(int) # Цей буде використовуватися для лічильника в UPDATE
    
    # Словник для збереження імен рекламодавців
    advertiser_names = {row['AdvertiserID']: row['AdvertiserName'] for _, row in df.iterrows()}

    for index, row in df.iterrows():
        event_time = row['EventTime']
        # Переконаємося, що event_time є об'єктом datetime
        if not isinstance(event_time, datetime):
            event_time = pd.to_datetime(event_time)

        was_clicked = pd.notna(row['ClickID']) # Перевіряємо наявність ClickID для визначення кліка
        ad_cost = Decimal(str(row['AdCost'])) if pd.notna(row['AdCost']) else Decimal('0.00')
        region = row['Region'] if pd.notna(row['Region']) else 'UNKNOWN'

        campaign_id = row['CampaignID']
        user_id = row['UserID']
        advertiser_id = row['AdvertiserID']
        
        # 1. Оновлення лічильників для кампанії (виконується одразу)
        session.execute(update_campaign_performance, (
            1 if was_clicked else 0, # +1 клік, якщо клікнуто, +0 в іншому випадку
            row['CampaignName'],
            row['AdvertiserName'],
            campaign_id,
            event_time.date() # Використовуємо лише дату для денної статистики
        ))

        # 3. Додавання запису в історію користувача (виконується одразу)
        session.execute(insert_user_history, (
            user_id,
            event_time,
            row['ImpressionID'], # Використовуємо вже перетворений UUID
            row['CampaignName'],
            was_clicked
        ))
        
        # Агрегація даних для інших таблиць
        year_month = event_time.strftime('%Y-%m')

        if ad_cost > 0:
            key_month = (year_month, advertiser_id)
            advertiser_spend_month[key_month] += float(ad_cost) # Агрегуємо як float, потім перетворюємо в Decimal при вставці

            key_region = (region, year_month, advertiser_id)
            advertiser_spend_region[key_region] += float(ad_cost)

        if was_clicked:
            key_user = (year_month, user_id)
            user_clicks_month[key_user] += 1
    
    print("Запис агрегованих даних у Cassandra...")
    # Запис агрегованих даних
    for (year_month, advertiser_id), total_spend_float in advertiser_spend_month.items():
        advertiser_name = advertiser_names.get(advertiser_id, 'Unknown')
        session.execute(insert_advertiser_spend_month, (year_month, advertiser_id, advertiser_name, Decimal(str(total_spend_float))))

    for (region, year_month, advertiser_id), total_spend_float in advertiser_spend_region.items():
        advertiser_name = advertiser_names.get(advertiser_id, 'Unknown')
        session.execute(insert_advertiser_spend_region, (region, year_month, advertiser_id, advertiser_name, Decimal(str(total_spend_float))))

    # Оновлюємо лічильники для user_clicks_by_month
    for (year_month, user_id), clicks_inc in user_clicks_month.items():
        session.execute(insert_user_clicks_month, (clicks_inc, year_month, user_id))

    print("Завантаження даних завершено.")


def main():
    """Головна функція для запуску процесу міграції."""
    mysql_conn = connect_mysql()
    cassandra_cluster, cassandra_session = connect_cassandra()

    if mysql_conn and cassandra_session:
        data_df = fetch_data_from_mysql(mysql_conn)
        if not data_df.empty:
            load_data_to_cassandra(cassandra_session, data_df)

        if mysql_conn: # Перевіряємо, чи існує з'єднання перед закриттям
            mysql_conn.close()
        if cassandra_cluster: # Перевіряємо, чи існує кластер перед закриттям
            cassandra_cluster.shutdown()
        print("З'єднання з базами даних закрито.")

if __name__ == '__main__':
    main()

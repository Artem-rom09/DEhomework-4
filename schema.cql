-- Створюємо простір ключів 
CREATE KEYSPACE IF NOT EXISTS ad_tracking
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE ad_tracking;

-- Таблиця для Запиту 1: CTR за кампанією за день
-- Використовує лічильники для ефективної агрегації показів та кліків
CREATE TABLE IF NOT EXISTS campaign_performance_by_day (
    campaign_id int,
    day date,
    campaign_name text,
    advertiser_name text,
    impressions counter, -- Лічильник для кількості показів
    clicks counter,      -- Лічильник для кількості кліків
    PRIMARY KEY (campaign_id, day)
) WITH CLUSTERING ORDER BY (day DESC); -- Сортування за датою в порядку спадання для легкого доступу до останніх даних

-- Таблиця для Запиту 2: Топ рекламодавців за витратами
-- та Запиту 5: Топ рекламодавців за витратами в регіоні
CREATE TABLE IF NOT EXISTS advertiser_spend_by_region_and_month (
    region text,       -- Регіон рекламної кампанії
    year_month text,   -- Формат 'YYYY-MM' для місяця та року
    total_spend decimal, -- Загальні витрати рекламодавця
    advertiser_id int,   -- Ідентифікатор рекламодавця
    advertiser_name text, -- Ім'я рекламодавця
    PRIMARY KEY ((region, year_month), total_spend, advertiser_id)
) WITH CLUSTERING ORDER BY (total_spend DESC); -- Сортування за витратами в порядку спадання для топ-N запитів

-- Таблиця для Запиту 3: Історія взаємодій користувача
CREATE TABLE IF NOT EXISTS user_engagement_history (
    user_id int,          -- Ідентифікатор користувача
    event_time timestamp, -- Час взаємодії (показу або кліку)
    impression_id uuid,   -- Унікальний ідентифікатор показу
    campaign_name text,   -- Назва рекламної кампанії
    was_clicked boolean,  -- Чи був клік на рекламу (true/false)
    PRIMARY KEY (user_id, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC); -- Сортування за часом події в порядку спадання для останніх взаємодій

-- Таблиця для Запиту 4: Топ користувачів за кліками
CREATE TABLE IF NOT EXISTS user_clicks_by_month (
    year_month text,   -- Формат 'YYYY-MM' для місяця та року
    total_clicks int,  -- Загальна кількість кліків користувача за місяць
    user_id int,       -- Ідентифікатор користувача
    PRIMARY KEY (year_month, total_clicks, user_id)
) WITH CLUSTERING ORDER BY (total_clicks DESC); -- Сортування за кількістю кліків в порядку спадання для топ-N запитів

-- Таблиця для Запиту 2: Топ рекламодавців за витратами (глобальна версія)
-- Ця таблиця агрегує витрати рекламодавців за місяць по всіх регіонах.
CREATE TABLE IF NOT EXISTS advertiser_spend_by_month (
    year_month text,     -- Формат 'YYYY-MM'
    total_spend decimal, -- Загальні витрати рекламодавця за місяць
    advertiser_id int,   -- Ідентифікатор рекламодавця
    advertiser_name text, -- Ім'я рекламодавця
    PRIMARY KEY (year_month, total_spend, advertiser_id)
) WITH CLUSTERING ORDER BY (total_spend DESC); -- Сортування за витратами в порядку спадання для топ-N запитів

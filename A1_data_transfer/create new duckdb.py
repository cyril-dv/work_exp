# Создание локалькой duckdb БД
# Примерное время: ~1 мин/партиция

import duckdb
import os

# Дата, на которую была создана локальная копия БД
curr_date = '2024-01-10'
curr_date = '_' + curr_date

# Директория, где хранится локальная копия БД
local_dir = r'D:\Копии retail_sales'
local_dir = os.path.join(local_dir, 'retail_sales' + curr_date)

# Директория и файл duckdb
db_fn = r'D:\duckdb\retail_sales.duckdb'

# Открытие соединения и создание файла duckdb
conx = duckdb.connect(database=db_fn)

# Создать схему и таблицы
conx.execute("""
        CREATE TABLE retail_sales (
            month_year DATE NOT NULL,
            week_start_date DATE NOT NULL,
            network_name VARCHAR NOT NULL,
            network_subname VARCHAR NOT NULL,
            global_sku_code VARCHAR NOT NULL,
            global_pos_code VARCHAR NOT NULL,
            pieces INTEGER NOT NULL,
            sales_rub DOUBLE NOT NULL
        );

        CREATE TABLE calendar (
            week INTEGER NOT NULL,
            "month" INTEGER NOT NULL,
            "year" INTEGER NOT NULL,
            month_upr INTEGER NOT NULL,
            year_upr INTEGER NOT NULL,
            week_start_date DATE NOT NULL,
            week_end_date DATE NOT NULL,
            month_date DATE NOT NULL,
            month_upr_date DATE NOT NULL,
        );

        CREATE TABLE base_sku (
            global_sku_code VARCHAR NOT NULL,
            network_name VARCHAR NOT NULL,
            sku_code VARCHAR NOT NULL,
            barcode VARCHAR,
            sku_name VARCHAR NOT NULL,
            global_sku_name VARCHAR NOT NULL,
            category VARCHAR NOT NULL,
            segment VARCHAR NOT NULL,
            "group" VARCHAR NOT NULL,
            brand VARCHAR NOT NULL,
            producer VARCHAR NOT NULL,
            weight DOUBLE NOT NULL,
            "change" timestamp NULL
        );

        CREATE TABLE base_sales_point (
            global_pos_code VARCHAR NOT NULL,
            network_name VARCHAR NOT NULL,
            pos_code VARCHAR NOT NULL,
            pos_name VARCHAR NOT NULL,
            store_address VARCHAR NOT NULL,
            store_format VARCHAR NOT NULL,
            network_subname VARCHAR NOT NULL,
            district VARCHAR NOT NULL,
            region VARCHAR NOT NULL,
            city VARCHAR NOT NULL,
            "change" timestamp NULL
        );
    """
)

# Добавить часто используемые представления
try:
    with (open(r"N:\Аналитика\Запросы\Представления\Корпоративные центры.sql", encoding='utf-8') as q1,
          open(r"N:\Аналитика\Запросы\Представления\Справочник весовых категорий.sql", encoding='utf-8') as q2,
          open(r"N:\Аналитика\Запросы\Представления\Форматы магазинов.sql", encoding='utf-8') as q3):
        sql_query_1, sql_query_2, sql_query_3 = q1.read(), q2.read(), q3.read()
except:
    print(f'Не удается открыть файл с SQL запросом:')
    raise

conx.execute(sql_query_1)
conx.execute(sql_query_2)
conx.execute(sql_query_3)

# Скопировать данные из справочников
conx.execute(f"""
        COPY calendar FROM '{os.path.join(local_dir, 'calendar' + curr_date + '.zst')}' (FORMAT 'csv', DELIMITER ';', HEADER 'true', quote '"', compression 'zstd');
        COPY base_sku FROM '{os.path.join(local_dir, 'base_sku' + curr_date + '.zst')}' (FORMAT 'csv', DELIMITER ';', HEADER 'true', quote '"', compression 'zstd');
        COPY base_sales_point FROM '{os.path.join(local_dir, 'base_sales_point' + curr_date + '.zst')}' (FORMAT 'csv', DELIMITER ';', HEADER 'true', quote '"', compression 'zstd');
    """
)
print('Справочники импортированы')
print()

# Перечень файлов с данными
partitions = [file_name for file_name in os.listdir(local_dir) if 'retail_sales_y' in file_name]
partitions.sort(key=lambda x: int(x[14:21].replace('_', '').split('m')[0])*1000 + int(x[14:21].replace('_', '').split('m')[1]))
print(f'Всего {len(partitions)} партиций')
print()

for partition in partitions:
    conx.execute(f"""
        INSERT INTO retail_sales SELECT month_year, week_start_date, network_name, network_subname, global_sku_code, global_pos_code, pieces, sales_rub
        FROM read_csv('{os.path.join(local_dir, partition)}', header=true, delim=';', compression='zstd',
                        columns={{
                            'year': 'int4',
                            'week': 'int4',
                            'network_name': 'varchar',
                            'network_subname': 'varchar',
                            'sku_code': 'varchar',
                            'global_sku_code': 'varchar',
                            'pos_code': 'varchar',
                            'global_pos_code': 'varchar',
                            'pieces': 'int4',
                            'sales_rub': 'float8',
                            'alt_sku': 'varchar',
                            'week_start_date': 'date',
                            'month': 'int4',
                            'file': 'text',
                            'created': 'timestamp',
                            'sheet': 'text',
                            'file_id': 'int4',
                            'month_year': 'date'
                        }}
                    )
    """)
    print(f'{partition} импортирован')

conx.close()
print()
print('Скрипт завершен')
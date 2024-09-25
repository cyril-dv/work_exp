# Создание копии данных из PostgreSQL
# Примерное время: ~4 мин/партиция

import pandas as pd
import psycopg2
import pyzstd
import os
from pprint import pprint
from datetime import date

# Данные для соединения
conx = psycopg2.connect(
    host='...',
    dbname='...',
    user='...',
    password='...'
)

# Директория, где будет хранится локальная копия БД
curr_date = '_' + str(date.today())
local_dir = r'D:\Копии retail_sales'

if not os.path.isdir(os.path.join(local_dir, 'retail_sales' + curr_date)):
    os.mkdir(os.path.join(local_dir, 'retail_sales' + curr_date))

# Перечень всех партиций таблицы retail_sales в БД
curs = conx.cursor()
curs.execute("select inhrelid::regclass as child from pg_catalog.pg_inherits where inhparent = 'public.retail_sales'::regclass order by 1")
partitions = curs.fetchall()
partitions = pd.Series(partitions).apply(lambda x: x[0]).to_list()
partitions = partitions[-1:]
# partitions = [m for m in partitions if 'y2024m' in m]

# Добавить таблицы со справочниками
partitions = ['base_sku', 'base_sales_point', 'calendar'] + partitions
pprint(partitions)
print()

# Сохранение и архивация данных в разрезе партиций
for partition in partitions:
    with open(os.path.join(local_dir, 'retail_sales' + curr_date, partition + curr_date + '.csv'), mode='w', encoding='utf_8') as csv_file:
        curs.copy_expert(f"COPY {partition} TO STDOUT WITH (FORMAT csv, HEADER true, DELIMITER ';', ENCODING 'UTF8')", csv_file)

    with open(os.path.join(local_dir, 'retail_sales' + curr_date, partition + curr_date + '.csv'), 'rb') as fi:
        with open(os.path.join(local_dir, 'retail_sales' + curr_date, partition + curr_date + '.zst'), 'wb') as fo:
            pyzstd.compress_stream(fi, fo, level_or_option={pyzstd.CParameter.compressionLevel: 12, pyzstd.CParameter.nbWorkers: 6})

    os.remove(os.path.join(local_dir, 'retail_sales' + curr_date, partition + curr_date + '.csv'))
    pprint(f'{partition} экспортирован')

curs.close()
conx.commit()
conx.close()
print()
print('Скрипт завершен')
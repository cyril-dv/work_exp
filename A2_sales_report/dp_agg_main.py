import numpy as np
import pandas as pd

import psycopg2
from psycopg2.extras import execute_values
import duckdb

import re
import os
from datetime import date, datetime


# Директория, в которой расположен файл
local_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(local_dir)


# Детали для подключения к PostgreSQL
try:
    conx = psycopg2.connect(
            host='...',
            dbname='...',
            user='...',
            password='...'
    )
except (Exception, psycopg2.DatabaseError) as err:
    raise Exception('Не удается подключиться к базе данных.') from err


print('Агрегация данных для ежемесячной отчетности')
print('='*79)
print('(Данные с 01.01.2022, кроме ____ - 01.05.2023 и ____ - 01.08.2023)')
print()


# Определение периода для агрегации
try:
    month_start = input('Начальная дата агрегации (в формате ДД.ММ.ГГГГ): ')
    month_end = input('Конечная дата агрегации (в формате ДД.ММ.ГГГГ): ')
    month_start = datetime.strptime(month_start, '%d.%m.%Y')
    month_end = datetime.strptime(month_end, '%d.%m.%Y')
except:
    raise Exception('Одна из дат указана в неверном формате.')
if month_end < month_start:
    raise Exception('Начальная дата позже конечной.')


# Создание списка с месяцами между начальной и конечной датой
months = pd.date_range(month_start, month_end, freq='MS').to_pydatetime().tolist()
months = [d.date() for d in months]


# Выбор сети для агрегации
all_chains = {
    'Сеть 1': 'network_name_1',
    'Сеть 2': 'network_name_2',
    'Сеть 3': 'network_name_3',
    'Сеть 4': 'network_name_4',
    'Сеть 5': 'network_name_5',
    'Сеть 6': 'network_name_6',
    'Сеть 7': 'network_name_7',
    'Сеть 7 (без оптового канала)': 'network_name_7',
    'Сеть 8 (по форматам/сетям)': 'network_name_8',
    'Сеть 8 (целиком)': 'network_name_8',
    'Сеть 9': 'network_name_9',
    'Сеть 10': 'network_name_10',
    'Сеть 11': 'network_name_11',
    'Сеть 12 (по сетям)': 'network_name_12',
    'Сеть 12 (по корп. центрам)': 'network_name_12'
}

all_chains = {k:[v[0], v[1]] for k, v in enumerate(all_chains.items(), start=1)}

print()
print('Доступные сети:')
print("".join(f"{i[0]}. {i[1][0]}\n" for i in all_chains.items()))

try:
    chosen_num = int(input('Выберите номер сети: '))
except:
    raise Exception('Указан неверный номер сети.')
if chosen_num not in all_chains.keys():
    raise Exception('Указан неверный номер сети.')

chosen_name = all_chains[chosen_num][0]
chosen_net = all_chains[chosen_num][1]


# Определение запросов, которые будут исполнены
if chosen_name == 'Сеть 7 (без оптового канала)':
    export_query = 'export_query_n7_neopt.sql'
    agg_query = 'agg_query_n7_neopt.sql'
    del_query = 'del_query_n7_neopt.sql'
    upd_table = 'dp_agg_n7_neopt'
elif chosen_name == 'Сеть 8 (целиком)':
    export_query = 'export_query_n8_all.sql'
    agg_query = 'agg_query_n8_all.sql'
    del_query = 'del_query_n8_all.sql'
    upd_table = 'dp_agg_n8_all'
elif chosen_name == 'Сеть 12 (по корп. центрам)':
    export_query = 'export_query_n12_cntrs_m.sql'
    agg_query = 'agg_query_n12_cntrs_m.sql'
    del_query = 'del_query_n12_cntrs_m.sql'
    upd_table = 'dp_agg_n12_cntrs_m'
else:
    export_query = 'export_query_main.sql'
    agg_query = 'agg_query_main.sql'
    del_query = 'del_query_main.sql'
    upd_table = 'dp_agg_main'

with (open(os.path.join(local_dir, 'queries', export_query), encoding='utf-8') as fn1, 
      open(os.path.join(local_dir, 'queries', agg_query), encoding='utf-8') as fn2, 
      open(os.path.join(local_dir, 'queries', del_query), encoding='utf-8') as fn3):
        export_query = fn1.read()
        agg_query = fn2.read()
        del_query = fn3.read()


# Экспорт справочников
print()
print('Экспорт и агрегация данных:')

curs = conx.cursor()
curs.execute('select * from base_sku')
df_skus = pd.DataFrame(curs.fetchall())
try:
    df_skus.columns = [desc[0] for desc in curs.description]
except Exception as err:
    raise Exception('Таблица со справочником SKU не содержит данных.') from err

curs.execute('select * from base_sales_point')
df_shops = pd.DataFrame(curs.fetchall())
try:
    df_shops.columns = [desc[0] for desc in curs.description]
except Exception as err:
    raise Exception('Таблица со справочником ТТ не содержит данных.') from err

curs.execute('select * from networks')
df_networks = pd.DataFrame(curs.fetchall())
try:
    df_networks.columns = [desc[0] for desc in curs.description]
except Exception as err:
    raise Exception('Представление с форматами магазинов не содержит данных.') from err

curs.execute('select * from n12_corp_centers')
df_corp_centers = pd.DataFrame(curs.fetchall())
try:
    df_corp_centers.columns = [desc[0] for desc in curs.description]
except Exception as err:
    raise Exception('Представление с корпоративными центрами не содержит данных.') from err

print(f'{datetime.now().strftime("%H:%M:%S")}> Справочники загружены')


# Создание таблиц для справочников в оперативной памяти
duckdb.sql('create table base_sku as select * from df_skus')
duckdb.sql('create table base_sales_point as select * from df_shops')
duckdb.sql('create table networks as select * from df_networks')
duckdb.sql('create table n12_corp_centers as select * from df_corp_centers')


# Основной цикл по месяцам
for month in months:
    # Получение данных за один месяц
    df = pd.DataFrame()

    curs.execute(export_query, (month, chosen_net))
    df_tmp = pd.DataFrame(curs.fetchall())
    
    try:
        df_tmp.columns = [desc[0] for desc in curs.description]
    except Exception as err:
        raise Exception('Таблица с продажами не содержит данных.') from err

    # Создание таблицы в оперативной памяти
    duckdb.sql('create table retail_sales as select * from df_tmp')

    # Добавление результатов агрегации к итоговому датафрейму
    df = pd.concat([df, duckdb.sql(agg_query).to_df()], ignore_index=True)
    df[['kg','sales_rub']] = df[['kg','sales_rub']].round(2)
    df['pieces'] = df['pieces'].astype('int')
    df['last_update'] = datetime.now()
    df = df.where(pd.notnull(df), None)

    # Обнуление созданной в памяти таблицы
    duckdb.sql('drop table retail_sales')
    
    # Удаление данных предыдущей агрегации
    try:
        curs.execute(del_query, (month, chosen_net))
        conx.commit()
    except (Exception, psycopg2.DatabaseError) as err:
        curs.close()
        conx.close()
        duckdb.close()
        raise Exception('Не удается удалить старые данные.') from err

    # Загрузка новых данных
    try:    
        cols = ','.join(list(df.columns)).replace('group', '"group"')
        execute_values(
            curs,
            f"INSERT INTO {upd_table} ({cols}) VALUES %s",
            [tuple(i) for i in df.to_numpy()]
        )
        conx.commit()
        print(f'{datetime.now().strftime("%H:%M:%S")}> Завершен месяц {month} сети {chosen_net}')
    except (Exception, psycopg2.DatabaseError) as err:
        curs.close()
        conx.close()
        duckdb.close()
        raise Exception(f'Не удается обновить данные ({month}, {chosen_net}).') from err

print()
print('-'*79)
print('Экспорт и агрегация завершены')
print('-'*79)


curs.close()
conx.close()

duckdb.close()
import numpy as np
import pandas as pd
np.set_printoptions(suppress=True, precision=2)
pd.options.display.float_format = '{:,.2f}'.format

import matplotlib as mpl
import matplotlib.pyplot as plt

import openpyxl as op
import duckdb
from fpdf import FPDF

import os
import sys
from datetime import datetime


try:
    LOCAL_DIR = os.path.dirname(__file__)
except:
    LOCAL_DIR = os.getcwd()

PARENT_DIR = os.path.dirname(LOCAL_DIR)
sys.path.append(PARENT_DIR)

import tbl_calc

REPORT_DATE = ' (апр-июн 24)'
REPORT_MTH = tbl_calc.REPORT_MTH
DB_FILE = tbl_calc.DB_FILE


print('\n17. Распределение цен')
print('-'*79)


groups_dict = {
    'Группа 1': 'a. Группа 1',
    'Группа 2': 'b. Группа 2',
    'Группа 3': 'c. Группа 3',
    'Группа 4': 'e. Группа 4',
    'Группа 5': 'f. Группа 5',
    'Группа 6': 'd. Группа 6'
}

share_limits = [0.2]*4 + [0.1]

xx_brands = ['Бренд 1', 'Бренд 2', 'Бренд 3', 'Бренд 4', 'Бренд 5' 
             'Бренд 6', 'Бренд 7', 'Бренд 8', 'Бренд 9']


try:
    conx = duckdb.connect(database=DB_FILE, read_only=True)
except Exception as err:
    print(f'Не удается подключиться к базе данных:\n{err}')


try:
    with (open(os.path.join(LOCAL_DIR, '17. Распределение цен.sql'), encoding='utf-8') as q1, 
          open(os.path.join(LOCAL_DIR, '17. Распределение цен (XXXX).sql'), encoding='utf-8') as q2):
        sql_query_1, sql_query_2 = q1.read(), q2.read()
except Exception as err:
    print(f'Не удается открыть файл с SQL запросом:\n{err}')


df = pd.DataFrame()

for i, j in zip(list(groups_dict.keys())[0:-1], share_limits):
    df_tmp = conx.execute(sql_query_1, [i, j]).fetch_df()
    df = pd.concat([df, df_tmp])

df_tmp = conx.execute(sql_query_2).fetch_df()
df = pd.concat([df, df_tmp])

conx.close()


df['group'] = df['group'].replace(groups_dict)
df['pct'] = (df['pct'] * 100).round(2)
df['weight'] = df['weight'].astype(int)


grouped = df.groupby(['group', 'brand', 'weight'])


for name, group in grouped:
    group_name = str(name[0][3:])
    brand = str(name[1])
    weight = str(name[2])

    if group_name == 'Группа 1':
        measure = 'шт.'
        unit = 'Цена за шт.'
    if group_name in ['Группа 2', 'Группа 3']:
        measure = 'мл.'
        unit = 'Цена за кг.'
    if group_name in ['Группа 4', 'Группа 5', 'Группа 6']:
        measure = 'гр.'
        unit = 'Цена за кг.'

    if group['pieces'].sum():
        fig, ax = plt.subplots(figsize=(16, 4))
        if brand in xx_brands:
            barc = ax.bar(x=group['bin_range'], height=group['pct'], width=0.98, zorder=2, color=None, facecolor='#FFFFFF', edgecolor='#00AE81', linewidth=1, hatch='\\\\')
        else:
            barc = ax.bar(x=group['bin_range'], height=group['pct'], width=0.98, zorder=2, color=None, facecolor='#FFFFFF', edgecolor='#005AAD', linewidth=1, hatch='\\\\')
        ax.grid(True, which='major', axis='y', color='#E7E7E8', zorder=0)
        ax.bar_label(barc, label_type='edge', padding=2, fmt='%.2f')
        ax.tick_params(axis='both', which='both', labelsize=9, bottom=False, top=False, labelbottom=True, left=False, right=False, labelleft=True)
        ax.spines[["left", "top", "right"]].set_visible(False)
        ax.annotate(group_name, xy=(0,0), xytext=(0.01, 1.08), xycoords='axes fraction')
        ax.annotate(' '.join([brand, weight, measure]), xy=(0, 0), xytext=(0.01, 1.02), xycoords='axes fraction')
        ax.set_xlabel(unit, loc='center')
        ax.set_ylabel('Доля, %', loc='center')

        if not os.path.isdir(f"{LOCAL_DIR}\\{group_name}"):
            os.mkdir(f"{LOCAL_DIR}\\{group_name}")
        plt.savefig(f"{LOCAL_DIR}\\{group_name}\\{group_name}, {brand} {weight}.png", dpi=150, pad_inches=0, bbox_inches='tight')
        plt.close()


file_dict = {}
DIR_NAMES = [i[3:] for i in sorted(list(groups_dict.values()))]

for d in DIR_NAMES:
    file_list = []
    for f in os.listdir(os.path.join(LOCAL_DIR, d)):
        if f.endswith('png'):
            file_list.append(os.path.join(LOCAL_DIR, d, f))
    file_dict[d] = file_list


img_width = 1926/18 # approximate size in mm
img_height = 576/18
img_xmargin = 20
img_ymargin = 5

page_max = 10
col_max = 5

pdf = FPDF(orientation='L', unit='mm', format='A4')
pdf.set_margin(10)
# requires a unicode font in module's folder, for example in "C:\Users\...\AppData\Local\Miniconda3\Lib\site-packages\fpdf\font"
# GNU FreeFont @ https://www.gnu.org/software/freefont/ is used
pdf.add_font('FreeSerif', '', 'FreeSerif.ttf')
pdf.add_font('FreeSerifBold', 'B', 'FreeSerifBold.ttf')

for group, paths in file_dict.items():
    pdf.add_page()
    pdf.set_font('FreeSerifBold', 'B', 14)
    pdf.text(10, 10, 'Распределение объемов по ценовым диапазонам')
    pdf.set_font('FreeSerifBold', 'B', 10)
    pdf.text(10, 15, group + REPORT_DATE)

    img_on_page = 1
    img_in_col = 1
    
    for img in paths:
        if img_on_page > page_max:
            pdf.add_page()
            pdf.set_font('FreeSerifBold', 'B', 14)
            pdf.text(10, 10, 'Распределение объемов по ценовым диапазонам')
            pdf.set_font('FreeSerifBold', 'B', 10)
            pdf.text(10, 15, group + REPORT_DATE)

            img_on_page = 1
            img_in_col = 1
    
        if img_in_col <= col_max:
            pdf.image(img, x=10, y=img_xmargin+img_height*(img_in_col-1)+img_ymargin*(img_in_col-1), w=img_width, h=img_height)
            img_in_col += 1
        else:
            pdf.image(img, x=img_width+img_xmargin, y=img_xmargin+img_height*(img_in_col-1-col_max)+img_ymargin*(img_in_col-1-col_max), w=img_width, h=img_height)      
            img_in_col += 1

        img_on_page += 1

pdf.output(os.path.join(LOCAL_DIR, 'Приложение 2. Распределение цен.pdf'))


print(f'{datetime.now().strftime("%H:%M:%S")}> Готово')
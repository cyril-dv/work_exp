import numpy as np
import pandas as pd
np.seterr(all='raise')


REPORT_MTH = 6
DB_FILE = r'D:\duckdb\retail_sales.duckdb'


def insert_wsp(df, right_margin, column='wsp', value=' '):
    df.insert(loc=(df.shape[1]-right_margin), column=column, value=value, allow_duplicates=True)
    return df


def margins(df):
    return pd.concat(
            [df, pd.DataFrame(df.sum(axis=0), columns=['Итого']).T], 
            axis=0, 
            ignore_index=False
        )


def changes(row, curr_month=2, period='m_on_m', calc_method='abs_change', aggfunc='sum'):
    if period == 'm_on_m':
        period_prev = row.iloc[-2]
        period_curr = row.iloc[-1]
    elif period == 'm_on_m_last_year':
        period_prev = row.iloc[curr_month-1]
        period_curr = row.iloc[-1]
    elif period == 'ytd_on_ytd':
        if aggfunc == 'sum':
            period_prev = np.nansum(row.iloc[:curr_month])
            period_curr = np.nansum(row.iloc[-curr_month:])
        elif aggfunc == 'mean':
            period_prev = np.nanmean(row.iloc[:curr_month])
            period_curr = np.nanmean(row.iloc[-curr_month:])
        else:
            raise Exception("Указан неверный метод агрегации. Должен быть один из 'sum', 'mean'.")
    else:
        raise Exception("Указан неверный период для сравнения. Должен быть один из 'm_on_m', 'm_on_m_last_year', 'ytd_on_ytd'.")

    if calc_method == 'abs_change':
        try:
            result = f'{(period_curr - period_prev):,.0f}'.replace(',', ' ').replace('.', ',')
        except:
            result = '–'
    elif calc_method == 'pp_change':
        try:
            result = f'{(period_curr - period_prev):,.1f}'.replace(',', ' ').replace('.', ',')
        except:
            result = '–'
    elif calc_method == 'pct_change':
        try:
            if (period_curr/period_prev)-1 > 1.5:
                result = f'{(period_curr/period_prev):.1f}x'.replace('.', ',')
            else:
                result = f'{(period_curr/period_prev-1):.0%}'.replace('.', ',')
        except:
            result = '–'
    else:
        raise Exception("Указан неверный тип сравнения. Должен быть один из 'abs_change', 'pct_change', 'pp_change'.")
    
    result = result.replace('-0%', '0%').replace('nan%', '–').replace('nan', '–')

    if result == '-0':
        result = '0'

    if result == '-0,0':
        result = '0,0'

    return result
select
    global_sku_code,
    global_pos_code,
    wks,
    string_agg(extract(week from week_start_date)::text, ';') as wks_list,
    sum(kg) as kg,
    sum(rub) as rub
from
    (select
        m.network_subname_alt as network_subname,
        r.week_start_date,
        r.global_pos_code,
        p.global_sku_code,
        sum(r.pieces * p.weight / 1000.0) as kg,
        sum(r.sales_rub) as rub,
        count(r.week_start_date) over (partition by r.global_pos_code) as wks
        from retail_sales as r 
        join base_sku as p on r.global_sku_code = p.global_sku_code 
        join base_sales_point as n on r.global_pos_code = n.global_pos_code
        join networks as m on r.global_pos_code = m.global_pos_code
    where 1=1
        and m.network_subname_alt = ?
        and r.global_sku_code = ?
        and r.week_start_date between ? and ?
        group by 
            m.network_subname_alt,
            r.week_start_date,
            r.global_pos_code,
            p.global_sku_code
        order by
            r.global_pos_code,
            r.week_start_date
    ) as q1
    group by 
        global_sku_code,
        global_pos_code,
        wks
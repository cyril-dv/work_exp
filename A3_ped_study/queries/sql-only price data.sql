with weeks_list as (
    select *
    from generate_series('2024-01-15'::date, '2024-03-25'::date, '1 week'::interval) as wks
),
pos_list as (
    select
        global_sku_code,
        global_pos_code,
        wks,
        string_agg(extract(week from week_start_date)::text, ';') as wks_list,
        sum(kg) as kg,
        sum(rub) as rub
    from (
        select
            m.network_subname_alt as network_subname,
            r.week_start_date,
            r.global_pos_code,
            p.global_sku_code,
            sum(r.pieces*p.weight/1000.0) as kg,
            sum(r.sales_rub) as rub,
            count(r.week_start_date) over (partition by r.global_pos_code) as wks
            from retail_sales as r 
            join base_sku as p on r.global_sku_code = p.global_sku_code 
            join base_sales_point as n on r.global_pos_code = n.global_pos_code
            join networks as m on r.global_pos_code = m.global_pos_code
        where 1=1
            and r.global_sku_code = '____'
            and m.network_subname_alt = '____'
            and r.week_start_date between '2024-01-15' and '2024-03-25'
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
),
pos_list_cleaned as (
    select 
        global_sku_code,
        global_pos_code,
        wks,
        wks_list,
        kg,
        rub
    from pos_list
    where wks > (select floor(0.25*count(*)) as weeks_limit from weeks_list)
),
sales_data as (
    select
        wk_num,
        week_start_date,
        price_kg,
        price_bin,
        price_bin::int::varchar || '-' || (price_bin + 5)::int::varchar as price_bin_range,
        avg(kg) as turnover,
        sum(kg) as kg,
        count(global_pos_code) as akb
    from (
        select
            extract(week from r.week_start_date) as wk_num,
            r.week_start_date,
            r.global_pos_code,
            p.global_sku_code,
            p.global_sku_name,
            sum(r.pieces*p.weight/1000.0) as kg,
            sum(r.sales_rub) as rub,
            sum(r.sales_rub)/sum(r.pieces*p.weight/1000.0) as price_kg,
            floor(sum(r.sales_rub)/sum(r.pieces*p.weight/1000.0)/5.00)*5 as price_bin
        from retail_sales as r 
        join base_sku as p on r.global_sku_code = p.global_sku_code 
        join base_sales_point as n on r.global_pos_code = n.global_pos_code
        join networks as m on r.global_pos_code = m.global_pos_code
        where 1=1
            and r.global_sku_code = '____'
            and m.network_subname_alt = '____'
            and r.week_start_date between '2024-01-15' and '2024-03-25'
            and r.global_pos_code in (select global_pos_code from pos_list_cleaned)
        group by
            extract(week from r.week_start_date),
            r.week_start_date,
            r.global_pos_code,
            p.global_sku_code,
            p.global_sku_name
    ) as q    
    group by
        wk_num,
        week_start_date,
        price_kg,
        price_bin,
        price_bin::int::varchar || '-' || (price_bin + 2.5)::int::varchar
)
select
    price_bin + 2.5 as price_bin,
    price_bin_range,
    sum(kg) as kg,
    sum(akb) as akb,
    sum(kg)/sum(akb) as turnover
from sales_data
group by
    price_bin,
    price_bin_range
having sum(akb) >= (select round(0.0067*p1*p2)::int as pos_limit 
                       from (
                        (select 'limit' as lim, count(*) as p1 from weeks_list) as q1 
                        join 
                        (select 'limit' as lim, count(*) as p2 from pos_list_cleaned) as q2 on q1.lim = q2.lim
                        )
                    )
order by price_bin
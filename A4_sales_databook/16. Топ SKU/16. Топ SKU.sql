select 
    case 
        when prev_qtr.pos_prev is null then concat(curr_qtr.pos_curr, ' (+', curr_qtr.pos_curr, ')')
        when coalesce(prev_qtr.pos_prev, 0) - curr_qtr.pos_curr > 0 then concat(curr_qtr.pos_curr, ' (+', coalesce(prev_qtr.pos_prev, 0) - curr_qtr.pos_curr, ')')
        when coalesce(prev_qtr.pos_prev, 0) - curr_qtr.pos_curr < 0 then concat(curr_qtr.pos_curr, ' (',  coalesce(prev_qtr.pos_prev, 0) - curr_qtr.pos_curr, ')')
        when coalesce(prev_qtr.pos_prev, 0) - curr_qtr.pos_curr = 0 then concat(curr_qtr.pos_curr, ' (0)')
    end as num,
    curr_qtr.global_sku_name,
    curr_qtr."group",
    curr_qtr.producer,
    curr_qtr.brand,
    curr_qtr.weight,
    coalesce(prev_qtr.tn_prev, 0) as tn_prev,
    curr_qtr.tn_curr,
    coalesce(prev_qtr.thou_rub_prev, 0) as thou_rub_prev,
    curr_qtr.thou_rub_curr,
    coalesce(prev_qtr.avr_price_prev, 0) as avr_price_prev,
    coalesce(curr_qtr.avr_price_curr, 0) as avr_price_curr,
    coalesce(prev_qtr.akb_prev, 0) as akb_prev,
    curr_qtr.akb_curr,
    coalesce(prev_qtr.turnover_kg_mnth_prev, 0) as turnover_kg_mnth_prev,
    curr_qtr.turnover_kg_mnth_curr
from
    (
    select
        row_number() over (partition by p."group" order by sum(r.pieces * p.weight / 1000000) desc nulls last, sum(sales_rub) desc nulls last) as pos_curr,
        p.global_sku_name,
        p."group",
        p.producer,
        p.brand,
        p.weight,
        sum(sales_rub) / 1000 as thou_rub_curr,
        sum(r.pieces * p.weight / 1000000) as tn_curr,
        sum(sales_rub) / nullif(sum(r.pieces * p.weight / 1000), 0) as avr_price_curr,
        (sum(r.pieces * p.weight / 1000) / count(distinct r.global_pos_code)) / 3 as turnover_kg_mnth_curr,
        count(distinct r.global_pos_code) as akb_curr
    from retail_sales as r
    join base_sku as p on r.global_sku_code = p.global_sku_code
    join networks as m on r.global_pos_code = m.global_pos_code 
    where 1=1
        and r.month_year between ('2024-06-01'::date - '2 months'::interval)::date and '2024-06-01'
        and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6') 
        and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
        and m.store_format_alt = any(?)
    group by
        p.global_sku_name,
        p."group",
        p.producer,
        p.brand,
        p.weight
    ) as curr_qtr
left join
    (
    select
        row_number() over (partition by p."group" order by sum(r.pieces * p.weight / 1000000) desc nulls last, sum(sales_rub) desc nulls last) as pos_prev,
        p.global_sku_name,
        p."group",
        p.producer,
        p.brand,
        p.weight,
        sum(sales_rub) / 1000 as thou_rub_prev,
        sum(r.pieces * p.weight / 1000000) as tn_prev,
        sum(sales_rub) / nullif(sum(r.pieces * p.weight / 1000), 0) as avr_price_prev,
        (sum(r.pieces * p.weight / 1000) / count(distinct r.global_pos_code)) / 3 as turnover_kg_mnth_prev,
        count(distinct r.global_pos_code) as akb_prev
    from retail_sales as r
    join base_sku as p on r.global_sku_code = p.global_sku_code
    join networks as m on r.global_pos_code = m.global_pos_code 
    where 1=1
        and r.month_year between ('2024-06-01'::date - '5 months'::interval)::date and ('2024-06-01'::date - '3 months'::interval)::date
        and p."group" in ('group_1', 'group_2', 'group_3', 'group_4', 'group_5', 'group_6') 
        and r.network_name not in ('network_1', 'network_2', 'network_3', 'network_4', 'network_5')
        and m.store_format_alt = any(?)
    group by
        p.global_sku_name,
        p."group",
        p.producer,
        p.brand,
        p.weight
    ) as prev_qtr
on curr_qtr."group" = prev_qtr."group" and curr_qtr.global_sku_name = prev_qtr.global_sku_name
order by
    curr_qtr."group",
    curr_qtr.pos_curr asc
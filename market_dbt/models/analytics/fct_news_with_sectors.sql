with news as (
    select
        md5(concat_ws('|', time::string, feed, coalesce(source, ''),
                      coalesce(tickers, ''), coalesce(headline, ''))) as news_id,
        *
    from {{ source('raw', 'raw_dash_exports') }}
),

news_tickers as (
    select n.news_id, trim(t.value) as ticker
    from news n, lateral split_to_table(n.tickers, ',') t
    where n.tickers is not null
),

news_sectors as (
    select
        nt.news_id,
        listagg(distinct d.sector, ', ') within group (order by d.sector) as sectors
    from news_tickers nt
    join {{ ref('dim_tickers') }} d on d.ticker = nt.ticker
    group by nt.news_id
)

select n.*, s.sectors
from news n
left join news_sectors s on s.news_id = n.news_id
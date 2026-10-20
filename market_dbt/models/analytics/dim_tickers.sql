with latest as (
    select *
    from {{ source('raw', 'raw_ticker_details') }}
    qualify row_number() over (partition by ticker order by loaded_at desc) = 1
)

select
    ticker,
    name,
    type,
    sic_code,
    sic_description,
    case
        when type in ('ETF', 'ETN', 'ETV') then 'ETF / Fund'
        when type = 'WARRANT' then 'Warrant'
        when sic_code is null then 'Unknown'
        when sic_code::int between 1300 and 1399
          or sic_code::int between 1200 and 1299
          or sic_code::int between 2900 and 2999 then 'Energy'
        when sic_code::int between 2830 and 2836
          or sic_code::int between 3840 and 3851
          or sic_code::int between 8000 and 8099 then 'Health Care'
        when sic_code::int between 3570 and 3579
          or sic_code::int between 3660 and 3699
          or sic_code::int between 3800 and 3829
          or sic_code::int between 7370 and 7379 then 'Information Technology'
        when sic_code::int between 4800 and 4899
          or sic_code::int between 2700 and 2799
          or sic_code::int between 7310 and 7319
          or sic_code::int between 7800 and 7899 then 'Communication Services'
        when sic_code::int between 4900 and 4999 then 'Utilities'
        when sic_code::int between 6500 and 6599
          or sic_code::int = 6798 then 'Real Estate'
        when sic_code::int between 6000 and 6799 then 'Financials'
        when sic_code::int between 100 and 999
          or sic_code::int between 2000 and 2199
          or sic_code::int between 2840 and 2844
          or sic_code::int between 5400 and 5499
          or sic_code::int = 5912 then 'Consumer Staples'
        when sic_code::int between 3710 and 3716
          or sic_code::int between 2200 and 2399
          or sic_code::int between 2500 and 2599
          or sic_code::int between 3100 and 3199
          or sic_code::int between 3630 and 3652
          or sic_code::int between 3900 and 3999
          or sic_code::int between 5200 and 5999
          or sic_code::int between 7000 and 7299
          or sic_code::int between 7900 and 7999
          or sic_code::int between 8200 and 8299 then 'Consumer Discretionary'
        when sic_code::int between 1000 and 1099
          or sic_code::int between 1400 and 1499
          or sic_code::int between 2400 and 2499
          or sic_code::int between 2600 and 2699
          or sic_code::int between 2800 and 2899
          or sic_code::int between 3000 and 3099
          or sic_code::int between 3200 and 3399 then 'Materials'
        else 'Industrials'
    end as sector
from latest

-- For each week day, convert each fund nav into USD. 
-- If data is missing for that day, forward fill from previous
-- values...

WITH RECURSIVE dates(date) AS (
  select min(date) from fund_prices
  UNION ALL
  SELECT datetime(date, '+1 day')
  FROM dates
  WHERE date <= (select max(date) from fund_prices)
),

weekdays as (
    SELECT date
    FROM dates
    where strftime('%w', date) not in ('0','6') -- Sunday=0, Saturday=6
),

dates_per_fund as (
    select fund_id, min(date) as min_date, max(date) as max_date, currency
    from fund_prices
    group by fund_id, currency
),

all_generated_dates as (
    select weekdays.date, fund_id, currency from dates_per_fund, weekdays
    where weekdays.date >= min_date
    and max_date >= weekdays.date 
),

all_dates_with_missing_data as (
    select 
        all_generated_dates.date, 
        all_generated_dates.fund_id,
        nav,
        rate,
        all_generates_dates.currency,
        --ffill is not available in sqlite, so we use count + first_value trick!
        count(nav) over (partition by all_generated_dates.fund_id order by all_generated_dates.date) as nav_helper, 
        count(rate) over (partition by all_generated_dates.fund_id order by all_generated_dates.date) as fx_helper
    from all_generated_dates
    left join fund_prices on all_generated_dates.fund_id = fund_prices.fund_id and all_generated_dates.date = fund_prices.date
    left join fx on all_generated_dates.currency = fx.from_currency and all_generated_dates.date = fx.date
),

all_dates_with_filled_data as (

    select date,
        fund_id,
        first_value(nav) over (partition by fund_id, nav_helper order by date) as nav,
        first_value(rate) over (partition by fund_id, fx_helper order by date) as rate
    from all_dates_with_missing_data
)

select date, fund_id, nav*rate as nav_in_usd from all_dates_with_filled_data

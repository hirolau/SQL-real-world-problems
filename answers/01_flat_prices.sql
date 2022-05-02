
-- Version 1 without window functions:
-- List prices that have had more than 5 flat observations in a row for the most recent date:

with max_dates as (
    select max(date) as date, bond_id from bond_prices group by bond_id
),

last_prices as (
    select bond_prices.price, bond_prices.bond_id 
    from bond_prices join max_dates on max_dates.bond_id = bond_prices.bond_id
    and max_dates.date = bond_prices.date
),

last_date_with_different_price as (
    select max(bond_prices.date) as date, bond_prices.bond_id as bond_id from bond_prices join last_prices on last_prices.bond_id = bond_prices.bond_id
    where last_prices.price != bond_prices.price
    group by bond_prices.bond_id
),

number_flat_prices as (
    select bond_prices.bond_id, count(*) as number_of_flat_prices
    from bond_prices join last_date_with_different_price
    on bond_prices.bond_id = last_date_with_different_price.bond_id
    where bond_prices.date > last_date_with_different_price.date
    group by bond_prices.bond_id
    having count(*) > 5
)

select * from number_flat_prices;

-- Version 2 with window functions:
-- List prices that have had more than 5 flat observations in a row for the most recent date:

with islands as (
    select *,
    row_number() over (partition by bond_id order by date desc) 
    - row_number() over (partition by bond_id, price order by date desc) as island
    from bond_prices
)
select bond_id, count(*) as number_of_flat_prices from islands
where island = 0
group by bond_id
having count(*) > 5

-- Selects defaults from missed_payments table and makes a new
-- definition in which gaps with less than or equal to 3 perdios
-- are merge into 1 large default.

-- Step 1, use gaps and islands to mark each "period"
-- Step 2, find periods at the beginning and end of each customers lifetime
-- Step 3, find all non-defaulted spaces with 3 or less periods.

with step1 as (
    select customer_id, date, missed_payments, defaulted,
    row_number() over (partition by customer_id order by date) 
    - row_number() over (partition by customer_id, defaulted order by date) as island
    from missed_payments
),

step2 as (
    select *, 
        case when island = 0 then 1
        when island = max(island) over (partition by customer_id) then 1
        else 0 end as is_first_or_last_period
    from step1
)

-- Step 3
select customer_id, date, missed_payments,
case
    when is_first_or_last_period = 1 then defaulted
    when defaulted = 0 and count(island) over (partition by customer_id, island) <= 3 then 1
    else defaulted
end as new_defaulted
from step2
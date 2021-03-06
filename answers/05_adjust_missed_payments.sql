
-- Adjust missed payments with recoveries.
-- Recoveries and missed payments cancel each other out so that the latest missed payments
-- before the recovery is cancelled out as much as possible by the recovered amount.

with default_periods_prep as (
    -- First I want a flag to that is unique to the period in which a recovery can cancel out missed payments.
    -- I use when the default flag flips:
    select m.*, recovered_amount,
    case 
        when defaulted = 0 and lag(defaulted) over (partition by m.customer_id order by m.date) = 1 then 1 
        else 0 
    end as new_period_flag
    from missed_payments m left join recovered_payments j
    on m.date = j.date and m.customer_id = j.customer_id
),

default_periods as (
    select *, sum(new_period_flag) over (partition by customer_id order by date) as default_period
    from default_periods_prep
),

recovered_funds as (
select *,
    -- sum_missed_payments = cumulative sum of missed payments from the bottom up.
    sum(missed_payments) over (partition by customer_id, default_period order by date desc) as sum_missed_payments,
    
    -- Not cumulative since we do not have a direction. Just need recovered amount available on every row.
    sum(recovered_amount)  over (partition by customer_id, default_period) as recovered_in_period,
    
    -- By subtracting cumulative sum from the total payments in the period we get a variable on how much
    -- we have left to subtract. As soon as this turns negative we should stop removing values!
    
    sum(recovered_amount)  over (partition by customer_id, default_period) 
    - sum(missed_payments) over (partition by customer_id, default_period order by date desc) recoveries_left
    
from default_periods

),

final_recovered_funds as (
    -- On the first period recoveries_left turn negative we need to take special care to get the correct amount. Thus we create this help variable.
    -- We did just run out of recoveries when out_of_funds == 1.
    select *, sum(case when recoveries_left <= 0 then 1 else 0 end) over (partition by customer_id, default_period order by date desc) as out_of_funds
    from recovered_funds
)

select customer_id, 
       date, 
       missed_payments, 
       defaulted, 
       recovered_amount,  
       case 
            when out_of_funds = 0 then 0 
            when out_of_funds = 1 then -recoveries_left
            else missed_payments
       end as missed_payments_minus_recovered
from final_recovered_funds

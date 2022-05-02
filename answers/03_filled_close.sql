

-- This query recursivly calculates the filled_close variable.

with recursive basic_pivot as (
    -- We can do this as we know bid and ask exists on each date we care about...
    select a.date as date, 
        c.price as close,
        b.price as bid,
        a.price as ask,
        a.stock_id as stock_id
    from prices a 
        inner join prices b on a.date = b.date and a.stock_id = b.stock_id and b.price_type = 'bid'
        left join prices c on a.date = c.date and c.stock_id = a.stock_id and c.price_type = 'close'
    where a.price_type = 'ask'
),

base as (
    -- The rank is calculated to have a good variable for our recursion.
    select *, row_number() over (partition by stock_id order by date) as rnk from basic_pivot
),

final as (

  -- First date for each stock...
  select stock_id, bid, ask, close, date, close as filled_close, rnk
  from base 
  where rnk = 1
  
  UNION ALL
  
  -- Recursively join in the next period to get all datapoints needed for the calculation.
  select 
      base.stock_id, 
      base.bid, 
      base.ask, 
      base.close, 
      base.date, 
      case 
          when base.close is not null then base.close
          when base.bid > final.filled_close then base.bid
          when base.ask < final.filled_close then base.ask
          else final.filled_close
      end as filled_close, 
      base.rnk
  from final join base on final.stock_id = base.stock_id and base.rnk = final.rnk + 1
)
  
SELECT stock_id, date, filled_close FROM final;
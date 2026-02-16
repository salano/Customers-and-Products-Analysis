create or replace view gold.product_metrics as
with base_query as (
  /*
   Retrive core columns from the gold products and orders_fact tables
  */
  select
    of.OrderID,
    of.LastEditedWhen,
    of.CustomerID,
    round((of.Quantity * of.UnitPrice),2) as revenue,
    of.Quantity,
    of.UnitPrice,
    p.ProductID,
    p.ProductName,
    p.Brand
  from
    gold.orders_fact of 
    left join gold.products p 
    on of.ProductID = p.ProductID
  where
    p.active_flg = 1
)
,
product_aggregation as (
  /*
   Summarize key metrics
  */
  select
      ProductID,
      ProductName,
      Brand,
      UnitPrice,
      count(distinct OrderID) as total_orders,
      round(sum(revenue),2) as total_revenue,
      sum(Quantity) as total_quantity,
      count(distinct CustomerID) as total_customers,
      max(LastEditedWhen) as last_order_date,
      min(LastEditedWhen) as first_order_date,
      date_diff(MONTH, min(LastEditedWhen), max(LastEditedWhen)) as life_span,
      round(avg(UnitPrice), 2) average_price
  from 
    base_query
  group by 
    ProductID,
    ProductName,
    Brand,
    UnitPrice
) 

/*
Combine all results into final output
*/
select 
  ProductID,
    ProductName,
    Brand,
    UnitPrice,
    last_order_date,
    datediff(month, last_order_date, getdate()) as recency,
    case when  total_revenue > 50000 then 'High-Performer'
        when total_revenue > 20000 and total_revenue <= 50000 then 'Mid-Range'
        else 'Low-Performer' 
    end as product_segment,
    life_span,
    total_orders,
    total_revenue,
    total_quantity,
    total_customers,
    average_price,
    --Average order Revenue (AVO)
    case when total_orders = 0 then 0
         else total_revenue / total_orders
    end as Average_order_revenue,
    --Average Monthly Revenue 
    case when life_span = 0 then total_revenue
         else total_revenue / life_span
    end as Average_monthly_revenue
from
  product_aggregation

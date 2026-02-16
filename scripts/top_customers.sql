-- Find the top 10 customers who have generated the highest revenue
create or replace view gold.top_10_customers as
select *
from
(
  SELECT 
      customer_skey,
      c.CustomerID,
      concat(c.CustomerFirstName,' ',c.CustomerLastName) as customer_name,
      round(SUM(of.Quantity * of.UnitPrice),2) AS total_revenue,
      RANK() OVER (ORDER BY SUM(of.Quantity * of.UnitPrice) DESC) AS rank_customer
  FROM gold.orders_fact of
  LEFT JOIN gold.customers c
      ON c.CustomerID = of.CustomerID
  GROUP BY 
      customer_skey,
      c.CustomerID,
      concat(c.CustomerFirstName,' ',c.CustomerLastName) 
  ORDER BY total_revenue DESC
)
where 
  rank_customer <= 10
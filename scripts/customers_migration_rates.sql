create or replace view gold.migration_rates as 
  with base_query as(
    select 
      of.OrderID,
      of.LastEditedWhen,
      c.CustomerID,
      DATEDIFF(day, MAX(of.LastEditedDate), GETDATE()) AS Recency
    from 
      gold.dim_date dd
    join
      gold.orders_fact of 
    on of.LastEditedDate = dd.date
    left join 
      gold.customers c 
    on of.CustomerID = c.CustomerID
    where 
      c.active_flg = 1
    group by 
      of.OrderID,
      of.LastEditedWhen,
      c.CustomerID
  ), customer_orders as (
  select 
    CustomerID,
    Recency,
    count(distinct OrderID) as total_orders,
    max(LastEditedWhen) as last_order_date
  from 
    base_query
  group by 
    CustomerID,
    Recency
  ),
  MaxDateCTE as (
    select MAX(LastEditedWhen) AS MaxDate
    from gold.orders_fact
    where year(LastEditedWhen) = 2019
  )
  ,period_1 as (
    select c.*
    from 
      customer_orders c
    cross join 
      MaxDateCTE M
    where c.last_order_date >= DATEADD(month, -6, M.MaxDate) and c.last_order_date < DATEADD(month, -3, M.MaxDate)
   ),
   period_2 as (
    select c.*
    from 
      customer_orders c
    cross join 
      MaxDateCTE M
    where c.last_order_date >= DATEADD(month, -3, M.MaxDate) and c.last_order_date <= M.MaxDate 
   ),
   Segment_History AS (
    SELECT 
        coalesce(p1.CustomerID, p2.CustomerID) AS CustomerID,
        -- Period 1: e.g., 6 months ago to 3 months ago
        CASE 
            WHEN p1.recency <= 30 AND p1.Total_Orders > 5 THEN 'Loyal'
            WHEN p1.recency > 90 THEN 'At Risk'
            ELSE 'Standard'
        END AS Segment_P1,
        -- Period 2: e.g., the last 3 months
        CASE 
        WHEN p2.recency <= 30 AND p2.Total_Orders > 5 THEN 'Loyal'
            WHEN p2.recency > 90 THEN 'At Risk'
            ELSE 'Standard'
        END AS Segment_P2
    FROM period_1 AS p1
    FULL JOIN period_2 AS p2 ON p1.CustomerID = p2.CustomerID
),
Migration_Counts AS (
    SELECT 
        Segment_P1, 
        Segment_P2, 
        COUNT(CustomerID) AS Customer_Count
    FROM Segment_History
    GROUP BY Segment_P1, Segment_P2
)
SELECT 
    Segment_P1 AS Starting_Segment,
    Segment_P2 AS Ending_Segment,
    Customer_Count,
    -- Calculation: (Moved to B) / (Total who started in A)
    CAST(Customer_Count AS FLOAT) / 
        SUM(Customer_Count) OVER(PARTITION BY Segment_P1) * 100 AS Migration_Rate_Pct
FROM Migration_Counts
ORDER BY Starting_Segment, Migration_Rate_Pct DESC

;;
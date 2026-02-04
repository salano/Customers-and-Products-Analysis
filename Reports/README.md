<h2>In this section we will implement two reports using the data we ingested using the ETL processes.</h2>

1. A Customers Report
2. A Products Report

# Customer Report

Purpose: - This report consolidates key customer metrics and behaviors

Highlights: 1. Gathers essential fields such as names, and transaction details. 2. Segments customers into categories (VIP, Regular, New) groups. 3. Aggregates customer-level metrics: - total orders - total revenue - total quantity purchased - total products - lifespan (in months) 4. Calculates valuable KPIs: - recency (months since last order) - average order value - average monthly spend

Customers Report SQL View

```
create view if not exists gold.customer_segment as(
with base_query as(
select
of.OrderID,
of.ProductID,
of.LastEditedWhen,
round((of.Quantity \* of.UnitPrice),2) as revenue,
of.Quantity,
c.skey as customer_key,
c.CustomerID,
concat(c.CustomerFirstName,' ',c.CustomerLastName) as customer_name

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

), customer_segment as (
select
customer_key,
CustomerID,
customer_name,
count(distinct OrderID) as total_orders,
round(sum(revenue),2) as total_revenue,
sum(Quantity) as total_quantity,
count(distinct ProductID) as total_products,
max(LastEditedWhen) as last_order_date,
min(LastEditedWhen) as first_order_date,
date_diff(MONTH, min(LastEditedWhen), max(LastEditedWhen)) as life_span

from
base_query
group by
customer_key,
CustomerID,
customer_name
)
select
customer_key,
CustomerID,
customer_name,
case when life_span >= 12 and total_revenue > 10000 then 'Loyal'
when life_span >= 12 and total_revenue <= 10000 then 'Regular'
else 'New'
end as customer_segment,
last_order_date,
datediff(month, last_order_date, getdate()) as recency,
total_orders,
total_revenue,
total_quantity,
total_products,
life_span,
--Average order value
case when total_orders > 0 then
round(total_revenue / total_orders, 2)
else 0
end as avg_order_value,
--Average monthly spend
case when life_span > 0 then
round(total_revenue / life_span,2)
else total_revenue
end as avg_monthly_spend
from
customer_segment
)

Output
![Alt text](Reports/customers_report1.png)
![Alt text](Reports/customers_report2.png)
![Alt text](Reports/customers_report3.png)

<b># Product Report </b>

Purpose: - This report consolidates key product metrics and behaviors.

Highlights: 1. Gathers essential fields such as product name, and unit price. 2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers. 3. Aggregates product-level metrics: - total orders - total sales - total quantity sold - total customers (unique) - lifespan (in months) 4. Calculates valuable KPIs: - recency (months since last order) - average order revenue (AOR) - average monthly revenue
```

Products Report SQL View

```

CREATE or replace VIEW gold.report_products AS

WITH base_query AS (
/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from gold.orders_facts and gold.products
---------------------------------------------------------------------------*/
    SELECT
	    of.OrderID,
        of.LastEditedWhen,
		    of.CustomerID,
        round(of.Quantity * of.UnitPrice,2) as revenue,
        of.Quantity,
        p.ProductID,
        p.ProductName,
        p.UnitPrice
    FROM gold.orders_fact of
    LEFT JOIN gold.products p
        ON of.ProductID = p.ProductID
    WHERE of.OrderID IS NOT NULL  -- only consider valid order dates
      AND p.active_flg = 1
),

product_aggregations AS (
/*---------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
---------------------------------------------------------------------------*/
SELECT
    ProductID,
    ProductName,
    UnitPrice,
    DATEDIFF(MONTH, MIN(LastEditedWhen), MAX(LastEditedWhen)) AS lifespan,
    MAX(LastEditedWhen) AS last_order_date,
    COUNT(DISTINCT OrderID) AS total_orders,
	COUNT(DISTINCT CustomerID) AS total_customers,
    round(SUM(Revenue),2) AS total_revenue,
    round(SUM(quantity),2) AS total_quantity,
	ROUND(AVG(CAST(revenue AS FLOAT) / NULLIF(quantity, 0)),1) AS avg_selling_price
FROM base_query

GROUP BY
    ProductID,
    ProductName,
    UnitPrice
)

/*---------------------------------------------------------------------------
  3) Final Query: Combines all product results into one output
---------------------------------------------------------------------------*/
SELECT
	ProductID,
    ProductName,
    UnitPrice,
	last_order_date,
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency_in_months,
	CASE
		WHEN total_revenue > 50000 THEN 'High-Performer'
		WHEN total_revenue >= 10000 THEN 'Mid-Range'
		ELSE 'Low-Performer'
	END AS product_segment,
	lifespan,
	total_orders,
	total_revenue,
	total_quantity,
	total_customers,
	avg_selling_price,
	-- Average Order Revenue (AOR)
	CASE
		WHEN total_orders = 0 THEN 0
		ELSE round(total_revenue / total_orders,2)
	END AS avg_order_revenue,

	-- Average Monthly Revenue
	CASE
		WHEN lifespan = 0 THEN total_revenue
		ELSE round(total_revenue / lifespan,2)
	END AS avg_monthly_revenue

FROM product_aggregations
```

Output
![Alt text](products_report1.png)
![Alt text](products_report2.png)
![Alt text](products_report3.png)

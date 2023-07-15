# Stores

```revenue_per_city
select
    location_name as city,
    concat('/stores/', location_name) as store_link,
    count(distinct customer_id) as customers,
    count(*) as orders,
    sum(order_total) as revenue_usd

from analytics.orders

group by 1, 2
```

Click a row to see the report for that store:
<DataTable data={revenue_per_city} link=store_link/>
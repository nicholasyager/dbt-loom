# Welcome to Jaffle Shop 🥪

```monthly_stats
with
monthly_stats as (
    select 
        date_trunc('month', ordered_at) as month,
        sum(order_total) as revenue_usd1k,
        count(*)::float as orders,
        count(distinct customer_id)::float as customers

    from analytics.orders
    group by month
    order by month desc
)

select 
    *,
    revenue_usd1k / (lag(revenue_usd1k, -1) over (order by month desc)) - 1 as revenue_growth_pct1,
    orders / (lag(orders, -1) over (order by month desc)) - 1 as order_growth_pct1,
    customers / (lag(customers, -1) over (order by month desc)) - 1 as customer_growth_pct1,
    monthname(month) as month_name
from monthly_stats
```

<BigValue
    data={monthly_stats}
    value=revenue_usd1k
    comparison=revenue_growth_pct1
    title="Monthly Revenue"
    comparisonTitle="vs. prev. month"
/>

<BigValue
    data={monthly_stats}
    value=orders
    comparison=order_growth_pct1
    title="Monthly Orders"
    comparisonTitle="vs. prev. month"
/>

Jaffle Shop locations served <Value data={monthly_stats} column=customers/> happy customers in <Value data={monthly_stats} column=month_name/>. This was a change of <Value data={monthly_stats} column=customer_growth_pct1/> from <Value data={monthly_stats} column=month_name row=1/>.

## Store Openings

```store_opening
with 
most_recent_open as (
  select
      location_name as opened_store,
      min(ordered_at) as opened_date_mmmyyyy,
      sum(order_total) as opened_revenue_usd
  from analytics.orders
  group by location_name
  order by opened_date_mmmyyyy desc
  limit 1
),

company_total as (
  select 
    sum(order_total) as company_revenue_usd,
  from analytics.orders
  cross join most_recent_open
  where ordered_at >= opened_date_mmmyyyy
)

select 
  *,
  opened_revenue_usd / company_revenue_usd as revenue_pct
from most_recent_open
cross join company_total
```

The most recent Jaffle Shop store opening was <Value data={store_opening} column=opened_store/> in <Value data={store_opening} column=opened_date_mmmyyyy/>. Since opening, <Value data={store_opening} column=opened_store/> has contributed <Value data={store_opening} column=revenue_pct/> of total company sales.

```orders_per_week
select
    date_trunc('week', ordered_at) as week,
    location_name,
    count(*) as orders,
    sum(order_total) as revenue_usd

from analytics.orders

group by 1,2
order by 1
```

<AreaChart
    data={orders_per_week}
    x=week
    y=revenue_usd
    yAxisTitle="revenue per week"
    series=location_name
    title="Weekly Revenue by Store Location"
    subtitle="Last 12 Months"
/>

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

## Reports on Individual Stores
Click a row to see the report for that store:
<DataTable data={revenue_per_city} link=store_link/>

## Seasonality
See [Seasonality Investigation](/analysis/seasonality-investigation) for more information.

## Customers
To see individual customer purchase history, see [Customers](/customers)

### Customer Cohorts
Average order values are tracked using monthly cohorts, which are created by truncating `first_order_date` to month. 

```customers_with_cohort
select
    *,
    date_trunc('month', first_ordered_at) as cohort_month,
    lifetime_spend_pretax / count_lifetime_orders as average_order_value_usd0

from analytics.customers
```

```cohorts_aov
select
    cohort_month,
    avg(average_order_value_usd0) as cohort_aov_usd

from ${customers_with_cohort}

group by 1
order by cohort_month
```

<BarChart
    data={cohorts_aov}
    x=cohort_month
    y=cohort_aov_usd
    yAxisTitle="average order value"
    xAxisTitle="Monthly Cohort"
    title="Customer AOV by first month cohort"
/>

### Average Order Values

<Histogram
    data={customers_with_cohort}
    x=average_order_value_usd0
    title="Distribution of AOVs"
    subtitle="Customer count"
    xAxisTitle=true
/>

# Seasonality Investigation
*Written by Melissa Cranston in September 2017*

*Analysis covers the time period of September 2016 to August 2017. All queries have been limited to that range.*

[Jump to conclusions & recommendations &darr;](#Conclusions5)

## Variations in Order Volume
Plotting orders per day for the last 12 months reveals 3 things:
- An unnaturally large jump in orders per day in March 2017 - this was driven by the new store opening in [Brooklyn](/stores/Brooklyn)
- A repeating pattern of spikes which might be driven by different order volumes on specific days of the week
- A drop in total orders per day around June 2017

```orders_per_day
select
    date_trunc('day', ordered_at) as date,
    count(*) as orders

from analytics.orders
where ordered_at between '2016-09-01' and '2017-08-31'

group by 1
order by 1
```

<LineChart
    data={orders_per_day}
    x=date
    y=orders
    yAxisTitle="orders per day"
    title="Orders per Day"
/>

## Day of Week
We can calculate average orders by day of week to check if there are differences in order volume across days. 

```orders_by_weekday
select
    date_part('dayofweek', date) as day_of_week_num,
    dayname(date) as day_of_week,
    avg(orders) as avg_orders
from ${orders_per_day}
group by 1, 2
order by day_of_week_num
```

<BarChart
    data={orders_by_weekday}
    x=day_of_week
    y=avg_orders
    swapXY=true
    title="Average Orders by Day of Week"
    yAxisTitle="Avg Orders Per Day"
/>

This reveals that weekdays generate significantly higher order volume than weekends. It also shows that orders are fairly consistent across individual days on weekdays (202-209 orders/day) and weekends (~50 orders/day).

## Hour of Day
Now we'll break down orders by hour of day to see if there are patterns within days. Given the differences we just found between weekday and weekend volumes, we should split the results by those day types. We can use a loop for this.

```orders_hour_of_day
with
    orders_by_hour as (
        select
            date_part('hour', ordered_at) as hour_of_day,
            if(dayname(ordered_at) in ('Sunday', 'Saturday'), 'Weekend', 'Weekday') as day_type,
            count(*)::float as orders,
            count(distinct date_trunc('day', ordered_at)) as days
        from analytics.orders
        where ordered_at between '2016-09-01' and '2017-08-31'
        group by 1, 2
        order by hour_of_day
    )

select
    *,
    orders / days as orders_per_hour
from orders_by_hour
```

{#each ['Weekday', 'Weekend'] as day_type}

<BarChart
    data={orders_hour_of_day.filter(d => d.day_type === day_type)}
    x=hour_of_day
    y=orders_per_hour
    yAxisTitle=true
    xAxisTitle=true
    yMax=60
    title="{day_type} - Orders by Hour of Day"
/>

{/each}

We see a significant peak in order volume between 7 and 9am on weekdays. There is also a slight increase in volume around lunch times (12-2pm) across all days of the week.

## Dayparts
Based on the volumes shown above, we can break down our dayparts as:
- Breakfast: 7-9am
- Late Morning: 9am-12pm
- Lunch: 12-2pm
- Late Afternoon: 2-5pm

In future analyses, these timeframes should be lined up with any existing operational timeframes (e.g., breakfast, lunch service windows).

```dayparts
with
    orders_add_daypart as (
        select
            *,
            case
                when hour_of_day between 7 and 8 then 'Breakfast'
                when hour_of_day between 9 and 11 then 'Late Morning'
                when hour_of_day between 12 and 14 then 'Lunch'
                when hour_of_day between 15 and 24 then 'Late Afternoon'
            end as daypart
        from ${orders_hour_of_day}
    ),

    orders_by_daypart as (
        select
            daypart,
            day_type,
            sum(orders) / sum(days) as orders_per_hour,
            sum(orders) as orders
        from orders_add_daypart
        group by daypart, day_type
    )

    select
        *,
        orders / sum(orders) over () as orders_pct1
    from orders_by_daypart
```

<BarChart
    data={dayparts}
    x=daypart
    y=orders_pct1
    series=day_type
    yAxisTitle="% of Total Orders"
    title="Breakdown of Orders by Daypart"
    swapXY=true
/>

Almost half of all orders are generated from breakfast on weekdays. This might be driven by orders from customers who are on their way to work - a follow-up analysis on customer purchasing behaviour should be completed to investigate this.

## Conclusions
- Weekdays generate significantly more orders than weekend days (~4x more orders on an average weekday compared to an average weekend day)
- Early mornings (7-9am) on weekdays generate almost half of all orders for the company
- There was a drop in orders in June 2017 - this has not been covered in this analysis, but should be investigated

### Recommended Follow-on Analyses
- Investigate drop in orders in June 2017
- Study customer purchasing behaviour, especially during weekday early mornings
- Extend this analysis with a longer timeframe to investigate seasonality throughout the calendar year


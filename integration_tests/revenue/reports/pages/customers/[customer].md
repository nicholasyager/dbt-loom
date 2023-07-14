# {$page.params.customer}'s Customer Profile

```customers
select 
    *,
    first_ordered_at as first_order_longdate,
    last_ordered_at as last_order_longdate,
    lifetime_spend as lifetime_spend_usd,
    lifetime_spend / count_lifetime_orders as average_order_value_usd
from analytics.customers
```

{$page.params.customer} has been a customer since <Value data={customers.filter(d => d.customer_name === $page.params.customer)} column=first_order_longdate/>, with their most recent order occurring on <Value data={customers.filter(d => d.customer_name === $page.params.customer)} column=last_order_longdate/>.

### Key stats:
- <Value data={customers.filter(d => d.customer_name === $page.params.customer)} column=count_lifetime_orders/> lifetime orders
- <Value data={customers.filter(d => d.customer_name === $page.params.customer)} column=lifetime_spend_usd/> in lifetime spend
- <Value data={customers.filter(d => d.customer_name === $page.params.customer)} column=average_order_value_usd/> average order value

```monthly_purchases
select
    date_trunc('month', a.ordered_at) as month,
    b.customer_name,
    sum(a.order_total) as purchases_usd
from analytics.orders a
left join analytics.customers b
on a.customer_id = b.customer_id
group by month, customer_name
order by month asc
```

<BarChart 
    data={monthly_purchases.filter(d => d.customer_name === $page.params.customer)} 
    x=month
    y=purchases_usd
    title="Purchases per Month by {$page.params.customer}"
/>
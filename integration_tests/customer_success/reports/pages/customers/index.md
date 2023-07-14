# Customers

```customers
select 
    customer_name, 
    concat('/customers/', customer_name) as customer_link,
    count_lifetime_orders as lifetime_orders, 
    lifetime_spend as lifetime_spend_usd,
    lifetime_spend / count_lifetime_orders as average_order_value_usd
from analytics.customers
order by lifetime_spend_usd desc
```

Click a row to see the report for that customer:
<DataTable data={customers} search=true link=customer_link showLinkCol=false>
    <Column id=customer_name/>
    <Column id=lifetime_orders/>
    <Column id=lifetime_spend_usd/>
    <Column id=average_order_value_usd/>
</DataTable>
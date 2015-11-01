* Write SQL statement for following data retrieval task:

List distinct customers’ name (including first name and last name) who live in California (i.e., CA) but not in the city whose name begin with San. (40%)

```sql
select distinct customer_fname, customer_lname from customers where customer_state = 'CA' and (customer_city not like
'San %')
```

List order id which is on-progress. (i.e., not close, not complete) (20%)
```sql
select order_id from orders where order_status <> 'CLOSED' and order_status <> 'COMPLETE'
```

Use the keyword “Between” list products’ name whose price is greater than 100 and less 150 (except 100, 150) (40%)
```sql
select product_name from products where (product_price not between 99 and 100) and (product_price between 100
and 150) and (product_price not between 150 and 151)
```

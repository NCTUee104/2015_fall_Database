1.(20%) List number of customers of every state

```sql
select count(customer_id) from customers group by customer_state
```

2.(20%) List number of customers of every order status

```sql
select count(order_customer_id) from orders group by order_status
```

3.(20%) List product id and its total sales value (sales value = price * quantity) (hint: see table order_items)

```sql

```

4.(20%) List distinct customers’ name (combine first name and last name as single column) who live in California (i.e., CA) but not in the city whose name begin with San.

```sql
select distinct concat(customer_fname,' ' ,customer_lname) from customers where customer_state = 'CA' and (customer_city not like 'San%')
```

5.(10%) List number of orders which are on-progress. (i.e., not closed, not complete)

```sql
select count(order_id) from orders where order_status <> "CLOSED" and order_status <> "COMPLETE"
```

6.(10%) Use uppercase to show all products’ name whose price is greater than 100 and less 150 (except 100, 150)

```sql
select ucase(product_name) from products where product_price > 100 and product_price < 150
```

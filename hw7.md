寫一個名為"Retail_order_學號" 的class:
* (10%) 1.具有一個名為HCon的Configuration型態的屬性，一個名為MysqlAccount的String型態的屬性，一個名為MysqlPW的String型態的屬性　
* (10%) 2.建構子會要求一個Configuration型態的input (交給HCon)，一個String型態input(交給MysqlAccount)，一個String型態input(交給MysqlPW)
* (10%) 3.宣告一個convertH() method把MySQL中的retail_db中的products, order_items, orders三個table join起來並寫進HBase（table名稱：retail_order、family名稱用原MySQL中的table名稱、欄位名稱用原MySQL中的欄位名稱、rowkey用order_items中的primary key)

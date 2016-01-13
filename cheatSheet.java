// find all jars : Libraries -> add External JARs -> FileSystem/usr/jars
/** 型別轉換 */
int Integer.parseInt(); // 轉成Int
byte[] Bytes.toBytes();  // 轉成Bytes[]
int Bytes.toInt(byte[] b);   // Bytes[]轉成int
String toString(byte[] b);   // Bytes[]轉成String    

/** String */
boolean	equals(Object anObject) // 是否相等
String substring(int beginIndex, int endIndex) // eg. "abcd" -> substring(0, 2) = ab
String[] split(String regex) // 分割字串
String.valueOf();  // 轉成String

/** Regex */
.         any character except newline
\w \d \s  word, digit, whitespace
\W \D \S  not word, digit, whitespace
[abc]     any of a, b, or c
[^abc]    not a, b, or c
[a-g]     character between a & g
\. \* \\  escaped special characters
(abc)     capture group
ab|cd     match ab or cd
a* a+ a?  0 or more, 1 or more, 0 or 1

/** MapReduce */
extends TableMapper<KEYOUT,VALUEOUT>
@override map(ImmutableBytesWritable row, Result col, Context context)
extends TableReducer<KEYIN,VALUEIN,KEYOUT>
@override reduce(Text key, Iterable<IntWritable> values, Context context)
context.write(outputKey, outputValue); 
TableMapReduceUtil.initTableMapperJob(String table, Scan scan, Class<? extends TableMapper> mapper, Class<?> outputKeyClass, Class<?> outputValueClass, org.apache.hadoop.mapreduce.Job job)
TableMapReduceUtil.initTableReducerJob(String table, Class<? extends TableReducer> reducer, org.apache.hadoop.mapreduce.Job job)

/** IntWritable */
int get() // Return the value of this IntWritable

/** ArrayList */
boolean	add(E e) // Appends the specified element to the end of this list
boolean	contains(Object o) // Returns true if this list contains the specified element
E get(int index) // Returns the element at the specified position in this list
int	indexOf(Object o) // Returns the index of the first occurrence of the specified element in this list, or -1 if this list does not contain the element
int	size() // Returns the number of elements in this list

/** hbase shell */
list // 列出所有table
// 如果發生table已經存在的問題，就要把table刪掉
disable 'tableName'
drop 'tableName'
get 'tableName', 'rowKey' // 回傳此table中某row的值 可用來檢查table裡面的值是否正常 

/** Scan */
Scan scan = new Scan(); // Scan() : Create a Scan operation across all rows
scan.addFamily(byte[] family); // Get all columns from the specified family
scan.addColumn(byte[] family, byte[] qualifier) // Get the column from the specified family with the specified qualifier
ResultScanner rs = table.getScanner(scan);
for(Result r : rs)

/** Filter Scan */
Scan scan = new Scan();
Filter f = new ValueFilter(CompareOp.EQUAL, new RegexStringComparator("Regular_Expression"));
scan.setFilter(f);
ResultScanner rs = table.getScanner(scan);

/** Put */
Put put = new Put(byte[] row); // Create a Put operation for the specified row
put.addColumn(byte[] family, byte[] qualifier, byte[] value); // Add the specified column and value to this Put operation
table.put(put);

/** Delete */
Delete del = new Delete(byte[] row);
del.addColumn(byte[] family, byte[] qualifier); 
table.delete(del);

/** Result */
byte[] value = result.getValue(byte[] family, byte[] qualifier); // Get the latest version of the specified column
byte[] rowKey = result.getRow() // Retrieving the row key that corresponds to the row from which this Result was created

/** Admin : create, drop, list, enable and disable tables, add and drop table column families */
Admin admin = Connection.getAdmin();
admin.addColumnFamily(TableName tableName, HColumnDescriptor columnFamily); // Add a column family to an existing table
admin.deleteColumnFamily(TableName tableName, byte[] columnFamily); // Delete a column family from a table
admin.disableTable(TableName tableName); // Disable table and wait on completion
admin.deleteTable(TableName tableName); // Deletes a table
admin.close();
admin.createTable(HTableDescriptor desc); // Creates a new table, below is example
HTableDescriptor tableDesc = new HTableDescriptor(TableName name); // Construct a table descriptor with TableName
HColumnDescriptor colDesc = new HColumnDescriptor(String familyName); // Construct a column descriptor specifying only the family name
tableDesc.addFamily(HColumnDescriptor family); // Adds a column family
admin.createTable(HTableDescriptor desc);

/** SQL */
SELECT COUNT(column_name) FROM table_name; // returns the number of values (NULL values will not be counted) of the specified column
SELECT COUNT(*) FROM table_name; // returns the number of records in a table
SELECT COUNT(DISTINCT column_name) FROM table_name; // returns the number of distinct values of the specified column
SELECT column_name(s)
FROM table1
INNER JOIN table2
ON table1.column_name=table2.column_name; // selects all rows from both tables as long as there is a match between the columns in both tables
SELECT column_name(s)
FROM table1
LEFT JOIN table2
ON table1.column_name=table2.column_name; // returns all rows from the left table (table1), with the matching rows in the right table (table2)
SELECT column_name(s)
FROM table_name
WHERE column_name BETWEEN value1 AND value2; //  selects values within a range. The values can be numbers, text, or dates.
SELECT column_name(s)
FROM table_name
WHERE column_name LIKE pattern; // search for a specified pattern in a column
DELETE FROM table_name
WHERE some_column=some_value; // delete rows in a table
UPDATE table_name
SET column1=value1,column2=value2,...
WHERE some_column=some_value; // update existing records in a table
SELECT column_name, column_name
FROM table_name
ORDER BY column_name ASC|DESC, column_name ASC|DESC; // ORDER BY keyword sorts the records in ascending order by default
SELECT column_name,column_name
FROM table_name
WHERE column_name operator value; // extract only those records that fulfill a specified criterion
SELECT DISTINCT column_name,column_name
FROM table_name; // return only distinct (different) values
SELECT MAX(column_name) FROM table_name; // returns the largest value of the selected column
SELECT SUM(column_name) FROM table_name; // returns the total sum of a numeric column
SELECT column_name, aggregate_function(column_name)
FROM table_name
WHERE column_name operator value
GROUP BY column_name; // conjunction with the aggregate functions to group the result-set by one or more columns




SELECT DISTINCT column_name,column_name FROM table_name; // 

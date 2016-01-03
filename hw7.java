import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

//import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class Retail_order_0450742 {
	
	private static Configuration HCon;
	private String MysqlAc;
	private String MysqlPw;
	private static Connection con_h;
	private static java.sql.Connection con_sql;
	
	Retail_order_0450742(Configuration conf, String Ac, String Pw) {
		HCon = conf;
		MysqlAc = Ac;
		MysqlPw = Pw;
	}

	public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, IOException {
		//System.out.printf("test");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost"); 
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Retail_order_0450742 test = new Retail_order_0450742(conf, "root", "cloudera");
		test.convertH();
	}

	public void convertH() throws SQLException, IOException, ClassNotFoundException, IllegalAccessException, InstantiationException{
		/** create HBase connection */
		try {
			con_h =  ConnectionFactory.createConnection(HCon);
		} catch (IOException e) {
			e.printStackTrace();
		}
		/** create Mysql connection */
		try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            // con_sql = DriverManager.getConnection("jdbc:mysql:///retail_db", "root", "cloudera");
            con_sql = DriverManager.getConnection("jdbc:mysql:///retail_db", MysqlAc, MysqlPw);
            if (!con_sql.isClosed()) { 
                System.out.println("Successfully connected to MySQL server...");
            }
		} catch (Exception e) {
            System.err.println(e.getMessage());
        }
		/** create table and family */
		// Admin : create, drop, list, enable and disable tables, add and drop table column families
		Admin admin = con_h.getAdmin();
		// create Table Name
		TableName tableName = TableName.valueOf("retail_order");
		HTableDescriptor tableDesc = new HTableDescriptor(tableName); // details about an HBase table 
		// create Family Name
		String[] familyName = {"order_items", "orders", "products"};
		for(String name : familyName) {
			HColumnDescriptor colDesc = new HColumnDescriptor(name); // details about a column family
			tableDesc.addFamily(colDesc);
		}
		admin.createTable(tableDesc);
		
		/** query from mysql */
		if (con_sql != null) {
            Statement stat = con_sql.createStatement();
            stat.executeQuery("select * from products as p join order_items as i on p.product_id = i.order_item_product_id join orders as o on o.order_id = i.order_item_order_id order by order_item_id");
            ResultSet rs = stat.getResultSet();
        	while (rs.next()) {
            	// order_items
            	String rowKey = rs.getString("order_item_id");
            	Put put = new Put(Bytes.toBytes(rowKey));
            	String order_item_order_id = rs.getString("order_item_order_id");
            	put.addColumn(Bytes.toBytes("order_items"), Bytes.toBytes("order_item_order_id"), Bytes.toBytes(order_item_order_id));
            	String order_item_product_id = rs.getString("order_item_product_id");
            	put.addColumn(Bytes.toBytes("order_items"), Bytes.toBytes("order_item_product_id"), Bytes.toBytes(order_item_product_id));
            	String order_item_quantity = rs.getString("order_item_quantity");
            	put.addColumn(Bytes.toBytes("order_items"), Bytes.toBytes("order_item_quantity"), Bytes.toBytes(order_item_quantity));
            	String order_item_subtotal = rs.getString("order_item_subtotal");
            	put.addColumn(Bytes.toBytes("order_items"), Bytes.toBytes("order_item_subtotal"), Bytes.toBytes(order_item_subtotal));
            	String order_item_product_price = rs.getString("order_item_product_price");
            	put.addColumn(Bytes.toBytes("order_items"), Bytes.toBytes("order_item_product_price"), Bytes.toBytes(order_item_product_price));
            	// orders
            	String order_id = rs.getString("order_id");
            	put.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("order_id"), Bytes.toBytes(order_id));
            	String order_date = rs.getString("order_date");
            	put.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("order_date"), Bytes.toBytes(order_date));
            	String order_customer_id = rs.getString("order_customer_id");
            	put.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("order_customer_id"), Bytes.toBytes(order_customer_id));
            	String order_status = rs.getString("order_status");
            	put.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("order_status"), Bytes.toBytes(order_status));
            	// products
            	String product_id = rs.getString("product_id");
            	put.addColumn(Bytes.toBytes("products"), Bytes.toBytes("product_id"), Bytes.toBytes(product_id));
            	String product_category_id = rs.getString("product_category_id");
            	put.addColumn(Bytes.toBytes("products"), Bytes.toBytes("product_category_id"), Bytes.toBytes(product_category_id));
            	String product_name = rs.getString("product_name");
            	put.addColumn(Bytes.toBytes("products"), Bytes.toBytes("product_name"), Bytes.toBytes(product_name));
            	String product_description = rs.getString("product_description");
            	put.addColumn(Bytes.toBytes("products"), Bytes.toBytes("product_description"), Bytes.toBytes(product_description));
            	String product_price = rs.getString("product_price");
            	put.addColumn(Bytes.toBytes("products"), Bytes.toBytes("product_price"), Bytes.toBytes(product_price));
            	String product_image = rs.getString("product_image");
            	put.addColumn(Bytes.toBytes("products"), Bytes.toBytes("product_image"), Bytes.toBytes(product_image));
            	// put in table
            	Table table = con_h.getTable(tableName);
            	table.put(put); 
            	table.close();
            }
            rs.close();
            stat.close();
		}
		con_h.close();
		con_sql.close();
	}
	
}

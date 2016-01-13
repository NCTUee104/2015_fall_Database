import java.io.IOException;
//import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

import umontreal.iro.lecuyer.probdist.ChiSquareDist;
 
class CellOfHTable{
	int category_id;
	int countY;
	int countN;
	CellOfHTable(byte[] category_id,byte[] countY, byte[] countN){
		this.category_id = Bytes.toInt(category_id);
		this.countY = Bytes.toInt(countY);
		this.countN = Bytes.toInt(countN);
	}
}
public class ChiSquaredTest2_abc {
	private double alpha;
	private static Configuration conf_h;
	private String MysqlAc;
	private String MysqlPw;
	private static Connection conn_h;
	private static java.sql.Connection conn_sql;
	private TableName tableName_retail = TableName.valueOf("retail_order");
	private TableName tableName_chi = TableName.valueOf("chiTable");
	
	public void convertH() throws SQLException, IOException, ClassNotFoundException, IllegalAccessException, InstantiationException{
		/** create HBase connection */
		conf_h = HBaseConfiguration.create();
		conf_h.set("hbase.zookeeper.quorum", "localhost"); 
		conf_h.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			conn_h =  ConnectionFactory.createConnection(conf_h);
		} catch (IOException e) {
			e.printStackTrace();
		}
		/** create Mysql connection */
		try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            // con_sql = DriverManager.getConnection("jdbc:mysql:///retail_db", "root", "cloudera");
            conn_sql = DriverManager.getConnection("jdbc:mysql:///retail_db", "root", "cloudera");
            if (!conn_sql.isClosed()) { 
                System.out.println("Successfully connected to MySQL server...");
            }
		} catch (Exception e) {
            System.err.println(e.getMessage());
        }
		/** create table and family */
		Admin admin = conn_h.getAdmin();
		
		HTableDescriptor tableDesc = new HTableDescriptor(tableName_retail);
		// create Family Name
		String[] familyName = {"order_items", "orders", "products"};
		for(String name : familyName) {
			HColumnDescriptor colDesc = new HColumnDescriptor(name);
			tableDesc.addFamily(colDesc);
		}
		admin.createTable(tableDesc);
		
		/** query from mysql */
		if (conn_sql != null) {
            		Statement stat = conn_sql.createStatement();
            		stat.executeQuery("select * from products "+
					   "inner join order_items on product_id = order_item_product_id "+
					   "inner join orders on order_item_order_id = order_id ");
            		ResultSet rs = stat.getResultSet();
        	while (rs.next()) {
            	// order_items
            	String rowKey = rs.getString("order_item_id");
            	Put put = new Put(Bytes.toBytes(rowKey)); // new Put(rowKey)
            	put.addColumn(Bytes.toBytes("order_items"), Bytes.toBytes("order_item_id"), Bytes.toBytes(rowKey));
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
            	Table table = conn_h.getTable(tableName_retail);
            	table.put(put); 
            	table.close();
            }
            rs.close();
            stat.close();
		}
		conn_h.close();
		conn_sql.close();
	}
	public static class Map1 extends TableMapper<Text, IntWritable> { // TableMapper<KEYOUT,VALUEOUT>
		@Override
	    protected void map(ImmutableBytesWritable rowkey, Result result, Context context) throws IOException, InterruptedException {
	    	byte[] c = result.getValue(Bytes.toBytes("products"), Bytes.toBytes("product_category_id"));
	    	byte[] d = result.getValue(Bytes.toBytes("orders"), Bytes.toBytes("order_date"));
	    	String thg = "N";
	    	String[] dStr = Bytes.toString(d).split("\\W+");
	    	if(dStr[1].equals("11")){
	    		int date = Integer.parseInt(dStr[2]);
	    		if(date>=21) thg = "Y";
	    	}
	        context.write(new Text(Bytes.toString(c)+":"+thg), new IntWritable(1)); // Generate an output key/value pair.
	        // context.write(outputKey, outputValue) -> (123:Y, 1), (124:N, 1) 
	    }
	  }
	  public static class Reduce1 extends TableReducer<Text, IntWritable, ImmutableBytesWritable> { // TableReducer<KEYIN,VALUEIN,KEYOUT>
	    // reduce(Object, Iterable, Context) method is called for each <key, (collection of values)> in the sorted inputs
		  @Override
	    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    	// Iterable : allows an object to be the target of the "foreach" statement
	    	int sum = 0;
	    	for(IntWritable val : values) {
	            sum += val.get();
	        }
	    	String[] keyStr = key.toString().split(":");
	    	Put p = new Put(Bytes.toBytes(Integer.parseInt(keyStr[0]))); // keyStr[0] = id as row
	        p.addColumn(Bytes.toBytes("count"), Bytes.toBytes(keyStr[1]), Bytes.toBytes(sum));
	        // Put addColumn(byte[] family, byte[] qualifier, byte[] value)
	        // count as family, Y/N as qualifier, sum as value
	        context.write(new ImmutableBytesWritable(p.getRow()), p);
	    }
	  }

	public ChiSquaredTest2_abc(double alpha) {
		if(alpha > 1 || alpha < 0){
			System.out.println("Alpha should be between 0 and 1!");
		}else{
			this.alpha = alpha;
		}
	}
	public void setAlpha(double alpha){
		if(alpha > 1 || alpha < 0){
			System.out.println("Alpha should be between 0 and 1!");
		}else{
			this.alpha = alpha;
		}
	}
	public boolean doTest() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException, InterruptedException{
		/** create config */
		conf_h = HBaseConfiguration.create();
		conf_h.set("hbase.zookeeper.quorum", "localhost"); 
		conf_h.set("hbase.zookeeper.property.clientPort", "2181");
		Connection con_h = null;
		try {
			con_h = ConnectionFactory.createConnection(conf_h);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Admin admin = con_h.getAdmin();
		HTableDescriptor tableDesc = new HTableDescriptor(tableName_chi);
		HColumnDescriptor colFamDesc = new HColumnDescriptor("count");
		colFamDesc.setMaxVersions(1);
		tableDesc.addFamily(colFamDesc);
		admin.createTable(tableDesc);
		
		/** counting and insert in chiTable */
		Scan scan = new Scan();
	    scan.addColumn(Bytes.toBytes("products"), Bytes.toBytes("product_category_id"));
	    scan.addColumn(Bytes.toBytes("orders"), Bytes.toBytes("order_date"));
	    
		Job job = Job.getInstance(conf_h, "Count"); // getInstance(Configuration conf, String jobName)
	    job.setJarByClass(ChiSquaredTest2_abc.class); // Set the Jar by finding where a given class came from
	    // initTableMapperJob(String table, Scan scan, Class<? extends TableMapper> mapper, Class<?> outputKeyClass, Class<?> outputValueClass, org.apache.hadoop.mapreduce.Job job)
	    TableMapReduceUtil.initTableMapperJob(
	      "retail_order",
	      scan,
	      Map1.class,
	      Text.class,
	      IntWritable.class,
	      job);
	    // initTableReducerJob(String table, Class<? extends TableReducer> reducer, org.apache.hadoop.mapreduce.Job job)
	    TableMapReduceUtil.initTableReducerJob(
	      "chiTable",
	      Reduce1.class,
	      job);
		
	    // boolean waitForCompletion(boolean verbose), verbose - print the progress to the user
	    job.waitForCompletion(true);// Submit the job to the cluster and wait for it to finish
	    
	    /** extract value from chiTable */
		int totalY=0;
		int totalN=0;
		ArrayList<CellOfHTable> chiTable = new ArrayList<CellOfHTable>();
		Table table_h = con_h.getTable(tableName_chi);
		Scan s = new Scan();
	    s.addFamily(Bytes.toBytes("count"));
	    ResultScanner results = table_h.getScanner(s);
	    for(Result r : results) {
	    	CellOfHTable c = new CellOfHTable(
	    			r.getRow(),
	    			r.getValue(Bytes.toBytes("count"), Bytes.toBytes("Y")) == null
	    				? Bytes.toBytes(0)
	    				: r.getValue(Bytes.toBytes("count"), Bytes.toBytes("Y")),
	    			r.getValue(Bytes.toBytes("count"), Bytes.toBytes("N")) == null
	    				? Bytes.toBytes(0)
	    				: r.getValue(Bytes.toBytes("count"), Bytes.toBytes("N"))
	    			); // (id, count_Y, count_N)
	    	chiTable.add(c);
	    	totalY = totalY + c.countY;
	    	totalN = totalN + c.countN;
	    }
	    
	    results.close();
	    table_h.close();
		admin.disableTable(tableName_chi);
		admin.deleteTable(tableName_chi);
		
		double chisquare = 0.0;
		for(int i=0;i<chiTable.size();i++){
			CellOfHTable c = chiTable.get(i);
			double expectY = (double)(c.countY+c.countN)* (double)totalY / (double)(totalY+totalN);
			chisquare = chisquare + (((double)c.countY - expectY)*((double)c.countY - expectY) / expectY) ;
			double expectN = (double)(c.countY+c.countN)* (double)totalN / (double)(totalY+totalN);
			chisquare = chisquare + (((double)c.countN - expectN)*((double)c.countN - expectN) / expectN) ; 
		}
		
		System.out.println(chisquare);
        ChiSquareDist csd = new ChiSquareDist((chiTable.size()-1));
        if (chisquare > csd.inverseF(1.0-alpha)) {
        	return true;
        } 
        return false;
	}
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException, InterruptedException {
		
		ChiSquaredTest2_abc a=new ChiSquaredTest2_abc(0.05);
		a.convertH();
		boolean answer=a.doTest();
		if(answer==true){
			System.out.println("H0 is rejected!");
		}else{
			System.out.println("H0 is accepted!");
		}
		a.setAlpha(0.999999);
		answer=a.doTest();
		if(answer==true){
			System.out.println("H0 is rejected!");
		}else{
			System.out.println("H0 is accepted!");
		}
	}
}

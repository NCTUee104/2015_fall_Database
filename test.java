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
	double alpha;
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
		//create config
		Configuration conf_h = HBaseConfiguration.create();
		conf_h.set("hbase.zookeeper.quorum", "localhost"); 
		conf_h.set("hbase.zookeeper.property.clientPort", "2181");
		Connection con_h = null;
		try {
			con_h = ConnectionFactory.createConnection(conf_h);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Admin admin = con_h.getAdmin();
		TableName tableName = TableName.valueOf("chiTable");
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		HColumnDescriptor colFamDesc = new HColumnDescriptor("count");
		colFamDesc.setMaxVersions(1);
		tableDesc.addFamily(colFamDesc);
		admin.createTable(tableDesc);
		
		//counting and insert in chiTable
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
	    // System.exit(job.waitForCompletion(true) ? 0 : 1);

	    //extract value from chiTable
		int totalY=0;
		int totalN=0;
		ArrayList<CellOfHTable> chiTable = new ArrayList<CellOfHTable>();
		Table table_h = con_h.getTable(tableName);
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
		//drop chiTable
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		
		//calculating chisqure statistic
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

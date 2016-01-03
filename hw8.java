package hw8;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import umontreal.iro.lecuyer.probdist.ChiSquareDist;

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

public class ChiSquaredTest2_0450742 {
	private double alpha;
    private int df;
    private double x; 

	public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {
		ChiSquaredTest2_0450742 test = new ChiSquaredTest2_0450742(0.5);
		System.out.println(test.getX());
		System.out.println(test.doTest());
	}
	
    ChiSquaredTest2_0450742(double a) throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {
        if (a <= 0 || a >= 1 ) {
            System.out.println("alpha must > 0 or < 1");
        } else {
            this.alpha = a;
        }
        Configuration conf =  HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost"); 
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection conn = null;
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		TableName tableName = TableName.valueOf("retail_order");
		Table table = conn.getTable(tableName);

		/** calculate row length  */
		//scan.addColumn(Bytes.toBytes("products"), Bytes.toBytes("product_category_id")); // Get the column from the specified family with the specified qualifier.
		Scan scan1 = new Scan(); // Create a Scan operation across all rows.
		//ResultScanner rs = table.getScanner(Bytes.toBytes("products"), Bytes.toBytes("product_category_id"));
		ResultScanner rs = table.getScanner(scan1);
		int max = 0;
		for (Result r : rs) {
			String pid_tmp = Bytes.toString(r.getValue(Bytes.toBytes("products"), Bytes.toBytes("product_category_id")));
			int pid_value = Integer.valueOf(pid_tmp);
			if (pid_value > max) {
				max = pid_value;
			}
		}
		int row = max;
		// 59 275.38559275710566
		// max 48 275.38559275710566
		
		// 11-21 ~ 11-31
        List<String> date_thank = Arrays.asList("11-21", "11-22", "11-23", "11-24", "11-25", "11-26", "11-27", "11-28", "11-29", "11-30");
        //String[] thank = new String[9];
		int col = date_thank.size();
		double[][] observ = new double[row + 1][col + 1]; 
        for (int i = 0; i < row + 1; i++) {
            for (int j = 0; j < col + 1; j++) {
                observ[i][j] = 0.0;
            }
        }
        this.df = (row - 1) * (col - 1); // calculate degree of freedom

        /** p_id  */
		Scan scan = new Scan(); 
		rs = table.getScanner(scan);
		for (Result r : rs) {
			String pid = Bytes.toStringBinary(r.getValue(Bytes.toBytes("products"), Bytes.toBytes("product_category_id")));
			int idx_pid = Integer.valueOf(pid);
			String date = Bytes.toString(r.getValue(Bytes.toBytes("orders"), Bytes.toBytes("order_date"))); // 2013-07-25 00:00:00.0
			if (date_thank.contains(date.substring(5, 10))) { // substring(int beginIndex, int endIndex) : Returns a new string that is a substring of this string.
				int idx_date = date_thank.indexOf(date.substring(5, 10)); 
				observ[idx_pid][idx_date]++;
			}
		}
		rs.close();
		int row_sum = 0;
        for (int i = 1; i < row + 1; i++) {
            for (int j = 1; j < col + 1; j++) {
                row_sum += observ[i][j];
            }
            observ[i][0] = row_sum;
            row_sum = 0;
        }
        int col_sum = 0;
        for (int i = 1; i < col + 1; i++) {
            for (int j = 1; j < row + 1; j++) {
                col_sum += observ[j][i];
            }
            observ[0][i] = col_sum;
            col_sum = 0;
        }
        
        double sum = 0;
        for (int i = 1; i < row + 1; i++) {
            sum += observ[i][0]; // same as observ[0][i]
        }

        this.x = 0;
        for (int i = 1; i < row + 1; i++) {
            for (int j = 1; j < col + 1; j++) {
                if (observ[i][0] == 0 || observ[0][j] == 0) { 
                    this.x += 0;
                } else { // 
                    this.x += Math.pow(Math.abs(observ[i][j] - (observ[i][0] * observ[0][j] / sum)), 2) / (observ[i][0] * observ[0][j] / sum);
                } 
            }
        }
		table.close();
		conn.close();
		
    }
	public final boolean doTest() {
        boolean h0;
        ChiSquareDist csd = new ChiSquareDist(this.df);
        if (this.x > csd.inverseF(1 - this.alpha)) {
            h0 = true; // reject h0
        } else {
            h0 = false; // accept h0
        }
        return h0;
    }

    public final void setAlpha(double a) {
        if (a <= 0 || a >=1 ) {
            System.out.println("alpha must > 0 or < 1");
        } else {
            this.alpha = a;
        }
    }
    
    public final double getX() {
    	return this.x;
    }
}

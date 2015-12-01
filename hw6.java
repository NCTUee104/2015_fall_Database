
/**
 * Created by Edward on 2015/11/21.
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.HashMap;
import umontreal.iro.lecuyer.probdist.ChiSquareDist;

/**
 * The type Chi squared test 0450742.
 */
public class ChiSquaredTest_0450742 {

    private double alpha;
    private int df;
    private double x;

    /**
     * Instantiates a new Chi squared test 0450742.
     *
     * @param a the a
     * @throws ClassNotFoundException the class not found exception
     * @throws IllegalAccessException the illegal access exception
     * @throws InstantiationException the instantiation exception
     * @throws SQLException           the sql exception
     */
    ChiSquaredTest_0450742(double a) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        Connection con = null;
        if (a <= 0 || a >=1 ) {
            System.out.println("alpha must > 0 or < 1")
        } else {
            this.alpha = a;
        }
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            con = DriverManager.getConnection("jdbc:mysql:///retail_db", "root", "cloudera");
            if (!con.isClosed()) {
                System.out.println("Successfully connected to MySQL server...");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        if (con != null) {
            Statement stat = con.createStatement();

            /** build state_idx */
            stat.executeQuery("select distinct(c.customer_state) as state from customers as c order by c.customer_state");
            ResultSet rs = stat.getResultSet();
            int idx = 1;
            HashMap state_idx = new HashMap();
            while (rs.next()) {
                String state_name = rs.getString("state");
                state_idx.put(state_name, idx++);
            }

            /** row length : state */
            stat.executeQuery("select count(DISTINCT (customer_state)) as r from customers");
            rs = stat.getResultSet();
            int row = 0;
            if (rs.next()) {
                row = rs.getInt("r");
            }
            /** col length : category_id */
            stat.executeQuery("select max(product_category_id) as c from products");
            rs = stat.getResultSet();
            int col = 0;
            if (rs.next()) {
                col = rs.getInt("c");
            }
            /** init observ [][] */
            double[][] observ = new double[row + 1][col + 1]; // idx 0 := sum 
            for (int i = 0; i < row + 1; i++) {
                for (int j = 0; j < col + 1; j++) {
                    observ[i][j] = 0.0;
                }
            }

            this.df = (row - 1) * (col - 1); // calculate degree of freedom

            /** build observ [][] */
            stat.executeQuery("select c.customer_state as state, p.product_category_id as id from customers as c, products as p, orders as o, order_items as i where c.customer_id = o.order_customer_id and o.order_id = i.order_item_order_id and i.order_item_product_id = p.product_id");
            rs = stat.getResultSet();
            while (rs.next()) {
                String state = rs.getString("state");
                int id = rs.getInt("id");
                int row_idx = (int) state_idx.get(state);
                observ[row_idx][id] += 1;
            }

            /** calculate sum at index 0*/
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

            /**
            for (int i = 0; i< row + 1; i++) {
                for (int j = 0; j < col + 1; j++) {
                    System.out.printf("%7.1f ", observ[i][j]);
                }
                System.out.println();
            }
            */

            /** calculate x */
            this.x = 0;
            for (int i = 1; i < row + 1; i++) {
                for (int j = 1; j < col + 1; j++) {
                    if (observ[i][0] == 0 || observ[0][j] == 0) {
                        this.x += 0;
                    } else {
                        this.x += Math.pow(Math.abs(observ[i][j] -(observ[i][0] * observ[0][j] / sum)), 2) / (observ[i][0] * observ[0][j] / sum);
                    }
                }
            }
            /** System.out.println(x); */
            rs.close();
            stat.close();
        }
        if (con != null) {
            con.close();
        }
    }

    /**
     * Do test boolean.
     *
     * @return the boolean
     */
    public final boolean doTest(){
        boolean h0;
        ChiSquareDist csd = new ChiSquareDist(this.df);
        if (this.x > csd.inverseF(1 - this.alpha)) {
            h0 = true; // reject h0
        } else {
            h0 = false; // accept h0
        }
        return h0;
    }

    /**
     * Set alpha.
     *
     * @param a the a
     */
    public final void setAlpha(double a){
        if (a <= 0 || a >=1 ) {
            System.out.println("alpha must > 0 or < 1")
        } else {
            this.alpha = a;
        }
    }
}


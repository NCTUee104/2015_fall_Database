
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
    private int df; // degree of freedom
    private double x; // 估計值

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
            con = DriverManager.getConnection("jdbc:mysql:///retail_db", "root", "cloudera"); // 建立連線
            if (!con.isClosed()) { 
                System.out.println("Successfully connected to MySQL server...");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        if (con != null) {
            Statement stat = con.createStatement(); // 創建一個用來執行sql的敘述

            /** build state_idx 
            *  當我們想要創建一個2維的陣列來存放觀察值時
            *  需要index來存取這個2維陣列
            *  我們用category_id當作陣列的“行”，customer_state當作陣列的“列”
            *  id是數字可以直接拿來做index用。state不行，所以必須透過下面的hashMap來轉換成數字(Key/Value的概念)
            *  當你給這個hashMap一個字串，他就會回傳對應的數字(eg. state_idx.get(state)會回傳對應的index)
            *  在這個sql式已經把state都排序好，所以會從A字頭的state開始，對應1....直到所有state都被對應完
            */
            stat.executeQuery("select distinct(c.customer_state) as state from customers as c order by c.customer_state");
            ResultSet rs = stat.getResultSet(); // 接收sql回傳的東西
            int idx = 1;
            HashMap state_idx = new HashMap();
            while (rs.next()) {
                String state_name = rs.getString("state");
                state_idx.put(state_name, idx++); // 用來把對應關係放進state_idx
            }
            
            /** 找出行列的大小才能創建2維的觀察值陣列 */
            /** row length : state */
            stat.executeQuery("select count(DISTINCT (customer_state)) as r from customers");
            rs = stat.getResultSet();
            int row = 0;
            if (rs.next()) { 
                row = rs.getInt("r");
            }
            /** col length : category_id */
            stat.executeQuery("select max(product_category_id) as c from products"); // id是數字直接用max就可以找到id到多少
            rs = stat.getResultSet();
            int col = 0;
            if (rs.next()) {
                col = rs.getInt("c");
            }
            /** init observ [][] 
            *  之所以要把row跟col都+1是因為要把第0列跟第0行當做放總和的地方
            *  在計算期望值的時候需要用到列或行的總和，也因此前面的state_idx的idx才會從1開始
            *  再來就只是把陣列的值全部初始化為0
            */
            double[][] observ = new double[row + 1][col + 1]; // idx 0 := sum 
            for (int i = 0; i < row + 1; i++) {
                for (int j = 0; j < col + 1; j++) {
                    observ[i][j] = 0.0;
                }
            }

            this.df = (row - 1) * (col - 1); // calculate degree of freedom

            /** build observ [][] 
            *  把這些有對應state, id的紀錄全都找出來，每當存在一筆這樣的資料
            *  就根據id找對應的行，根據stat_idx對應的state找列
            */
            stat.executeQuery("select c.customer_state as state, p.product_category_id as id from customers as c, products as p, orders as o, order_items as i where c.customer_id = o.order_customer_id and o.order_id = i.order_item_order_id and i.order_item_product_id = p.product_id");
            rs = stat.getResultSet();
            while (rs.next()) {
                String state = rs.getString("state");
                int id = rs.getInt("id");
                int row_idx = (int) state_idx.get(state);
                observ[row_idx][id] += 1;
            }

            /** calculate sum at index 0
            *  把一整行加總後放在第0列，一整列加總後放在第0行
            */
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

            /** 第0列加總 = 第0行加總 = sum */
            double sum = 0;
            for (int i = 1; i < row + 1; i++) {
                sum += observ[i][0]; // same as observ[0][i]
            }

            /** 測試用
            for (int i = 0; i< row + 1; i++) {
                for (int j = 0; j < col + 1; j++) {
                    System.out.printf("%7.1f ", observ[i][j]);
                }
                System.out.println();
            }
            */

            /** calculate x 
            *  x = sigma { (期望值 - 觀察值)^2 / 期望值 }
            */
            this.x = 0;
            for (int i = 1; i < row + 1; i++) {
                for (int j = 1; j < col + 1; j++) {
                    if (observ[i][0] == 0 || observ[0][j] == 0) { // 會讓分母為0
                        this.x += 0;
                    } else { // 期望值 = 該列總和 * 該行總和 / sum 
                        this.x += Math.pow(Math.abs(observ[i][j] -(observ[i][0] * observ[0][j] / sum)), 2) / (observ[i][0] * observ[0][j] / sum);
                    } // pow(x, 2) := x^2   , abs := 絕對值
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


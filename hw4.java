/**
 * Created by Edward on 2015/11/9.
 *
 * @author Edward
 * @version 1.0
 * @since   2015-11-09
 * Default Interface is Public.
 */
interface Calculator {
    /**
     * Sum long.
     *
     * @param r which row number needed to use.
     * @return  sum value of long type.
     *
     * Default methods in Interface are all Public and Abstract.
    */
    long sum(int r);

    /**
     * Avg double.
     *
     * @param r which row number needed to use.
     * @return  average value of double type.
     *
     * Default methods in Interface are all Public and Abstract.
     */
    double avg(int r);
}

/**
 * The type Progression 2 0450742.
 */
public class Progression2_0450742 extends Progression_0450742 implements Calculator {
    /**
     * Instantiates a new Progression 2 0450742.
     *
     * @param m row
     * @param n col
     */
    public Progression2_0450742(final int m, final int n) {
        super(m, n);
    }

    /**
     * Return sum value of given row.
     *
     * @param r specify which row needed to use.
     * @return  sum value
     * @throws  ArithmeticException If adding process is overflow.
     */
    public final long sum(final int r) {
        long ans = 0;
        long[] row = super.returnRow(r);
        for (long i : row) {
            if (ans + i < ans) {
                throw new ArithmeticException("Overflow occur");
            } else {
                ans += i;
            }
        }
        return ans;
    }

    /**
     * Return average value of all elements in given row.
     *
     * @param r specify which row needed to use.
     * @return  average value.
     * @throws  ArithmeticException If adding process is overflow.
     */
    public final double avg(final int r) {
        double ans = 0;
        long[] row = super.returnRow(r);
        for (long i : row) {
            if (ans + i < ans) {
                throw new ArithmeticException("Overflow occur");
            } else {
                ans += i;
            }
        }
        ans /= row.length;
        return ans;
    }

}

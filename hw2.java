/**
 * Created by Edward on 2015/10/27.
 * HW2 at 2015 fall Database Management Systems and Practice
 *
 * Refer to Google Java Style
 * <a href="https://google.github.io/styleguide/javaguide.htm"></a>
 * Refer to JavaDoc
 * <a href="http://www.oracle.com/technetwork/articles/java/index-137868.htm"></a>
 * Refer to Oracle Java Tutorial
 * <a href="http://docs.oracle.com/javase/tutorial/essential/exceptions/runtime.htm"></a>
 * Refer to Checkstyle
 * <a href="http://checkstyle.sourceforge.net/"></a>
 */
//import java.lang.RuntimeException;
//import java.lang.ArrayIndexOutOfBoundsException;
//import java.lang.ArithmeticException;
//import java.lang.System;
//import java.lang.NegativeArraySizeException;
//import java.lang.OutOfMemoryError;
//import java.lang.NullPointerException;
//import java.lang.String.*;
//import java.lang.System.*;
//import java.lang.Long;

/**
 * The type Progression 0450742.
 *
 * <p>Runtime exceptions can occur anywhere in a program,
 * and in a typical one they can be very numerous.
 * Thus, having to add runtime exceptions in every method declaration
 * would reduce a program's clarity.
 * RuntimeException are not in the methods signature
 * but are documented in the javadoc.
 *
 * @author  Edward
 * @version 4.0
 * @since   2015-10-27
 */
public class Progression_0450742 {

    /** Row number of Progression Array. */
    private int row;
    /** Column number of Progression Array. */
    private int col;
    /** Use Progression Array to show or return row, col, array. */
    private long[][] progressionArray;

    /**
     * Instantiates a new Progression 0450742.
     *
     * @param  m                              the m is row of ProgressionArray
     * @param  n                              the n is col of ProgressionArray
     * @throws ArithmeticException            If value in array is overflow
     * @throws ArrayIndexOutOfBoundsException If access out of array bounds
     * @throws NegativeArraySizeException     If m, n is negative number
     * @throws NullPointerException           If attempts to use null
     * @throws OutOfMemoryError               If m, n is too big for heap memory
     */
    public Progression_0450742(final int m, final int n) {
		if (m < 1 || n < 1) {
            throw new NegativeArraySizeException("Array size must be > 0");
		}
        try {
            progressionArray = new long[m][n];
        }   catch (OutOfMemoryError e) {
            System.err.printf("Catch Exception : %s !!!\n", e);
            System.err.println("m, n is too big for heap memory to create an array");
        }
        for (int i = 0; i < m; i++) {
            progressionArray[i][0] = i;
            for (int j = 1; j < n; j++) {
                if (i % 2 == 1) {
                    progressionArray[i][j] = progressionArray[i][j - 1] + i;
                } else {
                    if (i != 0 && (progressionArray[i][j - 1] * i) % i != 0) {
                        System.out.printf("Overflow occur in row : %d, col : %d\n", i, j);
                        System.out.printf("Previous value is %d\n", progressionArray[i][j - 1]);
                        System.out.printf("Overflow value is %d\n", progressionArray[i][j - 1]  * i);
                        throw new ArithmeticException("Long type is overflow");
                    } else if (progressionArray[i][j - 1] > progressionArray[i][j - 1] * i) {
                        System.out.printf("Overflow occur in row : %d, col : %d\n", i, j);
                        System.out.printf("Previous value is %d\n", progressionArray[i][j - 1]);
                        System.out.printf("Overflow value is %d\n", progressionArray[i][j - 1]  * i);
                        throw new ArithmeticException("Long type is overflow");
                    } else {
                        progressionArray[i][j] = progressionArray[i][j - 1] * i;
                    }
                }
            }
        }
        this.row = m;
        this.col = n;
    }
    /**
     * Print out the whole row with respect to given r.
     *
     * @param r the r is the row number
     *          which corresponding to whole row in Progression Array
     * @throws ArrayIndexOutOfBoundsException If access out of array bounds
     */
    public final void showRow(final int r) {
        if (r < 0 || r > row) {
            throw new ArrayIndexOutOfBoundsException(r);
        }
        System.out.printf("Show Row %d :\n", r);
        for (int i = 0; i < col; i++) {
            System.out.printf("%d ", progressionArray[r][i]);
        }
        System.out.println();
    }

    /**
     * Print out the whole column with respect to given c.
     *
     * @param c the c is the column number
     *          which corresponding to whole column in Progression Array
     * @throws ArrayIndexOutOfBoundsException If access out of array bounds
     */
    public final void showCol(final int c) {
        if (c < 0 || c > col) {
            throw new ArrayIndexOutOfBoundsException(c);
        }
        System.out.printf("Show Column %d :\n", c);
        try {
            for (int i = 0; i < row; i++) {
                System.out.printf("%d\n", progressionArray[i][c]);
            }
        }   catch (ArrayIndexOutOfBoundsException e) {
            System.err.printf("Catch Exception : %s !!!\n", e);
        }
        //System.out.println();
    }

    /**
     * Print out the whole Progression Array.
     */
    public final void showArray() {
        System.out.println("Show Array :");
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < col; j++) {
                System.out.printf("%d ", progressionArray[i][j]);
            }
            System.out.println();
        }
    }

    /**
     * Return the whole row with respect to given r which is long[ ] type.
     *
     * @param r the r is the row number
     *          which corresponding to whole row in Progression Array
     * @return  the long [ ] which corresponding to row number
     * @throws ArrayIndexOutOfBoundsException If given r is out of array bounds
     */
    public final long[] returnRow(final int r) {
        if (r < 0 || r > row) {
            throw new ArrayIndexOutOfBoundsException(r);
        }
        System.out.printf("Return Row %d :\n", r);
        return progressionArray[r];
    }

    /**
     * Return the whole column with respect to given c which is long[ ] type.
     *
     * @param c the c is the column number
     *          which corresponding to whole column in Progression Array
     * @return  the long [ ] which corresponding to column number
     * @throws ArrayIndexOutOfBoundsException If given c is out of array bounds
     */
    public final long[] returnCol(final int c) {
        if (c < 0 || c > col) {
            throw new ArrayIndexOutOfBoundsException(c);
        }
        System.out.printf("Return Col %d :\n", c);
        long[] ans = new long[row];
        for (int i = 0; i < row; i++) {
            ans[i] = this.progressionArray[i][c];
        }
        return ans;
    }

    /**
     * Return whole Progression Array which is long[][] type.
     *
     * @return the long [ ] [ ] which is exactly Progression Array
     */
    public final long[][] returnArray() {
        System.out.println("Return array :");
        return this.progressionArray;
    }

}






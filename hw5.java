/**
 * Created by Edward on 2015/11/14.
 */

public class Progression3_0450742 {
    /**
     * The entry point of application.
     */
}

/**
 * The type Invalid progression size exception.
 */
class InvalidProgressionSizeException extends Exception {

    /** errorType to specify which massage */
    private int e;

    /**
     * Instantiates a new Invalid progression size exception.
     *
     * @param errorType INT type
     */
    InvalidProgressionSizeException(final int errorType) {
        e = errorType;
        super.getMessage();
    }

    /**
     * To string for getMassage()
     *
     * @return different msg based on errorType
     */
    @Override public String toString() {
        switch (e) {
            case 0:
                return "M should be greater than 0";
            case 1:
                return "Out of range of long type";
            case 2:
                return "No enough memory to construct array";
            case 3:
                return "Must use positive number to create array";
            default:
                return "other error occur";
        }
    }
}

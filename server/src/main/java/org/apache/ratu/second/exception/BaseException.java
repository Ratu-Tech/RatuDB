package org.apache.ratu.second.exception;

public class BaseException extends RuntimeException {

    /**
     * Constructor.
     */
    public BaseException() {
        super();
    }

    /**
     * Constructor.
     *
     * @param msg The error message to pass along
     */
    public BaseException(final String msg) {
        super(msg);
    }

    /**
     * Constructor.
     *
     * @param msg   The error message to pass along
     * @param cause The cause of the error
     */
    public BaseException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}

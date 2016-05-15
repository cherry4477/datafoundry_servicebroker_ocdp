package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception;

public class KerberosOperationException extends Exception {

    /**
     * Creates a new KerberosOperationException with a message
     *
     * @param message a String containing the message indicating the reason for this exception
     */
    public KerberosOperationException(String message) {
        super(message);
    }

    /**
     * Creates a new KerberosOperationException with a message and a cause
     *
     * @param message a String containing the message indicating the reason for this exception
     * @param cause   a Throwable declaring the previously thrown Throwable that led to this exception
     */
    public KerberosOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}

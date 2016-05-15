package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception;

public class KerberosKDCConnectionException extends KerberosOperationException {

    /**
     * Creates a new KerberosKDCConnectionException with a message
     *
     * @param message a String containing the message indicating the reason for this exception
     */
    public KerberosKDCConnectionException(String message) {
        super(message);
    }

    /**
     * Creates a new KerberosKDCConnectionException with a message and a cause
     *
     * @param message a String containing the message indicating the reason for this exception
     * @param cause   a Throwable declaring the previously thrown Throwable that led to this exception
     */
    public KerberosKDCConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}

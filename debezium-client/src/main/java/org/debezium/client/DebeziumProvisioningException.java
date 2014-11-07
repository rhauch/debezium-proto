/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

/**
 * An exception that denotes a failure to connect to Debezium.
 * 
 * @author Randall Hauch
 */
public class DebeziumProvisioningException extends DebeziumException {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Constructs a new runtime exception with {@code null} as its detail message. The cause is not initialized, and may
     * subsequently be initialized by a call to {@link #initCause}.
     */
    public DebeziumProvisioningException() {
    }
    
    /**
     * Constructs a new runtime exception with the specified detail message. The cause is not initialized, and may subsequently be
     * initialized by a call to {@link #initCause}.
     * 
     * @param message the message
     */
    public DebeziumProvisioningException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new runtime exception with the specified cause and a detail message of
     * <tt>(cause==null ? null : cause.toString())</tt> (which typically contains the class and detail message of <tt>cause</tt>).
     * This constructor is useful for runtime exceptions that are little more than wrappers for other throwables.
     * 
     * @param cause the cause. (A {@code null} value is permitted,
     *            and indicates that the cause is nonexistent or unknown.)
     */
    public DebeziumProvisioningException(Throwable cause) {
        super(cause);
    }
    
    /**
     * Constructs a new runtime exception with the specified detail message and cause.
     * <p>
     * Note that the detail message associated with {@code cause} is <i>not</i> automatically incorporated in this runtime
     * exception's detail message.
     * 
     * @param message the message
     * @param cause the cause. (A {@code null} value is permitted,
     *            and indicates that the cause is nonexistent or unknown.)
     */
    public DebeziumProvisioningException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructs a new runtime exception with the specified detail message, cause, suppression enabled or disabled, and writable
     * stack trace enabled or disabled.
     * 
     * @param message the message
     * @param cause the cause. (A {@code null} value is permitted,
     *            and indicates that the cause is nonexistent or unknown.)
     * @param enableSuppression whether or not suppression is enabled
     *            or disabled
     * @param writableStackTrace whether or not the stack trace should
     *            be writable
     */
    public DebeziumProvisioningException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
    
}

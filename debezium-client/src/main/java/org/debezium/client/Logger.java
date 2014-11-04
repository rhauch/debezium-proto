/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

/**
 * @author Randall Hauch
 *
 */
interface Logger {
    
    public static enum Level {
        INFO, ERROR, WARN, DEBUG, TRACE;
    }
    
    default void info(String msg, Object... params) {
        log(Level.INFO,msg,params);
    }
    
    default void error(String msg, Object... params) {
        log(Level.ERROR,msg,params);
    }
    
    default void warn(String msg, Object... params) {
        log(Level.WARN,msg,params);
    }
    
    default void debug(String msg, Object... params) {
        log(Level.DEBUG,msg,params);
    }
    
    default void trace(String msg, Object... params) {
        log(Level.TRACE,msg,params);
    }
    
    default void info(Throwable error, String msg, Object... params) {
        log(Level.INFO,error,msg,params);
    }
    
    default void error(Throwable error, String msg, Object... params) {
        log(Level.ERROR,error,msg,params);
    }
    
    default void warn(Throwable error, String msg, Object... params) {
        log(Level.WARN,error,msg,params);
    }
    
    default void debug(Throwable error, String msg, Object... params) {
        log(Level.DEBUG,error,msg,params);
    }
    
    default void trace(Throwable error, String msg, Object... params) {
        log(Level.TRACE,error,msg,params);
    }
    
    void log(Level level, String msg, Object... params);
    
    void log(Level level, Throwable error, String msg, Object... params);
}

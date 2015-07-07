/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.util;

/**
 * @author Randall Hauch
 *
 */
public interface Timer {
    
    public Timer start();
    
    public Timer stop();

}

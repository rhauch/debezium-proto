/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import org.debezium.core.DbzNode;

/**
 * @author Randall Hauch
 *
 */
public interface Service {

    void start( DbzNode node );
    
    void stop();
}

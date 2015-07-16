/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * @author Randall Hauch
 *
 */
public class DbzDatabasesTest extends AbstractDbzNodeTest {

    private DbzPartialResponses responses;
    private DbzDatabases databases;
    
    @Override
    protected void addServices(DbzNode node) {
        responses = new DbzPartialResponses();
        databases = new DbzDatabases(responses);
        node.add(responses,databases);
    }
    
    @Test
    public void test() {
        fail("Not yet implemented");
    }

}

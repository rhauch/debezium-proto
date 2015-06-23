/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.example;

import org.debezium.driver.Configuration;
import org.debezium.driver.Debezium;
import org.debezium.driver.RandomContent;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class LoadAppTest {

    private static final RandomContent RANDOM_CONTENT = RandomContent.load();
    
    @Before
    public void beforeEach() {
        LoadApp.clientFactory = this::mock;
    }
    
    @Ignore
    @Test
    public void testVerbose() {
        LoadApp.main(args("-v","-r","1000003","-t","1"));
    }

    @Ignore
    @Test
    public void testNonVerbose() {
        LoadApp.main(args("-r","1000003","-t","3"));
    }

    protected Debezium.Client mock( Configuration config ) {
        return MockDatabase.createClient(RANDOM_CONTENT);
    }
    
    protected String[] args( String...args ) {
        return args;
    }
    
}

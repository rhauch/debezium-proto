/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.TimeUnit;

import org.debezium.Testing;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.junit.After;
import org.junit.Before;

/**
 * A base for tests that use a Debezium client, with utilities for testing various operations.
 * 
 * @author Randall Hauch
 */
public abstract class AbstractDebeziumTest implements Testing {

    protected static final RandomContent RANDOM_CONTENT = RandomContent.load();
    private final DatabaseId dbId = Identifier.of("my-db");
    protected final EntityType contactType = Identifier.of(dbId, "contact");
    protected String username = "jsmith";
    protected String device = "my-device";
    protected String appVersion = "2.1";
    protected Debezium dbz;
    protected SessionToken session;

    /**
     * Create a new Debezium.Client. This is called once at the beginning of each test.
     * 
     * @return the new client; may not be null
     */
    protected abstract Debezium createClient();

    /**
     * Method called before the client is shut down. This method does nothing by default.
     * 
     * @param client the client; never null
     */
    protected void preShutdown(Debezium client) {
    }

    @Before
    public void beforeEach() {
        resetBeforeEachTest();
        dbz = createClient();
    }

    @After
    public void afterEach() {
        preShutdown(dbz);
        try {
            dbz.shutdown(10, TimeUnit.SECONDS);
        } finally {
            dbz = null;
        }
    }

    /**
     * Connect to one or more databases.
     * @param databaseIds the identifiers of the databases
     */
    public void connect( String...databaseIds ) {
        session = dbz.connect(username,device,appVersion,databaseIds);
    }
    
    public void provision( String databaseId ) {
        dbz.provision(session, databaseId, 10, TimeUnit.SECONDS);
    }
    
    public void provision( String databaseId, long seconds ) {
        dbz.provision(session, databaseId, seconds, TimeUnit.SECONDS);
    }
    
    /**
     * Submit a batch operation to create the specified number of entities of the given {@link EntityType type}.
     * 
     * @param count the number of new entities; must be positive
     * @param type the type of entities to create; may not be null
     * @return the result of the batch operation; never null
     */
    public BatchResult createEntities(int count, EntityType type) {
        return RANDOM_CONTENT.createGenerator()
                             .addToBatch(dbz.batch(),count, 0, type)
                             .submit(session, 10, TimeUnit.SECONDS);
    }

}

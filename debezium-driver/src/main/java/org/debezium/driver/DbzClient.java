/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.TimeUnit;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.message.Topic;


/**
 * @author Randall Hauch
 *
 */
final class DbzClient implements Debezium.Client {

    private final DbzNode node;
    private final DbzDatabases databases;
    private final ResponseHandlers responseHandlers;
    private final Environment env;
    
    public DbzClient( Configuration config, Environment env ) {
        this.env = env;
        this.node = new DbzNode(config,env);
        this.responseHandlers = new ResponseHandlers();
        this.databases = new DbzDatabases(this.responseHandlers);
        this.node.add(this.databases);
        this.node.add(this.responseHandlers); // will be shut down after 'databases'
    }
    
    public DbzClient start() {
        node.start();
        // Subscribe to the 'partial-responses' topic and forward to all of the response handlers ...
        node.subscribe(node.id(), Topics.of(Topic.PARTIAL_RESPONSES), 1, (topic,partition,offset,key,msg)->responseHandlers.submit(msg));
        return this;
    }
    
    @Override
    public Database connect(DatabaseId id, String username, String device, String appVersion) {
        return connect(id,username,device,appVersion, 10, TimeUnit.SECONDS);
    }
    
    @Override
    public Database connect(DatabaseId id, String username, String device, String appVersion, long timeout, TimeUnit unit) {
        return databases.connect(new ExecutionContext(id,username,device,appVersion, timeout, unit));
    }
    
    @Override
    public Database provision(DatabaseId id, String username, String device, String appVersion) {
        return provision(id,username,device,appVersion, 10, TimeUnit.SECONDS);
    }
    
    @Override
    public Database provision(DatabaseId id, String username, String device, String appVersion, long timeout, TimeUnit unit) {
        return databases.provision(new ExecutionContext(id,username,device,appVersion, timeout, unit));
    }
    
    @Override
    public void shutdown( long timeout, TimeUnit unit ) {
        // Shutdown the cluster node, which shuts down all services and the service manager ...
        try {
            node.shutdown();
        } finally {
            env.shutdown(timeout,unit);
        }
    }
}

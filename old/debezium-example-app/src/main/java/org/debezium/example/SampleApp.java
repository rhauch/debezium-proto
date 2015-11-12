/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.util.IoUtil;
import org.debezium.driver.BatchResult;
import org.debezium.driver.Debezium;
import org.debezium.driver.DebeziumAuthorizationException;
import org.debezium.driver.Entity;
import org.debezium.driver.Schema;
import org.debezium.driver.SessionToken;

/**
 * @author Randall Hauch
 *
 */
public class SampleApp {

    public static void main(String[] args) {
        String pathToConfigFile = "debezium.json";
        String dbName = "my-db";
        String username = "jsmith";
        String device = UUID.randomUUID().toString();
        String appVersion = "1.0";

        Debezium driver = null;
        try {
            InputStream stream = findConfiguration(pathToConfigFile);
            driver = Debezium.driver()
                             .load(stream)
                             .start();
        } catch (IOException e) {
            System.out.println("Unable to read Debezium client configuration file at '" + pathToConfigFile + "': " + e.getMessage());
            System.exit(1);
        }

        SampleApp app = new SampleApp(driver);
        app.connect(username, device, appVersion, dbName);

        // Do stuff ...
        try {
            app.readAndDisplaySchema(dbName);
            app.loadNewContacts(dbName);
            app.readAndDisplaySchema(dbName);
        } finally {
            try {
                app.shutdown(10, TimeUnit.SECONDS);
                System.exit(0);
            } catch (Throwable e) {
                System.out.println("Error shutting down Debezium driver '" + dbName + "': " + e.getMessage());
                System.exit(3);
            }
        }
    }

    private final ConcurrentMap<EntityId, Document> localStore = new ConcurrentHashMap<>();
    private final Debezium driver;
    private SessionToken session;

    public SampleApp(Debezium driver) {
        this.driver = driver;
    }

    public void connect(String username, String device, String appVersion, String dbName) {
        try {
            session = driver.connect(username, device, appVersion, dbName);
        } catch (DebeziumAuthorizationException e) {
            // Connecting failed, so try to provision ...
            try {
                driver.provision(session, dbName, 10, TimeUnit.SECONDS);
            } catch (Throwable e2) {
                System.out.println("Error connecting to or provisioning Debezium database '" + dbName + "': " + e.getMessage());
                System.exit(2);
            }
        }
    }

    public void shutdown(long timeout, TimeUnit unit) {
        System.out.println("Shutting down Debezium driver...");
        driver.shutdown(timeout, unit);
        System.out.println("Completed shutting down Debezium driver");
    }

    protected void print(Object msg) {
        System.out.println(msg);
    }

    public void readAndDisplaySchema(String dbName) {
        System.out.println("Reading schema from '" + dbName + "'...");
        Schema schema = driver.readSchema(session, dbName, 10, TimeUnit.SECONDS);
        if (schema.asDocument() != null) {
            print("Schema: " + schema.asDocument());
        } else {
            print("Unable to read the schema for '" + dbName + "'");
        }
    }

    public void loadNewContacts(String dbName) {
        System.out.println("Creating 2 new contacts in '" + dbName + "'...");
        // Create 2 contacts ...
        EntityType type = Identifier.of(dbName, "Contacts");
        BatchResult result = driver.batch()
                                   .createEntity(type)
                                   .add("firstName", Value.create("Sally"))
                                   .add("lastName", Value.create("Anderson"))
                                   .add("homePhone", Value.create("1-222-555-1234"))
                                   .end()
                                   .createEntity(type)
                                   .add("firstName", Value.create("William"))
                                   .add("lastName", Value.create("Johnson"))
                                   .add("mobilePhone", Value.create("1-222-555-9876"))
                                   .end().submit(session, 10, TimeUnit.SECONDS);
        result.changeStream().forEach(change -> {
            if (change.succeeded()) {
                // We successfully changed this entity by applying our patch ...
                contactUpdated(change.entity());
            } else {
                // Our patch for this entity could not be applied ...
                System.out.println("Failed to submit batch with changes to contacts: " + change.failureReasons());
                switch (change.status()) {
                    case OK:
                        break;
                    case DOES_NOT_EXIST:
                        // The entity was removed by someone else, so we'll delete it locally ...
                        contactRemoved(change.id());
                        break;
                    case PATCH_FAILED:
                        // We put preconditions into our patch that were not satisfied, so perhaps we
                        // should rebuild a new patch with the latest representation of the target.
                        // For now, we'll just update the contact ...
                        // Patch<EntityId> failedPatch = change.patch();
                        contactUpdated(change.entity());
                        break;
                }
            }
        });
    }

    protected void contactUpdated(Entity entity) {
        localStore.put(entity.id(), entity.asDocument());
        System.out.println("Updated contact: \n" + entity.asDocument());
    }

    protected void contactRemoved(String entityId) {
        localStore.remove(Identifier.parse(entityId));
        System.out.println("Removed contact: " + entityId);
    }

    protected static InputStream findConfiguration(String path) {
        InputStream stream = IoUtil.getResourceAsStream(path, SampleApp.class.getClassLoader(), null, "configuration file", System.out::println);
        if (stream == null) {
            System.out.println("Unable to read Debezium client configuration file at '" + path + "': file not found");
            System.exit(3);
        }
        return stream;
    }
}

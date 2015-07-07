/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Entity;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.Schema;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Batch;
import org.debezium.driver.Configuration;
import org.debezium.driver.Database;
import org.debezium.driver.Debezium;
import org.debezium.driver.DebeziumConnectionException;
import org.debezium.driver.Database.Change;
import org.debezium.driver.Database.Completion;
import org.debezium.driver.Database.Outcome;

/**
 * @author Randall Hauch
 *
 */
public class SampleApp {
    
    protected static Configuration readConfiguration( String path ) {
        try {
            InputStream stream = SampleApp.class.getClassLoader().getResourceAsStream(path);
            if ( stream == null ) {
                // Try path as-is ...
                File f = FileSystems.getDefault().getPath(path).toAbsolutePath().toFile();
                if ( f== null || !f.exists() || f.isDirectory() ) {
                    // Try relative to current working directory ...
                    Path current = FileSystems.getDefault().getPath(".").toAbsolutePath();
                    Path absolute = current.resolve(Paths.get(path)).toAbsolutePath();
                    f = absolute.toFile();
                }
                if ( f != null ) {
                    stream = new FileInputStream(f);
                }
            }
            if ( stream != null ) {
                return Debezium.configure(stream).build();
            }
        } catch ( IOException e ) {
            System.out.println("Unable to read Debezium client configuration file at '" + path + "': " + e.getMessage());
            System.exit(1);
        }
        System.out.println("Unable to read Debezium client configuration file at '" + path + "': file not found");
        System.exit(1);
        return null;
    }

    public static void main(String[] args) {
        String pathToConfigFile = "debezium.json";
        String dbName = "my-db";
        String username = "jsmith";
        String device = UUID.randomUUID().toString();
        String appVersion = "1.0";

        Configuration config = readConfiguration(pathToConfigFile);
        Debezium.Client client = Debezium.start(config);

        DatabaseId dbId = Identifier.of(dbName);
        Database db = null;
        try {
            db = client.connect(dbId, username,device,appVersion);
        } catch ( DebeziumConnectionException e ) {
            // Connecting failed, so try to provision ...
            try {
                db = client.provision(dbId,username,device,appVersion);
            } catch (Throwable e2) {
                System.out.println("Error connecting to or provisioning Debezium database '" + dbName + "': " + e.getMessage());
                System.exit(2);
            }
        }

        // Do stuff ...
        SampleApp app = new SampleApp(db);
        try {
            attempt(app::readAndDisplaySchema);
            attempt(app::loadNewContacts);
            attempt(app::readAndDisplaySchema);
        } finally {
            try {
                app.shutdown();
                System.exit(0);
            } catch (Throwable e) {
                System.out.println("Error shutting down Debezium database '" + dbName + "': " + e.getMessage());
                System.exit(3);
            } finally {
                try {
                    client.shutdown(10, TimeUnit.SECONDS);
                } catch (Throwable e) {
                    System.out.println("Error shutting down Debezium client: " + e.getMessage());
                    System.exit(4);
                }
            }
        }
    }
    
    private static void attempt( Supplier<Completion> runnable ) {
        try {
            Completion result = runnable.get();
            if ( result != null && !result.isComplete() ) {
                System.out.println("Waiting for results...");
                result.await(10, TimeUnit.SECONDS);
                if ( !result.isComplete() ) System.out.println("Timed out");
            } else {
                System.out.println("No need to wait");
            }
        } catch ( TimeoutException e ) {
            System.out.println("Timeout waiting for response for " + runnable + ": " + e.getMessage());
        } catch ( InterruptedException e ) {
            Thread.interrupted();
            System.out.println("Interrupted while waiting for response for " + runnable + ": " + e.getMessage());
        }
    }

    private final ConcurrentMap<EntityId, Document> localStore = new ConcurrentHashMap<>();
    private Database db;

    public SampleApp(Database db) {
        this.db = db;
    }

    public void shutdown() {
        System.out.println("Shutting down Debezium client...");
        db.close();
        System.out.println("Shutdown complete.");
    }
    
    protected void print( Object msg ) {
        System.out.println(msg);
    }

    public Completion readAndDisplaySchema() {
        System.out.println("Reading schema from '" + db.databaseId() + "'...");
        return db.readSchema(outcome -> {
            if (outcome.succeeded()) {
                Schema schema = outcome.result();
                print("Schema: " + schema.document());
            } else {
                // Unable to read the schema, so handle the error
                String reason = outcome.failureReason();
                print("Unable to read the schema: " + reason);
            }
        });
    }

    public Completion loadNewContacts() {
        System.out.println("Creating 2 new contacts in '" + db.databaseId() + "'...");
        // Create 2 contacts ...
        EntityType type = Identifier.of(db.databaseId(),"Contacts");
        Batch<EntityId> batch = Batch.entities()
                                     .create(Identifier.newEntity(type))
                                     .add("firstName", Value.create("Sally"))
                                     .add("lastName", Value.create("Anderson"))
                                     .add("homePhone", Value.create("1-222-555-1234"))
                                     .end()
                                     .create(Identifier.newEntity(type))
                                     .add("firstName", Value.create("William"))
                                     .add("lastName", Value.create("Johnson"))
                                     .add("mobilePhone", Value.create("1-222-555-9876"))
                                     .end()
                                     .build();
        return db.changeEntities(batch,this::handleUpdate);
    }
    
    protected void handleUpdate( Outcome<Stream<Change<EntityId,Entity>>> outcome ) {
        if ( outcome.succeeded() ) {
            outcome.result().forEach(change -> {
                if (change.succeeded()) {
                    // We successfully changed this entity by applying our patch ...
                    contactUpdated(change.target());
                } else {
                    // Our patch for this entity could not be applied ...
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
                            contactUpdated(change.target());
                            break;
                    }
                }
            });
        } else {
            // Unable to even submit the batch, so handle the error. Perhaps the system is not available, or
            // our request was poorly formed (e.g., was empty). The best way to handle this is to do different
            // things based upon the exception type ...
            String reason = outcome.failureReason();
            System.out.println("Failed to submit batch with changes to contacts: " + reason );
        }
    }
    
    protected void contactUpdated( Entity entity ) {
        localStore.put(entity.id(),entity.document());
        System.out.println("Updated contact: \n" + entity.document());
    }
    
    protected void contactRemoved( EntityId entityId ) {
        localStore.remove(entityId);
        System.out.println("Removed contact: " + entityId);
    }

}

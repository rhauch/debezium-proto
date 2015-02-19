/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.debezium.core.annotation.NotThreadSafe;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;

/**
 * A service (or task in Samza parlance) responsible for locally storing the devices for a user and the databases used by each
 * device.
 * <p>
 * This service consumes the "{@link Streams#connections connections}" topic, where each incoming message contains the username,
 * {@link DatabaseId}, device, client ID, application version, and the timestamp.
 * <em><strong>NOTE: At this time the service does not current generate any information.</strong></em>
 * <p>
 * This service uses Samza's storage feature to maintain a durable log of all users, devices, and databases, and to use an
 * in-process database for quick access. If this service fails, another can be restarted and can completely recover the database
 * from the durable log.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class DeviceService implements StreamTask, InitableTask {

    private KeyValueStore<String, Document> store;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) {
        this.store = (KeyValueStore<String, Document>) context.getStore("device-store");
    }

    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        try {
            Document request = (Document) env.getMessage();
            String username = Message.getUser(request);
            DatabaseId dbId = Message.getDatabaseId(request);
            String device = Message.getDevice(request);
            String version = Message.getAppVersion(request);
            long timestamp = Message.getBegun(request).getAsLong();

            // Look up the user's record in the store ...
            Document devices = store.get(username);
            if (devices == null) {
                // This is a new user ...
                devices = Document.create();
                store.put(username, devices);
            }
            // Record this device ...
            addDevice(devices, device, timestamp, dbId, version);

        } catch (RuntimeException t) {
            t.printStackTrace();
            throw t;
        }
    }

    /**
     * Records the device in the given document using the following structure:
     * <pre>
     * {
     *   "&lt;device>" : {
     *     "firstUsed" : &lt;timestampAsLong>,
     *     "lastUsed" : &lt;timestampAsLong>,
     *     "dbs" : {
     *        &lt;dbId> : {
     *           "firstUsed" : &lt;timestampAsLong>,
     *           "lastUsed" : &lt;timestampAsLong>,
     *           "appVersion" : &lt;versionAsString>,
     *        }
     *     }
     *   }
     * }
     * </pre>
     * @param devices the user's document of devices
     * @param device the device that is being used
     * @param timestamp the timestamp of the usage
     * @param dbId the identifier of the database being accessed
     * @param appVersion the version of the application used to access the database
     * @return {@code true} if this is the first time the device has been seen, or {@code false} otherwise
     */
    private boolean addDevice(Document devices, String device, long timestamp, DatabaseId dbId, String appVersion) {
        boolean added = false;
        Document deviceInfo = devices.getDocument(device);
        if (deviceInfo == null) {
            deviceInfo = Document.create();
            deviceInfo.setNumber("firstUsed", timestamp);
            devices.setDocument(device, deviceInfo);
            added = true;
        }
        deviceInfo.setNumber("lastUsed", timestamp);
        // Set the database info on this device ...
        Document databases = deviceInfo.getDocument("dbs");
        if (databases == null) {
            databases = Document.create();
            deviceInfo.setDocument("dbs", databases);
        }
        Document dbInfo = databases.getDocument(dbId.asString());
        if (dbInfo == null) {
            dbInfo = Document.create();
            dbInfo.setNumber("firstUsed", timestamp);
            databases.setDocument(dbId.asString(), dbInfo);
        }
        dbInfo.setNumber("lastUsed", timestamp);
        dbInfo.setString("appVersion", appVersion);
        return added;
    }
}

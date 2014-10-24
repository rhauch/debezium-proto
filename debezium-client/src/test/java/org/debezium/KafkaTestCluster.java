/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.TestUtils;

import org.apache.curator.test.TestingServer;

/**
 * A utility that allows running Kafka and Zookeeper in-process for testing purposes only.
 * @author Randall Hauch
 */
public class KafkaTestCluster {

    public static KafkaTestCluster forTest( Class<?> testClass ) throws Exception {
        return new KafkaTestCluster(Paths.get("target",testClass.getSimpleName()));
    }
    
    private final Path dataDir;
    private final Path zkDir;
    private final Path kafkaDir;
    private final KafkaServerStartable kafkaServer;
    private final TestingServer zkServer;
    
    private KafkaTestCluster( Path dataDir ) throws Exception {
        this.dataDir = dataDir.normalize().toAbsolutePath();
        this.zkDir = dataDir.resolve("zookeeper");
        this.kafkaDir = dataDir.resolve("kafka");
        zkServer = new TestingServer(-1,zkDir.toFile());
        KafkaConfig config = getKafkaConfig(zkServer.getConnectString(),kafkaDir.toFile());
        kafkaServer = new KafkaServerStartable(config);
        System.out.println("Getting ready to start Kafka Test Server");
        kafkaServer.startup();
        System.out.println("Startup was successful");
    }
 
    private static KafkaConfig getKafkaConfig(final String zkConnectString, File kafkaLogDir) {
        scala.collection.Iterator<Properties> propsI =
            TestUtils.createBrokerConfigs(1).iterator();
        assert propsI.hasNext();
        Properties props = propsI.next();
        assert props.containsKey("zookeeper.connect");
        props.put("zookeeper.connect", zkConnectString);
        props.put("log.dir", kafkaLogDir.getAbsolutePath());
        return new KafkaConfig(props);
    }
 
    public String getKafkaBrokerString() {
        return String.format("localhost:%d",
                kafkaServer.serverConfig().port());
    }
 
    public String getZkConnectString() {
        return zkServer.getConnectString();
    }
 
    public int getKafkaPort() {
        return kafkaServer.serverConfig().port();
    }
 
    public void stop() throws IOException {
        kafkaServer.shutdown();
        zkServer.stop();
    }
    
    public void stopAndCleanUp() throws IOException {
        stop();
        Testing.debug("Deleting directory for Zookeeper and Kafka: " + dataDir);
        Testing.Files.delete(dataDir);
    }
}
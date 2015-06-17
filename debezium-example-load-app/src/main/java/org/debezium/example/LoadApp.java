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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.util.CommandLineOptions;
import org.debezium.core.util.Stopwatch;
import org.debezium.driver.Configuration;
import org.debezium.driver.Database;
import org.debezium.driver.Debezium;
import org.debezium.driver.DebeziumConnectionException;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * @author Randall Hauch
 *
 */
public class LoadApp {

    private static final String VERSION = "0.01";

    public static enum ReturnCode {
        SUCCESS,
        UNABLE_TO_READ_CONFIGURATION,
        INCORRECT_USAGE,
        CONNECT_FAILURE,
        DATABASE_SHUTDOWN_ERROR,
        CLIENT_SHUTDOWN_FAILURE,
        EXECUTORS_SHUTDOWN_ERROR
    }

    private static boolean verbose = false;

    public static void main(String[] args) {
        // Process the command line parameters ...
        final CommandLineOptions options = CommandLineOptions.parse(args);
        if (options.getOption("-?", "--help", false) || options.hasParameter("help")) {
            printUsage();
            exit(ReturnCode.SUCCESS);
        }
        if (options.hasOption("--version")) {
            print(LoadApp.class.getSimpleName() + " version " + VERSION);
            exit(ReturnCode.SUCCESS);
        }
        final String pathToConfigFile = options.getOption("-c", "--config", "debezium.json");
        final String dbName = options.getOption("-d", "--db", "my-db");
        final String username = options.getOption("-u", "--user", "jsmith");
        final String device = options.getOption("-D", "--device", UUID.randomUUID().toString());
        final String entityTypeName = options.getOption("-e", "--entity-type", "contact");
        final int numThreads = options.getOption("-t", "--threads", 1);
        final int numRequestsPerThread = options.getOption("-r", "--requests-per-thread", 10000);
        verbose = options.getOption("-v", "--verbose", false);
        System.out.println("**** verbose = " + verbose );
        if (options.hasUnknowns()) {
            print("Unknown option: " + options.getFirstUnknownOptionName());
            printUsage();
            exit(ReturnCode.INCORRECT_USAGE);
        }

        // Read the configuration, start the client, and prepare the random content ...
        final Configuration config = readConfiguration(pathToConfigFile);
        printVerbose("Starting Debezium client with configuration: \n" + config);
        final Debezium.Client client = clientFactory.apply(config);
        printVerbose("Started Debezium client");
        final RandomContent randomContent = RandomContent.load();

        // Connect to the database ...
        final DatabaseId dbId = Identifier.of(dbName);
        final EntityType entityType = Identifier.of(dbId, entityTypeName);
        Database db = null;
        try {
            printVerbose("Connecting to Debezium database '" + dbId + "' as user '" + username + "' and device '" + device + "'...");
            db = client.connect(dbId, username, device, VERSION);
        } catch (DebeziumConnectionException e) {
            // Connecting failed, so try to provision ...
            try {
                printVerbose("Database '" + dbId + "' was not found; attempting to provision it ...");
                db = client.provision(dbId, username, device, VERSION);
            } catch (Throwable e2) {
                error("Error connecting to or provisioning Debezium database '" + dbName + "': " + e.getMessage());
                try {
                    print("Shutting down client ...");
                    client.shutdown(10, TimeUnit.SECONDS);
                    exit(ReturnCode.CONNECT_FAILURE);
                } catch (Throwable e3) {
                    error("Error shutting down Debezium client: " + e.getMessage());
                    exit(ReturnCode.CLIENT_SHUTDOWN_FAILURE);
                }
            }
        }

        // We have a valid connection, so create the threads that will talk to the database ...
        final MetricRegistry registry = new MetricRegistry();
        Meter batchMeter = registry.meter("BatchesPerSecond");
        Stopwatch sw = Stopwatch.simple();
        sw.start();
        ExecutorService executors = Executors.newFixedThreadPool(numThreads);
        try {
            List<Future<Results>> futures = new ArrayList<>();
            for (int i = 0; i != numThreads; ++i) {
                printVerbose("Starting thread " + (i + 1) + " to generate " + numRequestsPerThread + " requests");
                futures.add(executors.submit(new Client("" + i, db, randomContent.createGenerator(), numRequestsPerThread, entityType,
                        batchMeter)));
            }
            // Accumulate the results ...
            Results results = futures.stream()
                                     .map(LoadApp::getResults)
                                     .filter(r -> r != null)
                                     .collect(Collectors.reducing(Results::combine))
                                     .get();
            print(results);
        } finally {
            sw.stop();
            print("Completed with the following request rates over " + numThreads + " thread(s) in " + sw.totalDuration().getSeconds() + " seconds");
            print("  Mean rate:   " + new DecimalFormat("#,###,##0.0").format(batchMeter.getMeanRate()) + " batch/sec");
            print("  1 min rate:  " + new DecimalFormat("#,###,##0.0").format(batchMeter.getOneMinuteRate()) + " batch/sec");
            print("  5 min rate:  " + new DecimalFormat("#,###,##0.0").format(batchMeter.getFiveMinuteRate()) + " batch/sec");
            print("  15 min rate: " + new DecimalFormat("#,###,##0.0").format(batchMeter.getFifteenMinuteRate()) + " batch/sec");
            try {
                executors.shutdownNow();
            } catch (Throwable e) {
                error("Error shutting down executors: " + e.getMessage());
                exit(ReturnCode.EXECUTORS_SHUTDOWN_ERROR);
            } finally {
                try {
                    db.close();
                    //exit(ReturnCode.SUCCESS);
                } catch (Throwable e) {
                    error("Error shutting down Debezium database '" + dbName + "': " + e.getMessage());
                    exit(ReturnCode.DATABASE_SHUTDOWN_ERROR);
                } finally {
                    try {
                        client.shutdown(10, TimeUnit.SECONDS);
                    } catch (Throwable e) {
                        error("Error shutting down Debezium client: " + e.getMessage());
                        exit(ReturnCode.CLIENT_SHUTDOWN_FAILURE);
                    }
                }
            }
        }
    }

    protected static void exit(ReturnCode code) {
        System.exit(code.ordinal());
    }

    protected static Results getResults(Future<Results> future) {
        try {
            Results result = future.get();
            return result;
            // return future.get();
        } catch (InterruptedException | ExecutionException e) {
            print("Error getting results from client");
        }
        return null;
    }

    protected static Configuration readConfiguration(String path) {
        try {
            printVerbose("Looking for configuration on classpath at '" + path + "'");
            InputStream stream = Client.class.getClassLoader().getResourceAsStream(path);
            if (stream == null) {
                // Try path as-is ...
                Path filePath = FileSystems.getDefault().getPath(path).toAbsolutePath();
                printVerbose("Looking for configuration at '" + filePath + "'");
                File f = filePath.toFile();
                if (f == null || !f.exists() || f.isDirectory()) {
                    // Try relative to current working directory ...
                    Path current = FileSystems.getDefault().getPath(".").toAbsolutePath();
                    Path absolute = current.resolve(Paths.get(path)).toAbsolutePath();
                    printVerbose("Looking for configuration relative to '" + current + "' at '" + absolute + "'");
                    f = absolute.toFile();
                }
                if (f != null) {
                    path = f.getAbsolutePath();
                    stream = new FileInputStream(f);
                }
            }
            if (stream != null) {
                printVerbose("Parsing configuration file at '" + path + "'");
                return Debezium.configure(stream).build();
            }
        } catch (IOException e) {
            print("Unable to read Debezium client configuration file at '" + path + "': " + e.getMessage());
            exit(ReturnCode.UNABLE_TO_READ_CONFIGURATION);
        }
        print("Unable to read Debezium client configuration file at '" + path + "': file not found");
        exit(ReturnCode.UNABLE_TO_READ_CONFIGURATION);
        return null;
    }

    protected static void error(Object msg) {
        System.err.println(msg);
    }

    protected static void print(Object msg) {
        System.out.println(msg);
    }

    protected static void printVerbose(Object msg) {
        if (verbose) System.out.println(msg);
    }

    protected static void printUsage() {
        print("usage:  " + LoadApp.class.getSimpleName() + " [--version] [-?|--help] [-c|--config <path>]");
        print("          [-d|--db <database-name>] [-D|--device <device-id>]");
        print("          [-e|--entity-type <entity-type-name>] [-t|--threads <num-threads>]");
        print("          [-r|--requests-per-thread <num-requests-per-thread>] [-v|--verbose]");
        print("");
    }

    protected static Function<Configuration, Debezium.Client> clientFactory = new Function<Configuration, Debezium.Client>() {
        @Override
        public org.debezium.driver.Debezium.Client apply(Configuration config) {
            return Debezium.start(config);
        }
    };

}

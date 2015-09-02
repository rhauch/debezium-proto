/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.apache.kafka.clients.processor.PTopology;
import org.apache.kafka.clients.processor.ProcessorProperties;
import org.apache.kafka.stream.KStreamProcess;
import org.debezium.Debezium;
import org.debezium.annotation.NotThreadSafe;
import org.debezium.driver.Configuration;
import org.debezium.util.CommandLineOptions;
import org.debezium.util.IoUtil;

/**
 * An abstraction of a service that provides standard command-line processing, configuration file reading, and startup logic.
 * <p>
 * This class is normally used to run a Debezium service from within a main method:
 * 
 * <pre>
 * public static void main(String[] args) {
 *     ServiceRunner.use(EntityStorageService.class, EntityStorageTopology.class)
 *                  .setVersion(Debezium.getVersion())
 *                  .run(args);
 * }
 * </pre>
 * 
 * or, if a special class loader is needed:
 * 
 * <pre>
 * public static void main(String[] args) {
 *     ServiceRunner.use(&quot;EntityStorageService&quot;, EntityStorageTopology.class)
 *                  .setVersion(Debezium.getVersion())
 *                  .withClassLoader(myClassLoader)
 *                  .withOption('s', &quot;storage&quot;, &quot;The directory where the storage should be located&quot;)
 *                  .run(args);
 * }
 * </pre>
 * 
 * The {@link ServiceRunner} can be configured using any of the {@code set...} or {@code with...} methods. However, once the
 * {@link #run(String[])} method is called, the runner blocks forever.
 * <p>
 * To stop the service, simply kill the process.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class ServiceRunner {

    /**
     * Create a ServiceRunner instance with the application name and stream processing topology.
     * 
     * @param appName the application name; may not be null or empty
     * @param topology the stream processing topology class; may not be null
     * @return the service runner; never null
     */
    public static ServiceRunner use(String appName, Class<? extends PTopology> topology) {
        return new ServiceRunner(appName, topology);
    }

    /**
     * Create a ServiceRunner instance with the given application class and stream processing topology.
     * The resulting service runner uses the supplied class' class loader by default.
     * 
     * @param mainClass the main class; may not be null
     * @param topology the stream processing topology class; may not be null
     * @return the service runner; never null
     */
    public static ServiceRunner use(Class<?> mainClass, Class<? extends PTopology> topology) {
        return new ServiceRunner(mainClass.getSimpleName(), topology).withClassLoader(mainClass.getClassLoader());
    }

    private static final String DEFAULT_SYSTEM_PROPERTY_NAME_PREFIX = "DEBEZIUM_";

    protected static enum ReturnCode {
        SUCCESS,
        UNABLE_TO_READ_CONFIGURATION,
        CONFIGURATION_ERROR,
        ERROR_DURING_EXECUTION,
    }

    private boolean verbose = false;
    private final List<Option> options = new ArrayList<>();
    private final String appName;
    private final Class<? extends PTopology> topology;
    private ClassLoader classLoader = getClass().getClassLoader();
    private String systemPropertyNamePrefix = DEFAULT_SYSTEM_PROPERTY_NAME_PREFIX;
    private String version = Debezium.getVersion();
    private Consumer<ReturnCode> completionHandler;
    private Consumer<Throwable> errorHandler;
    private volatile Future<Void> running = null;

    protected ServiceRunner(String appName, Class<? extends PTopology> topology) {
        if (appName == null || appName.trim().isEmpty())
            throw new IllegalArgumentException("The application name may not be null or empty");
        if (topology == null) throw new IllegalArgumentException("The topology class may not be null");
        this.appName = appName;
        this.topology = topology;
        this.completionHandler = this::exit;
        this.errorHandler = this::recordError;
        withOption('c', "config", "The path to the configuration file");
    }
    
    @Override
    public int hashCode() {
        return getName().hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if ( obj == this ) return true;
        if ( obj instanceof ServiceRunner ) {
            ServiceRunner that = (ServiceRunner) obj;
            return Objects.equals(this.getName(), that.getName()) && Objects.equals(this.getVersion(),that.getVersion());
        }
        return super.equals(obj);
    }
    
    /**
     * Get the name of this application or service name.
     * @return the name; never null
     */
    public String getName() {
        return appName;
    }

    /**
     * Get the version exposed by the application.
     * 
     * @return the version; never null
     */
    public String getVersion() {
        return version;
    }

    /**
     * Set the version exposed by the application.
     * 
     * @param version the version; may not be null
     * @return this service runner instance so that methods can be chained together; never null
     */
    public ServiceRunner setVersion(String version) {
        if (version == null || version.trim().isEmpty()) throw new IllegalArgumentException("The version may not be null or empty");
        this.version = version;
        return this;
    }

    /**
     * Get the prefix that identify the system properties to include when overwriting configuration properties.
     * 
     * @return the prefix; never null
     */
    public String getSystemPropertyPrefix() {
        return systemPropertyNamePrefix;
    }

    /**
     * Set the prefix that identify the system properties to include when overwriting configuration properties.
     * 
     * @param prefix the property name prefix
     * @return this service runner instance so that methods can be chained together; never null
     */
    public ServiceRunner setSystemPropertyPrefix(String prefix) {
        this.systemPropertyNamePrefix = prefix == null ? "" : prefix;
        return this;
    }

    /**
     * Update the service runner to include in the usage statement a command line option with the given name and description.
     * 
     * @param name the name of the option; usually this is a multi-character value that when used on the command line the name is
     *            prefixed with "--"
     * @param description the description of the option
     * @return this service runner instance so that methods can be chained together; never null
     */
    public ServiceRunner withOption(String name, String description) {
        return withOption((char) 0, name, description);
    }

    /**
     * Update the service runner to include in the usage statement a command line option with the given flag and description.
     * 
     * @param flag the single-character shortcut or flag of the option; usually when used on the command line the flag is prefixed
     *            with "-"
     * @param description the description of the option
     * @return this service runner instance so that methods can be chained together; never null
     */
    public ServiceRunner withOption(char flag, String description) {
        return withOption(flag, null, description);
    }

    /**
     * Update the service runner to include in the usage statement a command line option with the given flag and/or name, and
     * description.
     * 
     * @param flag the single-character shortcut or flag of the option; usually when used on the command line the flag is prefixed
     *            with "-"
     * @param name the name of the option; usually this is a multi-character value that when used on the command line the name is
     *            prefixed with "--"
     * @param description the description of the option
     * @return this service runner instance so that methods can be chained together; never null
     */
    public ServiceRunner withOption(char flag, String name, String description) {
        if (flag == 0 && name == null) throw new IllegalArgumentException("The option must have a flag or name");
        Option option = new Option(flag, name, description);
        if (!this.options.contains(option)) this.options.add(option);
        return this;
    }

    /**
     * Set the class loader that should be used to load the service and topology classes. By default this is set to the same
     * class loader that loaded this class.
     * 
     * @param classLoader the class loader; may not be null
     * @return this service runner instance so that methods can be chained together; never null
     */
    public ServiceRunner withClassLoader(ClassLoader classLoader) {
        if (classLoader == null) throw new IllegalArgumentException("The class loader may not be null");
        this.classLoader = classLoader;
        return this;
    }

    /**
     * Register the function that will be called with the response code when {@link #run(String[])} completes. The default handler
     * will call {@link System#exit(int)}.
     * 
     * @param completionHandler the function that will be called with the response code when {@link #run(String[])} completes; if
     *            null then the default handler will be used to call {@link System#exit(int)}.
     * @return this service runner instance so that methods can be chained together; never null
     */
    public ServiceRunner withCompletionHandler(Consumer<ReturnCode> completionHandler) {
        this.completionHandler = completionHandler != null ? completionHandler : this::exit;
        return this;
    }

    /**
     * Register the function that will be called if an error occurs during execution during {@link #run(Properties)} or
     * {@link #run(String[])}.
     * The default handler will write to {@link System#out}.
     * 
     * @param errorHandler the function that will be called when {@link #run(String[])} or {@link #run(Properties)} throws an
     *            exception; if null then the default handler will be used to call {@link System#exit(int)}.
     * @return this service runner instance so that methods can be chained together; never null
     */
    public ServiceRunner withErrorHandler(Consumer<Throwable> errorHandler) {
        this.errorHandler = errorHandler != null ? errorHandler : this::recordError;
        return this;
    }

    /**
     * Run the service by reading the configuration, applying any system properties that match the configuration property rule,
     * and that starts the Kafka Streams framework to consume and process the desired topics.
     * <p>
     * This method block until the thread calling it is interrupted or until service is {@link #shutdown(long, TimeUnit)}.
     * 
     * @param args the command line arguments
     * @see #run(Properties)
     * @see #run(Properties, ExecutorService)
     * @see #shutdown(long, TimeUnit)
     * @see #isRunning()
     */
    public synchronized void run(String[] args) {
        if (running != null) return;
        try {
            running = new CompletableFuture();
            Configuration config = null;
            try {
                final CommandLineOptions options = CommandLineOptions.parse(args);
                if (options.getOption("-?", "--help", false) || options.hasParameter("help")) {
                    printUsage();
                    completionHandler.accept(ReturnCode.SUCCESS);
                    return;
                }
                if (options.hasOption("--version")) {
                    print(getClass().getSimpleName() + " version " + version);
                    completionHandler.accept(ReturnCode.SUCCESS);
                    return;
                }
                final String pathToConfigFile = options.getOption("-c", "--config", "debezium.json");
                verbose = options.getOption("-v", "--verbose", false);

                config = Configuration.load(pathToConfigFile, classLoader,this::printVerbose);
                if (config.isEmpty()) {
                    print("Unable to read Debezium client configuration file at '" + pathToConfigFile + "': file not found");
                    completionHandler.accept(ReturnCode.UNABLE_TO_READ_CONFIGURATION);
                    return;
                }

                printVerbose("Found configuration at " + pathToConfigFile + ":");
                printVerbose(config);

                // Adjust the properties by setting any system properties to the configuration ...
                printVerbose("Applying system properties to configuration");
                config = config.withSystemProperties(DEFAULT_SYSTEM_PROPERTY_NAME_PREFIX);
                printVerbose(config);
            } catch (Throwable t) {
                print("Unexpected exception while processing the configuration: " + t.getMessage());
                t.printStackTrace();
                completionHandler.accept(ReturnCode.CONFIGURATION_ERROR);
                return;
            }

            try {
                execute(config);
                completionHandler.accept(ReturnCode.SUCCESS);
            } catch (Throwable t) {
                errorHandler.accept(t);
                completionHandler.accept(ReturnCode.ERROR_DURING_EXECUTION);
            }
        } finally {
            ((CompletableFuture) running).complete();
        }
    }

    /**
     * Run the service by reading the configuration, applying any system properties that match the configuration property rule,
     * and that starts the Kafka Streams framework to consume and process the desired topics.
     * <p>
     * This method block until the thread calling it is interrupted or until service is {@link #shutdown(long, TimeUnit)}.
     * 
     * @param config the configuration properties for the service
     * @see #run(String[])
     * @see #run(Properties, ExecutorService)
     * @see #shutdown(long, TimeUnit)
     * @see #isRunning()
     */
    public synchronized void run(Properties config) {
        if (running != null) return;
        try {
            running = new CompletableFuture();
            try {
                // Start the stream processing framework ...
                execute(Configuration.from(config));
                completionHandler.accept(ReturnCode.SUCCESS);
            } catch (Throwable t) {
                errorHandler.accept(t);
                completionHandler.accept(ReturnCode.ERROR_DURING_EXECUTION);
            }
        } finally {
            ((CompletableFuture) running).complete();
        }
    }

    /**
     * Run the service by reading the configuration, applying any system properties that match the configuration property rule,
     * and that starts the Kafka Streams framework to consume and process the desired topics.
     * <p>
     * This method block until the thread calling it is interrupted or until service is {@link #shutdown(long, TimeUnit)}.
     * 
     * @param config the configuration properties for the service; may be null
     * @param executor the executor that should be used to asynchronously run the service
     * @see #run(String[])
     * @see #run(Properties)
     * @see #shutdown(long, TimeUnit)
     * @see #isRunning()
     */
    public synchronized void run(Properties config, ExecutorService executor) {
        if (running != null) return;
        running = executor.submit(() -> {
            try {
                // Start the stream processing framework ...
                execute(Configuration.from(config));
                completionHandler.accept(ReturnCode.SUCCESS);
            } catch (Throwable t) {
                errorHandler.accept(t);
                completionHandler.accept(ReturnCode.ERROR_DURING_EXECUTION);
            }
            return null;
        });
    }

    /**
     * The internal method that actually executes the streaming process. This method will block until the thread that is calling
     * it is interrupted or until service is {@link #shutdown(long, TimeUnit)}.
     * 
     * @param config the configuration properties for the service
     * @throws Exception if there is a problem configuring or executing the streaming process
     */
    private void execute(Configuration config) throws Exception {
        // Start the stream processing framework ...
        ProcessorProperties processorProps = new ProcessorProperties(config.asProperties());
        if ( processorProps.timestampExtractor() == null ) {
            // Use the system time extractor ...
            processorProps.timestampExtractor((topic,key,value)->System.currentTimeMillis());
        }
        KStreamProcess streaming = new KStreamProcess(topology,processorProps);
        printVerbose("Starting Kafka streaming process using topology " + topology.getName());
        streaming.run(); // must be interrupted to complete, but the interrupt is caught and handled within run()
        completionHandler.accept(ReturnCode.SUCCESS);
    }

    /**
     * Determine if this service runner is still running.
     * 
     * @return true if its still running, or false otherwise
     * @see #run(String[])
     * @see #run(Properties)
     * @see #run(Properties, ExecutorService)
     * @see #shutdown(long, TimeUnit)
     */
    public boolean isRunning() {
        return running != null;
    }

    /**
     * Shutdown the service and wait for it to complete.
     * 
     * @param timeout the amount of time to wait for completion
     * @param unit the unit of time for the timeout; may not be null
     * @return true if the service was or is shutdown, or false otherwise
     * @see #run(String[])
     * @see #isRunning()
     */
    public boolean shutdown(long timeout, TimeUnit unit) {
        Future<Void> future = this.running;
        if ( future != null ) {
            future.cancel(true);
            try {
                future.get(timeout,unit);
            } catch (InterruptedException e) {
                // Had to interrupt the thread, but it still stopped ...
            } catch ( ExecutionException e ) {
                // Should not happen, but just in case ...
                recordError(e);
            } catch ( TimeoutException e ) {
                // Could not shut it down in time ...
                return false;
            }
        }
        // There was no thread or no latch, so it's shutdown ...
        return true;
    }

    private void exit(ReturnCode code) {
        print("Exiting " + appName);
        System.exit(code.ordinal());
    }

    private void recordError(Throwable error) {
        print("Error while running Kafka streaming process: " + error.getMessage());
        error.printStackTrace();
    }

    protected Properties readConfiguration(String path) {
        try (InputStream stream = IoUtil.getResourceAsStream(path, classLoader, null, "configuration file", this::printVerbose)) {
            if (stream != null) {
                Properties props = new Properties();
                props.load(stream);
                return props;
            }
        } catch (IOException e) {
            error(e.getMessage());
        }
        return null;
    }

    protected void error(Object msg) {
        System.err.println(msg);
    }

    protected void print(Object msg) {
        System.out.println(msg);
    }

    protected void printVerbose(Object msg) {
        if (verbose) System.out.println(msg);
    }

    protected void printUsage() {
        options.sort(Option::compareTo);
        print("usage:  " + appName + " [--version] [-?|--help]");
        Iterator<Option> iter = options.iterator();
        while (iter.hasNext()) {
            String optionA = iter.next().toString();
            String optionB = iter.hasNext() ? " " + iter.next().toString() : "";
            print("        " + optionA + optionB);
        }
        print("");
    }

    private static class Option implements Comparable<Option> {
        private final char flag;
        private final String name;
        private final String description;

        protected Option(char flag, String name, String description) {
            this.flag = flag;
            this.name = name != null && name.trim().length() != 0 ? name : null;
            this.description = description != null && description.trim().length() != 0 ? description : null;
            assert this.flag != 0 || this.name != null;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            boolean started = false;
            sb.append('[');
            if (flag != 0) {
                sb.append("-").append(flag);
                started = true;
            }
            if (name != null) {
                if (started) sb.append("|");
                sb.append("--").append(name);
                started = true;
            }
            if (description != null) {
                if (started) sb.append(' ');
                sb.append(description).append('>');
            }
            sb.append(']');
            return sb.toString();
        }

        @Override
        public int hashCode() {
            return this.flag;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj instanceof Option) {
                Option that = (Option) obj;
                return this.compareTo(that) == 0;
            }
            return false;
        }

        @Override
        public int compareTo(Option that) {
            if (this == that) return 0;
            if (this.flag != 0) {
                if (that.flag != 0) return this.flag - that.flag;
                return -1;
            } else if (that.flag != 0) {
                return 1;
            }
            if (this.name != null) {
                if (that.name != null) return this.name.compareTo(that.name);
                return -1;
            } else if (that.name != null) {
                return 1;
            }
            return 0;
        }
    }

    private static final class CompletableFuture implements Future<Void> {
        private final CountDownLatch runningLatch = new CountDownLatch(1);
        private volatile boolean cancelled = false;
        private volatile Thread runningThread = Thread.currentThread();

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (runningLatch.getCount() > 0 && !this.cancelled) {
                // Interrupt the thread being used to run the service; the KStreamProcess.run will catch the interrupt and
                // complete normally
                this.cancelled = true;
                runningThread.interrupt();
                return true;
            }
            return false;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return runningLatch.getCount() < 1;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            runningLatch.await();
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            runningLatch.await(timeout, unit);
            return null;
        }

        public void complete() {
            try {
                runningLatch.countDown();
            } finally {
                runningThread = null;
            }
        }
    }

}

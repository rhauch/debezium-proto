/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.debezium.core.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Randall Hauch
 *
 */
class Environment {

    public static Environment create(Configuration config) {
        return new Environment(Environment::createExecutor, Environment::createScheduler, createFoundation(config));
    }

    public static Environment create(Function<Supplier<Executor>, Foundation> foundationFactory ) {
        return new Environment(Environment::createExecutor, Environment::createScheduler, foundationFactory);
    }

    public static Environment create(ExecutorService executor, ScheduledExecutorService scheduler, Foundation foundation ) {
        return new Environment(()->executor,()->scheduler,(exec)->foundation);
    }

    public static Environment create(ExecutorService executor, ScheduledExecutorService scheduler, Function<Supplier<Executor>, Foundation> foundationFactory ) {
        return new Environment(Environment::createExecutor, Environment::createScheduler, foundationFactory);
    }

    protected static ExecutorService createExecutor() {
        boolean useDaemonThreads = false; // they will keep the VM running if not shutdown properly
        ThreadFactory threadFactory = new NamedThreadFactory("debezium", "consumer", useDaemonThreads);
        return Executors.newCachedThreadPool(threadFactory);
    }

    protected static ScheduledExecutorService createScheduler() {
        ThreadFactory scheduledThreadFactory = new NamedThreadFactory("debezium", "timer", true);
        return Executors.newScheduledThreadPool(0, scheduledThreadFactory);
    }
    
    protected static Function<Supplier<Executor>, Foundation> createFoundation( Configuration config ) {
        return execSupplier -> new KafkaFoundation(config, execSupplier);
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Function<Supplier<Executor>, Foundation> foundationSupplier;
    private final Supplier<ExecutorService> executorSupplier;
    private final Supplier<ScheduledExecutorService> scheduledExecutorSupplier;
    private final AtomicReference<ExecutorService> executor = new AtomicReference<>();
    private final AtomicReference<ScheduledExecutorService> scheduledExecutor = new AtomicReference<>();
    private final AtomicReference<Foundation> foundation = new AtomicReference<>();

    private Environment(Supplier<ExecutorService> executorSupplier,
            Supplier<ScheduledExecutorService> scheduledExecutorSupplier,
            Function<Supplier<Executor>, Foundation> foundationSupplier) {
        this.executorSupplier = executorSupplier;
        this.scheduledExecutorSupplier = scheduledExecutorSupplier;
        this.foundationSupplier = foundationSupplier;
    }

    public Foundation getFoundation() {
        return foundation.updateAndGet(existing -> existing != null ? existing : foundationSupplier.apply(this::getExecutor));
    }

    public Executor getExecutor() {
        return executor.updateAndGet(existing -> existing != null ? existing : executorSupplier.get());
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor.updateAndGet(existing -> existing != null ? existing : scheduledExecutorSupplier.get());
    }

    public void shutdown(long timeout, TimeUnit unit) {
        try {
            scheduledExecutor.updateAndGet(existing -> {
                if (existing != null) {
                    logger.debug("Beginning shutdown of scheduledExecutor");
                    existing.shutdown();
                    logger.debug("Completed shutdown of scheduledExecutor");
                }
                return null;
            });
        } finally {
            executor.updateAndGet(existing -> {
                if (existing != null) {
                    try {
                        logger.debug("Beginning shutdown of executor service");
                        existing.shutdown();
                        logger.debug("Awaiting termination of executor service for {} {}",timeout,unit.name());
                        existing.awaitTermination(timeout, unit);
                        logger.debug("Completed shutdown of executor service");
                    } catch (InterruptedException e) {
                        // We were interrupted while blocking, so clear the status ...
                        Thread.interrupted();
                        logger.debug("Interrupted while awaiting termination of executor service, so continuing");
                    }
                }
                return null;
            });
        }
    }
}

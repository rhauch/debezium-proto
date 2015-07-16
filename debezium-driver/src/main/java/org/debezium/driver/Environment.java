/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Randall Hauch
 */
final class Environment {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Function<Supplier<Executor>, MessageBus> busSupplier;
    private final Supplier<ExecutorService> executorSupplier;
    private final Supplier<ScheduledExecutorService> scheduledExecutorSupplier;
    private final Supplier<SecurityProvider> securitySupplier;
    private final AtomicReference<ExecutorService> executor = new AtomicReference<>();
    private final AtomicReference<ScheduledExecutorService> scheduledExecutor = new AtomicReference<>();
    private final AtomicReference<MessageBus> bus = new AtomicReference<>();
    private final AtomicReference<SecurityProvider> security = new AtomicReference<>();

    Environment(Supplier<SecurityProvider> securitySupplier,
            Supplier<ExecutorService> executorSupplier,
            Supplier<ScheduledExecutorService> scheduledExecutorSupplier,
            Function<Supplier<Executor>, MessageBus> busSupplier) {
        assert securitySupplier != null;
        assert executorSupplier != null;
        assert scheduledExecutorSupplier != null;
        assert busSupplier != null;
        this.executorSupplier = executorSupplier;
        this.scheduledExecutorSupplier = scheduledExecutorSupplier;
        this.busSupplier = busSupplier;
        this.securitySupplier = securitySupplier;
    }

    public MessageBus getMessageBus() {
        return bus.updateAndGet(existing -> existing != null ? existing : busSupplier.apply(this::getExecutor));
    }

    public Executor getExecutor() {
        return executor.updateAndGet(existing -> existing != null ? existing : executorSupplier.get());
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor.updateAndGet(existing -> existing != null ? existing : scheduledExecutorSupplier.get());
    }

    public SecurityProvider getSecurity() {
        return security.updateAndGet(existing -> existing != null ? existing : securitySupplier.get());
    }

    public void shutdown(long timeout, TimeUnit unit) {
        try {
            security.updateAndGet(existing -> {
                if (existing != null) {
                    logger.debug("Beginning shutdown of security provider '{}'", existing.getName());
                    existing.shutdown();
                    logger.debug("Completed shutdown of security provider '{}'", existing.getName());
                }
                return null;
            });
        } finally {
            try {
                bus.updateAndGet(existing -> {
                    if (existing != null) {
                        logger.debug("Beginning shutdown of message bus '{}'", existing.getName());
                        existing.shutdown();
                        logger.debug("Completed shutdown of message bus '{}'", existing.getName());
                    }
                    return null;
                });
            } finally {
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
                                logger.debug("Awaiting termination of executor service for {} {}", timeout, unit.name());
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
    }
}

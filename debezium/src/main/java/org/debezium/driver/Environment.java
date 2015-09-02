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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.debezium.annotation.NotThreadSafe;
import org.debezium.annotation.ThreadSafe;
import org.debezium.util.Clock;
import org.debezium.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Representation of the environment in which a DebeziumDriver is run. The environment will have consistent references to all
 * components until the environment is {@link #shutdown(long, TimeUnit) shutdown}, at which time all contained components will
 * be shutdown (if needed) and released. The environment can be used again, as contained components will be recreated as required.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
public final class Environment {

    /**
     * Obtain a new builder to assemble and create a new Environment.
     * 
     * @return the Environment builder; never null
     */
    public static Builder build() {
        return new EnvironmentBuilder();
    }

    /**
     * An interface used to build a new {@link Environment} instance. By default, Environment instances will use a pass-through
     * security provider, a Kafka message bus, a {@link Executors#newCachedThreadPool(ThreadFactory) cached thread pool Executor},
     * and a {@link Executors#newScheduledThreadPool(int) ScheduledExecutor} that maintains 0 core threads.
     * 
     * @see Environment#build()
     */
    @NotThreadSafe
    public static interface Builder {

        /**
         * Specify the name of the environment, which is entirely optional and used primarily for logging. If a name is not
         * explicitly provided, the Environment will be given a name that is unique within this JVM.
         * 
         * @param name the desired name; may be null if an auto-generated unique name is to be used
         * @return this builder instance for chaining together methods; never null
         */
        Builder withName(String name);

        /**
         * Specify the {@link SecurityProvider} implementation that should be used.
         * 
         * @param security the function that supplies a security provider implementation; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        Builder withSecurity(Supplier<SecurityProvider> security);

        /**
         * Specify the {@link MessageBus} implementation that should be used.
         * 
         * @param messageBus the function that supplies the message bus implementation; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        Builder withBus(BiFunction<Supplier<Configuration>, Supplier<Executor>, MessageBus> messageBus);

        /**
         * Specify the {@link ExecutorService} implementation that should be used.
         * 
         * @param executor the function that supplies the executor service implementation; may be null if the default
         *            implementation is to be used
         * @return this builder instance for chaining together methods; never null
         */
        Builder withExecutor(Supplier<ExecutorService> executor);

        /**
         * Specify the {@link ScheduledExecutorService} implementation that should be used.
         * 
         * @param executor the function that supplies the scheduled executor service implementation; may be null if the default
         *            implementation is to be used
         * @return this builder instance for chaining together methods; never null
         */
        Builder withScheduledExecutor(Supplier<ScheduledExecutorService> executor);

        /**
         * Specify the {@link Clock} implementation that should be used.
         * 
         * @param clock the function that supplies the {@link Clock} implementation; may be null if the default implementation is
         *            to be used
         * @return this builder instance for chaining together methods; never null
         */
        Builder withClock(Supplier<Clock> clock);

        /**
         * Create and return a new Environment using the resources assigned to this builder.
         * 
         * @return the new environment; never null
         */
        Environment create();
    }

    @NotThreadSafe
    private static final class EnvironmentBuilder implements Builder {
        private BiFunction<Supplier<Configuration>, Supplier<Executor>, MessageBus> bus;
        private Supplier<ExecutorService> executor;
        private Supplier<ScheduledExecutorService> scheduledExecutor;
        private Supplier<SecurityProvider> security;
        private Supplier<Clock> clock;
        private String name;

        @Override
        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        @Override
        public Builder withBus(BiFunction<Supplier<Configuration>, Supplier<Executor>, MessageBus> messageBus) {
            this.bus = messageBus;
            return this;
        }

        @Override
        public Builder withExecutor(Supplier<ExecutorService> executor) {
            this.executor = executor;
            return this;
        }

        @Override
        public Builder withScheduledExecutor(Supplier<ScheduledExecutorService> executor) {
            this.scheduledExecutor = executor;
            return this;
        }

        @Override
        public Builder withSecurity(Supplier<SecurityProvider> security) {
            this.security = security;
            return this;
        }

        @Override
        public Builder withClock(Supplier<Clock> clock) {
            this.clock = clock;
            return this;
        }

        @Override
        public Environment create() {
            return new Environment(name, security, executor, scheduledExecutor, bus, clock);
        }

    }

    private static ExecutorService createDefaultExecutor() {
        return Executors.newCachedThreadPool(NAMED_THREAD_FACTORY);
    }

    private static ScheduledExecutorService createDefaultScheduledExecutor() {
        return Executors.newScheduledThreadPool(0, NAMED_THREAD_FACTORY);
    }

    private static final ThreadFactory NAMED_THREAD_FACTORY = new NamedThreadFactory("debezium-driver");
    private static final AtomicLong INSTANCE_COUNTER = new AtomicLong();

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String name;
    private final BiFunction<Supplier<Configuration>, Supplier<Executor>, MessageBus> busSupplier;
    private final Supplier<ExecutorService> executorSupplier;
    private final Supplier<ScheduledExecutorService> scheduledExecutorSupplier;
    private final Supplier<SecurityProvider> securitySupplier;
    private final Supplier<Clock> clockSupplier;
    private final AtomicReference<ExecutorService> executor = new AtomicReference<>();
    private final AtomicReference<ScheduledExecutorService> scheduledExecutor = new AtomicReference<>();
    private final AtomicReference<MessageBus> bus = new AtomicReference<>();
    private final AtomicReference<SecurityProvider> security = new AtomicReference<>();
    private final AtomicReference<Clock> clock = new AtomicReference<>();

    Environment(String name, Supplier<SecurityProvider> securitySupplier,
            Supplier<ExecutorService> executorSupplier,
            Supplier<ScheduledExecutorService> scheduledExecutorSupplier,
            BiFunction<Supplier<Configuration>, Supplier<Executor>, MessageBus> busSupplier,
            Supplier<Clock> clockSupplier) {
        this.name = name != null ? name : "Environment" + INSTANCE_COUNTER.incrementAndGet();
        this.executorSupplier = executorSupplier != null ? executorSupplier : Environment::createDefaultExecutor;
        this.scheduledExecutorSupplier = scheduledExecutorSupplier != null ? scheduledExecutorSupplier
                : Environment::createDefaultScheduledExecutor;
        this.busSupplier = busSupplier != null ? busSupplier : KafkaMessageBus::new;
        this.securitySupplier = securitySupplier != null ? securitySupplier : PassthroughSecurityProvider::new;
        this.clockSupplier = clockSupplier != null ? clockSupplier : Clock::system;
    }

    /**
     * Get the name of this environment.
     * 
     * @return the name; never null
     */
    public String getName() {
        return name;
    }

    /**
     * Get this environment's {@link Clock}.
     * 
     * @return the clock; never null
     */
    public Clock getClock() {
        return clock.updateAndGet(existing -> existing != null ? existing : clockSupplier.get());
    }

    /**
     * Get this environment's {@link MessageBus} given the supplied configuration.
     * 
     * @param config the supplier for the configuration for the message bus; may not be null
     * @return the message bus; never null
     */
    public MessageBus getMessageBus(Supplier<Configuration> config) {
        return bus.updateAndGet(existing -> existing != null ? existing : busSupplier.apply(config, this::getExecutor));
    }

    /**
     * Get this environment's {@link MessageBus} given the supplied configuration.
     * 
     * @param config the configuration for the message bus; may not be null
     * @return the message bus; never null
     */
    public MessageBus getMessageBus(Configuration config) {
        return getMessageBus(() -> config);
    }

    /**
     * Get this environment's {@link ExecutorService}.
     * 
     * @return the executor service; never null
     */
    public ExecutorService getExecutor() {
        return executor.updateAndGet(existing -> existing != null ? existing : executorSupplier.get());
    }

    /**
     * Get this environment's {@link ScheduledExecutorService}.
     * 
     * @return the scheduled executor service; never null
     */
    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor.updateAndGet(existing -> existing != null ? existing : scheduledExecutorSupplier.get());
    }

    /**
     * Get this environment's {@link SecurityProvider}.
     * 
     * @return the security provider; never null
     */
    public SecurityProvider getSecurity() {
        return security.updateAndGet(existing -> existing != null ? existing : securitySupplier.get());
    }

    public void shutdown(long timeout, TimeUnit unit) {
        try {
            security.updateAndGet(existing -> {
                if (existing != null) {
                    logger.debug("{} shutdown: beginning shutdown of security provider '{}'", name, existing.getName());
                    existing.shutdown();
                    logger.debug("{} shutdown: completed shutdown of security provider '{}'", name, existing.getName());
                }
                return null;
            });
        } finally {
            try {
                bus.updateAndGet(existing -> {
                    if (existing != null) {
                        logger.debug("{} shutdown: beginning shutdown of message bus '{}'", name, existing.getName());
                        existing.shutdown();
                        logger.debug("{} shutdown: completed shutdown of message bus '{}'", name, existing.getName());
                    }
                    return null;
                });
            } finally {
                try {
                    scheduledExecutor.updateAndGet(existing -> {
                        if (existing != null) {
                            logger.debug("{} shutdown: beginning shutdown of scheduled executor", name);
                            existing.shutdown();
                            logger.debug("{} shutdown: completed shutdown of scheduled executor", name);
                        }
                        return null;
                    });
                } finally {
                    executor.updateAndGet(existing -> {
                        if (existing != null) {
                            try {
                                logger.debug("{} shutdown: beginning shutdown of executor service", name);
                                existing.shutdown();
                                logger.debug("{} shutdown: awaiting termination of executor service for {} {}", name, timeout, unit.name());
                                existing.awaitTermination(timeout, unit);
                                logger.debug("{} shutdown: completed shutdown of executor service", name);
                            } catch (InterruptedException e) {
                                // We were interrupted while blocking, so clear the status ...
                                Thread.interrupted();
                                logger.debug("{} shutdown: interrupted while awaiting termination of executor service, so continuing", name);
                            }
                        }
                        return null;
                    });
                }
            }
        }
    }
}

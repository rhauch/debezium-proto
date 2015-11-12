/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.debezium.annotation.ThreadSafe;
import org.debezium.util.Iterators;

/**
 * An interface that defines the authentication and authorization checks used within a {@link DebeziumDriver Debezium driver}.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
public interface SecurityProvider {

    /**
     * Representation of a set of actions against a single database.
     * <p>
     * The {@link #equals(Object)} method compares only the {@link #databaseId() database ID}, and thus can be used to
     * ensure that multiple {@link Action} instances for the same database are easily identified. For example, {@link Action}
     * instances can be put into a hash set, and only the latest Action for a given database will be kept.
     * <p>
     * The {@link #compareTo(Action)} method, on the other hand, takes into account the {@link #databaseId() database ID}
     * and the set of individual actions.
     * @author Randall Hauch
     */
    public static final class Action implements Comparable<Action> {
        // For convenience, these are ordered with least privilege to greatest ...
        private static final int READ_CONTENTS = 1 << 0;
        private static final int WRITE_CONTENTS = 1 << 1;
        private static final int ADMINISTER_DB = 1 << 2;
        private static final int PROVISION_DB = 1 << 3;
        private static final int DESTROY_DB = 1 << 4;

        private final String databaseId;
        private final byte actions;

        /**
         * Create a new action for the given database but without any privileges. The result can be used to create actions
         * with privileges via the {@link #withRead()}, {@link #withWrite()}, {@link #withProvision()}, {@link #withAdminister()},
         * and {@link #withDestroy()} methods.
         * @param databaseId the identifier of the database to which this action is to apply; may not be null
         */
        public Action(String databaseId) {
            if ( databaseId == null ) throw new IllegalArgumentException("The database identifier may not be null");
            this.databaseId = databaseId;
            this.actions = (byte) 0;
        }

        private Action(String databaseId, int actions) {
            assert databaseId != null;
            this.databaseId = databaseId;
            this.actions = (byte) actions;
        }

        /**
         * Get the identifier of the database to which this action applies.
         * @return the database identifier; never null
         */
        public String databaseId() {
            return databaseId;
        }

        /**
         * Determine whether this action includes reading content from the database.
         * @return {@code true} if this action includes reading, or {@code false} otherwise
         */
        public boolean canRead() {
            return (actions & READ_CONTENTS) == READ_CONTENTS;
        }

        /**
         * Determine whether this action includes creating, updating, and removing content from the database.
         * @return {@code true} if this action includes writing, or {@code false} otherwise
         */
        public boolean canWrite() {
            return (actions & WRITE_CONTENTS) == WRITE_CONTENTS;
        }

        /**
         * Determine whether this action includes provisioning the new database.
         * @return {@code true} if this action includes provisioning a new database, or {@code false} otherwise
         */
        public boolean canProvision() {
            return (actions & PROVISION_DB) == PROVISION_DB;
        }

        /**
         * Determine whether this action includes administering the database.
         * @return {@code true} if this action includes administration, or {@code false} otherwise
         */
        public boolean canAdminister() {
            return (actions & ADMINISTER_DB) == ADMINISTER_DB;
        }

        /**
         * Determine whether this action includes destroying the the database.
         * @return {@code true} if this action includes destroying the database, or {@code false} otherwise
         */
        public boolean canDestroy() {
            return (actions & DESTROY_DB) == DESTROY_DB;
        }

        /**
         * Create a new action that is exactly this action plus reading content.
         * @return the new action that includes reading, or this action if it already includes reading
         */
        public Action withRead() {
            if (canRead()) return this;
            return new Action(this.databaseId, this.actions | READ_CONTENTS);
        }

        /**
         * Create a new action that is exactly this action plus writing content.
         * @return the new action that includes writing, or this action if it already includes writing
         */
        public Action withWrite() {
            if (canWrite()) return this;
            return new Action(this.databaseId, this.actions | WRITE_CONTENTS);
        }

        /**
         * Create a new action that is exactly this action plus provisioning the database.
         * @return the new action that includes provisioning, or this action if it already includes provisioning
         */
        public Action withProvision() {
            if (canProvision()) return this;
            return new Action(this.databaseId, this.actions | PROVISION_DB);
        }

        /**
         * Create a new action that is exactly this action plus administering the database.
         * @return the new action that includes administration, or this action if it already includes administration
         */
        public Action withAdminister() {
            if (canAdminister()) return this;
            return new Action(this.databaseId, this.actions | ADMINISTER_DB);
        }

        /**
         * Create a new action that is exactly this action plus destroying the database.
         * @return the new action that includes destroying the database, or this action if it already includes destroying the database
         */
        public Action withDestroy() {
            if (canDestroy()) return this;
            return new Action(this.databaseId, this.actions | DESTROY_DB);
        }

        private Action add( int action ) {
            return new Action(databaseId, this.actions | action);
        }

        @Override
        public int hashCode() {
            return databaseId.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj instanceof Action) {
                return databaseId.equals(((Action) obj).databaseId());
            }
            return false;
        }
        
        @Override
        public int compareTo(Action that) {
            int diff = this.databaseId.compareTo(that.databaseId());
            if ( diff != 0 ) return diff;
            return this.actions - that.actions;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(databaseId()).append(":");
            if ( canRead() ) sb.append("r");
            if ( canWrite() ) sb.append("w");
            if ( canAdminister() ) sb.append("a");
            if ( canProvision() ) sb.append("p");
            if ( canDestroy() ) sb.append("d");
            return sb.toString();
        }
    }

    /**
     * Get the name of this security provider, for logging and reporting purposes.
     * 
     * @return the provider's name; may not be null
     */
    public String getName();

    /**
     * Authenticate the named user for the given database.
     * 
     * @param username the username
     * @param device the device identifier or token
     * @param appVersion the version of the application
     * @param existingDatabaseIds the set of existing database IDs that match those in {@code databaseIds}; never null
     * @param databaseIds the identifier of the database(s) for which the user is to be authenticated
     * @return the session token for the user, or null if the user is not authorized
     */
    public SessionToken authenticate(String username, String device, String appVersion, Set<String> existingDatabaseIds, String... databaseIds);

    /**
     * Check whether the specified actions are allowed for a given token.
     * 
     * @param token the session token for the user; may not be null
     * @param actions the proposed actions; may not be null
     * @return the composite action used to record the actions that are to be checked; never null
     */
    public String check(SessionToken token, Iterator<Action> actions);

    /**
     * Check whether the specified actions are allowed for a given token.
     * 
     * @param token the session token for the user; may not be null
     * @param actions the proposed actions; may not be null
     * @return the composite action used to record the actions that are to be checked; never null
     */
    default public String check(SessionToken token, Iterable<Action> actions) {
        return check(token, actions.iterator());
    }

    /**
     * Check whether the specified action is allowed for a given token.
     * 
     * @param token the session token for the user; may not be null
     * @param action the proposed action; may not be null
     * @return the composite action used to record the actions that are to be checked; never null
     */
    default public String check(SessionToken token, Action action) {
        return check(token, Iterators.with(action));
    }

    /**
     * Check whether one or more actions are allowed for a given token.
     * 
     * @param token the session token for the user; may not be null
     * @return the composite action used to record the actions that are to be checked; never null
     */
    default public CompositeAction check(SessionToken token) {
        ConcurrentMap<String, Action> actionsByDb = new ConcurrentHashMap<>();
        return new CompositeAction() {
            private void merge( String databaseId, int action ) {
                actionsByDb.compute(databaseId,(k,v)->{
                    return v==null ? new Action(databaseId, action) : v.add(action);
                });
            }
            @Override
            public CompositeAction canProvision(String databaseId) {
                merge(databaseId,Action.PROVISION_DB);
                return this;
            }

            @Override
            public CompositeAction canAdminister(String databaseId) {
                merge(databaseId,Action.ADMINISTER_DB);
                return this;
            }

            @Override
            public CompositeAction canDestroy(String databaseId) {
                merge(databaseId,Action.DESTROY_DB);
                return this;
            }

            @Override
            public CompositeAction canRead(String databaseId) {
                merge(databaseId,Action.READ_CONTENTS);
                return this;
            }

            @Override
            public CompositeAction canWrite(String databaseId) {
                merge(databaseId,Action.WRITE_CONTENTS);
                return this;
            }

            @Override
            public String submit() {
                return actionsByDb.isEmpty() ? null : check(token, actionsByDb.values());
            }

            @Override
            public String toString() {
                return actionsByDb.values().toString();
            }
        };
    }

    public static interface CompositeAction {
        /**
         * Check whether the token associated with this composite request can provision a database with the given identifier.
         * 
         * @param databaseId the identifier of the database that the user is to create
         * @return this request instance for chaining together methods; never null
         */
        public CompositeAction canProvision(String databaseId);

        /**
         * Check whether the token associated with this composite request can administer the identified database.
         * 
         * @param databaseId the identifier of the database that the user is to administer
         * @return this request instance for chaining together methods; never null
         */
        public CompositeAction canAdminister(String databaseId);

        /**
         * Check whether the token associated with this composite request can read the content in the identified database.
         * 
         * @param databaseId the identifier of the database that the user is to read
         * @return this request instance for chaining together methods; never null
         */
        public CompositeAction canRead(String databaseId);

        /**
         * Check whether the token associated with this composite request can change the content in the identified database.
         * 
         * @param databaseId the identifier of the database that the user is to write
         * @return this request instance for chaining together methods; never null
         */
        public CompositeAction canWrite(String databaseId);

        /**
         * Check whether the token associated with this composite request can destroy the identified database.
         * 
         * @param databaseId the identifier of the database that the user is to destroy
         * @return this request instance for chaining together methods; never null
         */
        public CompositeAction canDestroy(String databaseId);

        /**
         * Submit this check and return the username if the token holder can perform all of these actions.
         * 
         * @return the username if the token holder can read the database, or null otherwise
         */
        public String submit();
    }

    /**
     * Determine if the supplied token remains valid for reading the information in the database.
     * 
     * @param token the user's session token; may not be null
     * @param databaseId the identifier of the database that the user is to access
     * @return the username if the token holder can read the database, or null otherwise
     */
    default public String canRead(SessionToken token, String databaseId) {
        return check(token, new Action(databaseId, Action.READ_CONTENTS));
    }

    /**
     * Determine if the supplied token remains valid for writing to the information in the database.
     * 
     * @param token the user's session token; may not be null
     * @param databaseId the identifier of the database that the user is to access
     * @return the username if the token holder can write to the database, or null otherwise
     */
    default public String canWrite(SessionToken token, String databaseId) {
        return check(token, new Action(databaseId, Action.WRITE_CONTENTS));
    }

    /**
     * Determine if the supplied token remains valid for destroying information in the database.
     * 
     * @param token the user's session token; may not be null
     * @param databaseId the identifier of the database that the user is to access
     * @return the username if the token holder can destroy contents within the database, or null otherwise
     */
    default public String canDestroy(SessionToken token, String databaseId) {
        return check(token, new Action(databaseId, Action.DESTROY_DB));
    }

    /**
     * Determine if the supplied token remains valid for administering the database, including provisioning and destroying
     * databases.
     * 
     * @param token the user's session token; may not be null
     * @param databaseId the identifier of the database that the user is to access
     * @return the username if the token holder can read the database, or null otherwise
     */
    default public String canAdminister(SessionToken token, String databaseId) {
        return check(token, new Action(databaseId, Action.ADMINISTER_DB));
    }

    /**
     * Determine if the supplied token allows provisioning a new database.
     * 
     * @param token the user's session token; may not be null
     * @param databaseId the identifier of the database that the user is to provision
     * @return the username if the token holder can read the database, or null otherwise
     */
    default public String canProvision(SessionToken token, String databaseId) {
        return check(token, new Action(databaseId, Action.PROVISION_DB));
    }

    /**
     * Get the session token information.
     * 
     * @param token the token; never null
     * @param accessor the accessor that will be used to access the information in the session token
     */
    public void info(SessionToken token, SessionTokenAccessor accessor);

    /**
     * Release all resources used by this provider.
     */
    public void shutdown();

    /**
     * A functional interface used to access the information within a {@link SessionToken}.
     * 
     * @author Randall Hauch
     * @see SecurityProvider#info(SessionToken, SessionTokenAccessor)
     */
    @FunctionalInterface
    public static interface SessionTokenAccessor {
        /**
         * Record the session token information
         * 
         * @param username the username; may not be null
         * @param device the device; may be null
         * @param appVersion the application version; may be null
         */
        void record(String username, String device, String appVersion);
    }
}

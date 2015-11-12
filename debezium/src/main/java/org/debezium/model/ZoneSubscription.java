/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

import java.util.EnumSet;

/**
 * @author Randall Hauch
 *
 */
public interface ZoneSubscription {

    public static enum Interest {
        ON_CREATE(1), ON_UPDATE(2), ON_DELETE(3);
        private final int code;
        private Interest(int code ) {
            this.code = code;
        }
        public int code() {
            return code;
        }
        public boolean includedIn( int mask ) {
            return (mask & this.code) == this.code;
        }
        public static int mask( boolean onCreate, boolean onUpdate, boolean onDelete ) {
            int mask = 0;
            if ( onCreate ) mask |= ON_CREATE.code;
            if ( onUpdate ) mask |= ON_UPDATE.code;
            if ( onDelete ) mask |= ON_DELETE.code;
            return mask;
        }
        public static int mask( Interest interest1 ) {
            return interest1.code;
        }
        public static int mask( Interest interest1, Interest interest2 ) {
            return interest1.code | interest2.code;
        }
        public static int mask( Interest interest1, Interest interest2, Interest interest3) {
            return interest1.code | interest2.code | interest3.code;
        }
    }

    public static ZoneSubscription create(ZoneId zoneId, Interest interest) {
        return create(zoneId, EnumSet.of(interest));
    }

    public static ZoneSubscription create(ZoneId zoneId, Interest interest1, Interest interest2) {
        return create(zoneId, EnumSet.of(interest1, interest2));
    }

    public static ZoneSubscription create(ZoneId zoneId, Interest interest1, Interest interest2, Interest interest3) {
        return create(zoneId, EnumSet.of(interest1, interest2, interest3));
    }

    public static ZoneSubscription create(ZoneId zoneId, EnumSet<Interest> interests) {
        return new SimpleZoneSubscription(Identifier.newSubscription(zoneId.databaseId()), zoneId, interests);
    }

    public static ZoneSubscription with(String id, ZoneId zoneId, Interest interest) {
        return with(id, zoneId, EnumSet.of(interest));
    }

    public static ZoneSubscription with(String id, ZoneId zoneId, Interest interest1, Interest interest2) {
        return with(id, zoneId, EnumSet.of(interest1, interest2));
    }

    public static ZoneSubscription with(String id, ZoneId zoneId, Interest interest1, Interest interest2, Interest interest3) {
        return with(id, zoneId, EnumSet.of(interest1, interest2, interest3));
    }

    public static ZoneSubscription with(String id, ZoneId zoneId, EnumSet<Interest> interests) {
        return with(Identifier.subscription(zoneId.databaseId(),id), zoneId, interests);
    }

    public static ZoneSubscription with(SubscriptionId id, ZoneId zoneId, Interest interest) {
        return with(id, zoneId, EnumSet.of(interest));
    }

    public static ZoneSubscription with(SubscriptionId id, ZoneId zoneId, Interest interest1, Interest interest2) {
        return with(id, zoneId, EnumSet.of(interest1, interest2));
    }

    public static ZoneSubscription with(SubscriptionId id, ZoneId zoneId, Interest interest1, Interest interest2, Interest interest3) {
        return with(id, zoneId, EnumSet.of(interest1, interest2, interest3));
    }

    public static ZoneSubscription with(SubscriptionId id, ZoneId zoneId, EnumSet<Interest> interests) {
        return new SimpleZoneSubscription(id, zoneId, interests);
    }

    /**
     * Get the unique identifier of this subscription.
     * @return the subscription's identifier; never null
     */
    SubscriptionId id();

    /**
     * Get the identifier of the zone to which this subscription applies.
     * @return the zone identifier; never null
     */
    ZoneId zoneId();

    /**
     * Get the set of {@link Interest}s that this subscription uses.
     * @return the interests for this subscription
     */
    EnumSet<Interest> interests();

    /**
     * Determine if this subscription includes the supplied interest.
     * @param interest the interest
     * @return {@code true} if this subscription includes the designated interest, or {@code false} otherwise
     */
    default boolean includes(Interest interest) {
        return interests().contains(interest);
    }
}

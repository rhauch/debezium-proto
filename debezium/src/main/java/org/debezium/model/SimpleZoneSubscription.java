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
final class SimpleZoneSubscription implements ZoneSubscription {

    private final SubscriptionId id;
    private final ZoneId zoneId;
    private final EnumSet<Interest> interests;

    SimpleZoneSubscription(SubscriptionId id, ZoneId zoneId, EnumSet<Interest> interests) {
        this.id = id;
        this.zoneId = zoneId;
        this.interests = interests;
    }

    @Override
    public SubscriptionId id() {
        return id;
    }

    @Override
    public ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public EnumSet<Interest> interests() {
        return interests.clone();
    }
}

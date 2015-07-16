/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Objects;

import org.debezium.core.annotation.Immutable;

/**
 * An immutable representation of a broker address.
 * 
 * @author Randall Hauch
 */
@Immutable
final class BrokerAddress {

    private final String brokerName;
    private final String machine;
    private final int port;

    protected BrokerAddress(String brokerName, String machine, int port) {
        this.brokerName = brokerName;
        this.machine = machine;
        this.port = port;
    }

    @Override
    public int hashCode() {
        return brokerName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof BrokerAddress) {
            BrokerAddress that = (BrokerAddress) obj;
            return this.brokerName.equals(that.brokerName) &&
                    Objects.equals(this.machine, that.machine) &&
                    this.port == that.port;
        }
        return false;
    }

    @Override
    public String toString() {
        return brokerName + ":" + (machine != null ? machine + ":" : "") + Integer.toString(port);
    }
}

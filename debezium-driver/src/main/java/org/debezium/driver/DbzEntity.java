/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Objects;

import org.debezium.core.annotation.Immutable;
import org.debezium.core.component.EntityId;
import org.debezium.core.doc.Document;

/**
 * A representation of an {@link Entity} that may or may not exist.
 * @author Randall Hauch
 */
@Immutable
final class DbzEntity implements Entity {

    private final EntityId id;
    private final Document doc;
    
    public DbzEntity( EntityId id, Document doc ) {
        assert id != null;
        this.id = id;
        this.doc = doc;
    }

    @Override
    public boolean exists() {
        return doc != null;
    }

    @Override
    public EntityId id() {
        return id;
    }

    @Override
    public Document asDocument() {
        return doc;
    }
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if ( obj == this ) return true;
        if ( obj instanceof Entity ) {
            Entity that = (Entity)obj;
            return this.id.equals(that.id()) && Objects.equals(this.doc, that.asDocument());
        }
        return false;
    }

}

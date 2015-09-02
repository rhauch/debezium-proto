/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.debezium.message.Document;
import org.debezium.message.Patch;
import org.debezium.model.Entity;
import org.debezium.model.EntityChange;
import org.debezium.model.EntityChange.ChangeStatus;

/**
 * @author Randall Hauch
 *
 */
public class DbzBatchResultTest extends BatchResultTest {

    @Override
    protected BatchResultFactory getBatchResultFactory() {
        final String dbId = "db1";
        final String entityId = "db1/ent1";
        final Entity entity = entity(dbId, entityId, Document.create());
        final EntityChange change = entityChange(entity, Patch.read(entity.id()), ChangeStatus.OK);
        return new BatchResultFactory() {
            @Override
            public BatchResult emptyResults() {
                return new DbzBatchResult(Collections.emptyMap(),Collections.emptyMap(),Collections.emptySet());
            }
            @Override
            public BatchResult singleReadResults() {
                Map<String,Entity> read = Collections.singletonMap(dbId, entity);
                return new DbzBatchResult(read,Collections.emptyMap(),Collections.emptySet());
            }
            @Override
            public BatchResult singleChangeResults() {
                Map<String,EntityChange> changed = Collections.singletonMap(dbId, change);
                return new DbzBatchResult(Collections.emptyMap(),changed,Collections.emptySet());
            }
            @Override
            public BatchResult singleRemovalResults() {
                Set<String> removed = Collections.singleton(entityId);
                return new DbzBatchResult(Collections.emptyMap(),Collections.emptyMap(),removed);
            }
        };
    }

}

/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Patch;
import org.debezium.driver.EntityChange.ChangeStatus;
import org.fest.util.Objects;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class BatchResultTest {

    public static interface BatchResultFactory {
        public BatchResult emptyResults();

        public BatchResult singleReadResults();

        public BatchResult singleChangeResults();

        public BatchResult singleRemovalResults();
    }

    protected BatchResultFactory getBatchResultFactory() {
        final String dbId = "db1";
        final String entityId = "db1/ent1";
        final Entity nonExistingEntity = entity(dbId, entityId, null);
        final Entity entity = entity(dbId, entityId, Document.create());
        final EntityChange change = entityChange(entity, Patch.read(entity.id()), ChangeStatus.OK);
        final BatchResult emptyResults = new EmptyResults();
        final BatchResult singleRemovalResults = new EmptyResults() {
            @Override
            public Set<String> removals() {
                return Collections.singleton(entityId);
            }
        };
        final BatchResult singleReadResults = new EmptyResults() {
            @Override
            public Map<String, Entity> reads() {
                return Collections.singletonMap(dbId, nonExistingEntity);
            }
        };
        final BatchResult singleChangeResults = new EmptyResults() {
            @Override
            public Map<String, EntityChange> changes() {
                return Collections.singletonMap(entity.id().databaseId().asString(), change);
            }
        };
        return new BatchResultFactory() {
            @Override
            public BatchResult emptyResults() {
                return emptyResults;
            }

            @Override
            public BatchResult singleReadResults() {
                return singleReadResults;
            }

            @Override
            public BatchResult singleChangeResults() {
                return singleChangeResults;
            }

            @Override
            public BatchResult singleRemovalResults() {
                return singleRemovalResults;
            }
        };
    }

    protected static class EmptyResults implements BatchResult {
        @Override
        public Map<String, EntityChange> changes() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, Entity> reads() {
            return Collections.emptyMap();
        }

        @Override
        public Set<String> removals() {
            return Collections.emptySet();
        }
    }

    protected static Entity entity(String databaseId, String entityId, Document doc) {
        EntityId id = Identifier.of(databaseId, "contacts", entityId);
        return new Entity() {
            @Override
            public Document asDocument() {
                return doc;
            }

            @Override
            public EntityId id() {
                return id;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) return true;
                if (obj instanceof Entity) {
                    Entity that = (Entity) obj;
                    return this.id().equals(that.id()) &&
                            Objects.areEqual(this.asDocument(), that.asDocument());
                }
                return false;
            }
        };
    }

    protected static EntityChange entityChange(Entity entity, Patch<EntityId> patch, ChangeStatus status) {
        return new EntityChange() {
            @Override
            public Entity entity() {
                return entity;
            }

            @Override
            public Stream<String> failureReasons() {
                return null;
            }

            @Override
            public Patch<EntityId> patch() {
                return patch;
            }

            @Override
            public ChangeStatus status() {
                return status;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) return true;
                if (obj instanceof EntityChange) {
                    EntityChange that = (EntityChange) obj;
                    return this.entity().equals(that.entity()) &&
                            this.status() == that.status() &&
                            Objects.areEqual(this.patch(), that.patch());
                }
                return false;
            }
        };
    }

    private BatchResultFactory factory;

    @Before
    public void beforeEach() {
        factory = getBatchResultFactory();
    }

    @Test
    public void shouldCorrectlyDetermineIsEmpty() {
        assertThat(factory.emptyResults().isEmpty()).isTrue();
        assertThat(factory.singleReadResults().isEmpty()).isFalse();
        assertThat(factory.singleChangeResults().isEmpty()).isFalse();
        assertThat(factory.singleRemovalResults().isEmpty()).isFalse();
    }

    @Test
    public void shouldCorrectlyGetReadResults() {
        assertThat(factory.emptyResults().reads().isEmpty()).isTrue();
        assertThat(factory.singleReadResults().reads().isEmpty()).isFalse();
        assertThat(factory.singleChangeResults().reads().isEmpty()).isTrue();
        assertThat(factory.singleRemovalResults().reads().isEmpty()).isTrue();

        assertThat(factory.emptyResults().hasReads()).isFalse();
        assertThat(factory.singleReadResults().hasReads()).isTrue();
        assertThat(factory.singleChangeResults().hasReads()).isFalse();
        assertThat(factory.singleRemovalResults().hasReads()).isFalse();

        assertThat(factory.emptyResults().readStream().count()).isEqualTo(0);
        assertThat(factory.singleReadResults().readStream().count()).isEqualTo(1);
        assertThat(factory.singleChangeResults().readStream().count()).isEqualTo(0);
        assertThat(factory.singleRemovalResults().readStream().count()).isEqualTo(0);

        Entity fromSet = factory.singleReadResults().reads().values().iterator().next();
        Entity fromStream = factory.singleReadResults().readStream().findFirst().get();
        assertThat(fromStream).isEqualTo(fromSet);
    }

    @Test
    public void shouldCorrectlyGetChangeResults() {
        assertThat(factory.emptyResults().changes().isEmpty()).isTrue();
        assertThat(factory.singleReadResults().changes().isEmpty()).isTrue();
        assertThat(factory.singleChangeResults().changes().isEmpty()).isFalse();
        assertThat(factory.singleRemovalResults().changes().isEmpty()).isTrue();

        assertThat(factory.emptyResults().hasChanges()).isFalse();
        assertThat(factory.singleReadResults().hasChanges()).isFalse();
        assertThat(factory.singleChangeResults().hasChanges()).isTrue();
        assertThat(factory.singleRemovalResults().hasChanges()).isFalse();

        assertThat(factory.emptyResults().changeStream().count()).isEqualTo(0);
        assertThat(factory.singleReadResults().changeStream().count()).isEqualTo(0);
        assertThat(factory.singleChangeResults().changeStream().count()).isEqualTo(1);
        assertThat(factory.singleRemovalResults().changeStream().count()).isEqualTo(0);

        EntityChange fromSet = factory.singleChangeResults().changes().values().iterator().next();
        EntityChange fromStream = factory.singleChangeResults().changeStream().findFirst().get();
        assertThat(fromStream).isEqualTo(fromSet);
    }

    @Test
    public void shouldCorrectlyGetRemovalResults() {
        assertThat(factory.emptyResults().removals().isEmpty()).isTrue();
        assertThat(factory.singleReadResults().removals().isEmpty()).isTrue();
        assertThat(factory.singleChangeResults().removals().isEmpty()).isTrue();
        assertThat(factory.singleRemovalResults().removals().isEmpty()).isFalse();

        assertThat(factory.emptyResults().hasRemovals()).isFalse();
        assertThat(factory.singleReadResults().hasRemovals()).isFalse();
        assertThat(factory.singleChangeResults().hasRemovals()).isFalse();
        assertThat(factory.singleRemovalResults().hasRemovals()).isTrue();

        assertThat(factory.emptyResults().removalStream().count()).isEqualTo(0);
        assertThat(factory.singleReadResults().removalStream().count()).isEqualTo(0);
        assertThat(factory.singleChangeResults().removalStream().count()).isEqualTo(0);
        assertThat(factory.singleRemovalResults().removalStream().count()).isEqualTo(1);

        String fromSet = factory.singleRemovalResults().removals().iterator().next();
        String fromStream = factory.singleRemovalResults().removalStream().findFirst().get();
        assertThat(fromStream).isEqualTo(fromSet);
    }

}

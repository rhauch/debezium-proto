/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services.learn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.debezium.Testing;
import org.debezium.core.component.EntityCollection;
import org.debezium.core.component.EntityCollection.FieldDefinition;
import org.debezium.core.component.EntityCollection.FieldType;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.DocumentReader;
import org.debezium.core.doc.Path;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Message;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Topic;
import org.debezium.services.learn.LearningEntityTypeModel.FieldUsage;
import org.fest.assertions.Fail;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class LearningEntityTypeModelTest implements Testing {

    private static final String FOLDER_NAME = Topic.SCHEMA_LEARNING;
    
    public static enum OptionalField {
        OPTIONAL, REQUIRED
    }

    private LearningEntityTypeModel student;
    private Patch<EntityType> schemaPatch;
    private Document schema;
    private FieldUsage fieldUsage;

    @Before
    public void beforeEach() {
        schemaPatch = null;
        schema = Document.create();
        fieldUsage = new InMemoryFieldUsage();
    }

    protected void setSchemaPatch(Patch<EntityType> schemaPatch) {
        this.schemaPatch = schemaPatch;
        if (schemaPatch != null) {
            Testing.debug("Schema patch: " + this.schemaPatch);
            schemaPatch.apply(schema, this::failedOperation);
        } else {
            Testing.debug("Schema patch: <no changes>");
        }
    }

    protected void failedOperation(Patch.Operation failedOp) {
        Fail.fail("Failed operation: " + failedOp);
    }

    @Test
    public void shouldLearnFromMultipleCreateRequestsForFlatEntities() throws IOException {
        Testing.Print.enable();
        // Testing.Debug.enable();

        EntityType type = Identifier.of("my-db", "contacts");
        student = new LearningEntityTypeModel(type, schema, fieldUsage);

        EntityCollection collection = processChanges("contacts-step1.json", type);
        assertField(collection, "firstName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "lastName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "homePhone", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "title", FieldType.STRING, OptionalField.OPTIONAL); // only bill has title
        assertField(collection, "age", FieldType.INTEGER, OptionalField.OPTIONAL); // only bill has age

        collection = processChanges("contacts-step2.json", type);
        assertField(collection, "firstName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "lastName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "homePhone", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "title", FieldType.STRING, OptionalField.OPTIONAL); // only bill has title
        assertField(collection, "age", FieldType.INTEGER, OptionalField.REQUIRED); // both have ages

        collection = processChanges("contacts-step3.json", type);
        assertField(collection, "firstName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "lastName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "homePhone", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "title", FieldType.STRING, OptionalField.REQUIRED); // both have titles
        assertField(collection, "age", FieldType.INTEGER, OptionalField.OPTIONAL);

        // Adds Veronica and Charlie with all required fields ...
        collection = processChanges("contacts-step4.json", type);
        assertField(collection, "firstName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "lastName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "homePhone", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "title", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "age", FieldType.INTEGER, OptionalField.OPTIONAL);

        // Moves Veronica's 'homePhone' to 'mobilePhone', making both fields optional ...
        collection = processChanges("contacts-step5.json", type);
        assertField(collection, "firstName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "lastName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "homePhone", FieldType.STRING, OptionalField.OPTIONAL);
        assertField(collection, "mobilePhone", FieldType.STRING, OptionalField.OPTIONAL);
        assertField(collection, "title", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "age", FieldType.INTEGER, OptionalField.OPTIONAL);
    }

    @Test
    public void shouldLearnFromMultipleCreateRequestsForComplexEntities() throws IOException {
         Testing.Print.enable();
        // Testing.Debug.enable();

        EntityType type = Identifier.of("my-db", "contacts");
        student = new LearningEntityTypeModel(type, schema, fieldUsage);

        EntityCollection collection = processChanges("complex-contacts-step1.json", type);
        assertField(collection, "firstName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "lastName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "phone/home", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "title", FieldType.STRING, OptionalField.OPTIONAL); // only bill has title
        assertField(collection, "age", FieldType.INTEGER, OptionalField.OPTIONAL); // only bill has age

        collection = processChanges("complex-contacts-step2.json", type);
        assertField(collection, "firstName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "lastName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "phone/home", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "title", FieldType.STRING, OptionalField.OPTIONAL); // only bill has title
        assertField(collection, "age", FieldType.INTEGER, OptionalField.REQUIRED); // both have ages

        collection = processChanges("complex-contacts-step3.json", type);
        assertField(collection, "firstName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "lastName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "phone/home", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "title", FieldType.STRING, OptionalField.REQUIRED); // both have titles
        assertField(collection, "age", FieldType.INTEGER, OptionalField.OPTIONAL);

        // Adds Veronica and Charlie with all required fields ...
        collection = processChanges("complex-contacts-step4.json", type);
        assertField(collection, "firstName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "lastName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "phone/home", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "title", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "age", FieldType.INTEGER, OptionalField.OPTIONAL);

        // Moves Veronica's 'homePhone' to 'mobilePhone', making both fields optional ...
        collection = processChanges("complex-contacts-step5.json", type);
        assertField(collection, "firstName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "lastName", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "phone/home", FieldType.STRING, OptionalField.OPTIONAL);
        assertField(collection, "phone/mobile", FieldType.STRING, OptionalField.OPTIONAL);
        assertField(collection, "title", FieldType.STRING, OptionalField.REQUIRED);
        assertField(collection, "age", FieldType.INTEGER, OptionalField.OPTIONAL);
    }

    protected EntityCollection processChanges(String filename, EntityType type) throws IOException {
        String json = Testing.Files.readResourceAsString(FOLDER_NAME + "/" + filename);
        Document requestsDoc = DocumentReader.defaultReader().read(json);

        schemaPatch = null;
        requestsDoc.getArray("entityChanges").streamValues().map(Value::asDocument).forEach(request -> {
            Patch<EntityId> patch = Patch.<EntityId> from(request);
            Document beforePatch = Message.getBefore(request);
            Document afterPatch = Message.getAfter(request);
            student.adapt(beforePatch, patch, afterPatch, this::setSchemaPatch);
        });
        Testing.print(schema);
        return EntityCollection.with(type, schema);
    }

    protected void assertField(EntityCollection collection, String name, FieldType type, OptionalField isOptional) {
        Path path = Path.parse(name);
        Optional<FieldDefinition> optionalField = collection.field(path);
        assertThat(optionalField.isPresent()).isTrue();
        FieldDefinition field = optionalField.get();
        assertThat(field.name()).isEqualTo(path.lastSegment().get());
        if (type != null) {
            assertThat(field.type().get()).isEqualTo(type);
        } else {
            assertThat(field.type().isPresent()).isEqualTo(false);
        }
        assertThat(field.isOptional()).isEqualTo(isOptional == OptionalField.OPTIONAL ? true : false);
    }

    protected void assertNoField(EntityCollection collection, String name) {

    }

    private static class InMemoryFieldUsage implements FieldUsage {

        private AtomicLong totalCount = new AtomicLong();
        private Map<String, AtomicLong> fieldCounts = new HashMap<>();

        @Override
        public void markNewEntity(EntityType type) {
            totalCount.incrementAndGet();
        }

        private String key(EntityType type, Path fieldPath) {
            return type.asString() + "::" + fieldPath.toRelativePath();
        }

        @Override
        public boolean markAdded(EntityType type, Path fieldPath) {
            long count = fieldCounts.computeIfAbsent(key(type, fieldPath), k -> new AtomicLong(0L))
                                    .incrementAndGet();
            return count < totalCount.get();
        }

        @Override
        public boolean markRemoved(EntityType type, Path fieldPath) {
            long count = fieldCounts.computeIfAbsent(key(type, fieldPath), k -> new AtomicLong(0L))
                                    .updateAndGet(this::decrementIfPositive);
            return count < totalCount.get();
        }

        private long decrementIfPositive(long value) {
            return value > 0L ? value - 1L : 0L;
        }

    }

}

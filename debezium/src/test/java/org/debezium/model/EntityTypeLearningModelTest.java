/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

import java.io.IOException;
import java.util.Optional;

import org.debezium.Testing;
import org.debezium.message.Document;
import org.debezium.message.DocumentReader;
import org.debezium.message.Message;
import org.debezium.message.Patch;
import org.debezium.message.Path;
import org.debezium.message.Value;
import org.debezium.model.EntityCollection.FieldDefinition;
import org.debezium.model.EntityCollection.FieldType;
import org.fest.assertions.Delta;
import org.fest.assertions.Fail;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class EntityTypeLearningModelTest implements Testing {

    private static final long TIMESTAMP = 100110011L;
    private static final String FOLDER_NAME = "entity-changes";
    
    public static enum OptionalField {
        OPTIONAL, REQUIRED
    }

    private EntityTypeLearningModel student;
    private Patch<EntityType> schemaPatch;
    private Document schema;

    @Before
    public void beforeEach() {
        resetBeforeEachTest();
        schemaPatch = null;
        schema = Document.create();
    }

    protected void setSchemaPatch(Document before, long previousTimestamp, Patch<EntityType> schemaPatch) {
        this.schemaPatch = schemaPatch;
        if (schemaPatch != null) {
            Testing.debug("Schema patch: " + this.schemaPatch);
            schemaPatch.apply(schema, this::failedOperation);
        } else {
            Testing.debug("Schema patch: <no changes>");
        }
    }

    protected void failedOperation(Path failedPath, Patch.Operation failedOp) {
        Fail.fail("Failed operation at path '" + failedPath + "': " + failedOp);
    }

    @Test
    public void shouldLearnFromMultipleCreateRequestsForFlatEntities() throws IOException {
        // Testing.Print.enable();
        // Testing.Debug.enable();

        EntityType type = Identifier.of("my-db", "contacts");
        student = new EntityTypeLearningModel(type, schema);

        // Add contacts for Sally and Bill ...
        EntityCollection collection = processChanges("contacts-step1.json", type);
        assertField(collection, "firstName", FieldType.STRING, 1.0);
        assertField(collection, "lastName", FieldType.STRING, 1.0);
        assertField(collection, "homePhone", FieldType.STRING, 1.0);
        assertField(collection, "title", FieldType.STRING, 0.5); // only bill has title
        assertField(collection, "age", FieldType.INTEGER, 0.5); // only bill has age

        // Add age to Sally ...
        collection = processChanges("contacts-step2.json", type);
        assertField(collection, "firstName", FieldType.STRING, 1.0);
        assertField(collection, "lastName", FieldType.STRING, 1.0);
        assertField(collection, "homePhone", FieldType.STRING, 1.0);
        assertField(collection, "title", FieldType.STRING, 0.5); // only bill has title
        assertField(collection, "age", FieldType.INTEGER, 1.0); // both have ages

        // Remove Bill's age and add title ...
        collection = processChanges("contacts-step3.json", type);
        assertField(collection, "firstName", FieldType.STRING, 1.0);
        assertField(collection, "lastName", FieldType.STRING, 1.0);
        assertField(collection, "homePhone", FieldType.STRING, 1.0);
        assertField(collection, "title", FieldType.STRING, 1.0); // both have titles
        assertField(collection, "age", FieldType.INTEGER, 0.5); // only bill has age

        // Adds Veronica and Charlie with all required fields ...
        collection = processChanges("contacts-step4.json", type);
        assertField(collection, "firstName", FieldType.STRING, 1.0);
        assertField(collection, "lastName", FieldType.STRING, 1.0);
        assertField(collection, "homePhone", FieldType.STRING, 1.0);
        assertField(collection, "title", FieldType.STRING, 1.0);
        assertField(collection, "age", FieldType.INTEGER, 0.25); // only bill has age

        // Moves Veronica's 'homePhone' to 'mobilePhone', making both fields optional ...
        collection = processChanges("contacts-step5.json", type);
        assertField(collection, "firstName", FieldType.STRING, 1.0);
        assertField(collection, "lastName", FieldType.STRING, 1.0);
        assertField(collection, "homePhone", FieldType.STRING, 0.75); // veronica has no home phone
        assertField(collection, "mobilePhone", FieldType.STRING, 0.25); // veronica has mobile phone
        assertField(collection, "title", FieldType.STRING, 1.0);
        assertField(collection, "age", FieldType.INTEGER, 0.25); // only bill has age
    }

    @Test
    public void shouldLearnFromMultipleCreateRequestsForComplexEntities() throws IOException {
        // Testing.Print.enable();
        // Testing.Debug.enable();

        EntityType type = Identifier.of("my-db", "contacts");
        student = new EntityTypeLearningModel(type, schema);

        // Add contacts for Sally and Bill ...
        EntityCollection collection = processChanges("complex-contacts-step1.json", type);
        assertField(collection, "firstName", FieldType.STRING, 1.0);
        assertField(collection, "lastName", FieldType.STRING, 1.0);
        assertField(collection, "phone/home", FieldType.STRING, 1.0);
        assertField(collection, "title", FieldType.STRING, 0.5); // only bill has title
        assertField(collection, "age", FieldType.INTEGER, 0.5); // only bill has age

        // Add age to Sally ...
        collection = processChanges("complex-contacts-step2.json", type);
        assertField(collection, "firstName", FieldType.STRING, 1.0);
        assertField(collection, "lastName", FieldType.STRING, 1.0);
        assertField(collection, "phone/home", FieldType.STRING, 1.0);
        assertField(collection, "title", FieldType.STRING, 0.5); // only bill has title
        assertField(collection, "age", FieldType.INTEGER, 1.0); // both have ages

        // Remove Bill's age and add title ...
        collection = processChanges("complex-contacts-step3.json", type);
        assertField(collection, "firstName", FieldType.STRING, 1.0);
        assertField(collection, "lastName", FieldType.STRING, 1.0);
        assertField(collection, "phone/home", FieldType.STRING, 1.0);
        assertField(collection, "title", FieldType.STRING, 1.0); // both have titles
        assertField(collection, "age", FieldType.INTEGER, 0.5); // only bill has age

        // Adds Veronica and Charlie with all required fields ...
        collection = processChanges("complex-contacts-step4.json", type);
        assertField(collection, "firstName", FieldType.STRING, 1.0);
        assertField(collection, "lastName", FieldType.STRING, 1.0);
        assertField(collection, "phone/home", FieldType.STRING, 1.0);
        assertField(collection, "title", FieldType.STRING, 1.0);
        assertField(collection, "age", FieldType.INTEGER, 0.25); // only bill has age

        // Moves Veronica's 'homePhone' to 'mobilePhone', making both fields optional ...
        collection = processChanges("complex-contacts-step5.json", type);
        assertField(collection, "firstName", FieldType.STRING, 1.0);
        assertField(collection, "lastName", FieldType.STRING, 1.0);
        assertField(collection, "phone/home", FieldType.STRING, 0.75); // Veronica has no home phone
        assertField(collection, "phone/mobile", FieldType.STRING, 0.25); // Veronica has mobile phone
        assertField(collection, "title", FieldType.STRING, 1.0);
        assertField(collection, "age", FieldType.INTEGER, 0.25); // only bill has age
    }

    protected EntityCollection processChanges(String filename, EntityType type) throws IOException {
        String json = Testing.Files.readResourceAsString(FOLDER_NAME + "/" + filename);
        Document requestsDoc = DocumentReader.defaultReader().read(json);

        schemaPatch = null;
        requestsDoc.getArray("entityChanges").streamValues().map(Value::asDocument).forEach(request -> {
            Patch<EntityId> patch = Patch.<EntityId> from(request);
            Document beforePatch = Message.getBefore(request);
            Document afterPatch = Message.getAfter(request);
            student.adapt(beforePatch, patch, afterPatch,  TIMESTAMP, true, this::setSchemaPatch);
        });
        Testing.print(schema);
        student.resetChangeStatistics();
        return EntityCollection.with(type, schema);
    }

    protected FieldDefinition assertField(EntityCollection collection, String name, FieldType type) {
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
        return field;
    }

    protected void assertField(EntityCollection collection, String name, FieldType type, double usage) {
        FieldDefinition field = assertField(collection,name,type);
        assertThat(field.usage()).isEqualTo((float)usage, Delta.delta(0.0001));
    }

    protected void assertField(EntityCollection collection, String name, FieldType type, OptionalField isOptional) {
        FieldDefinition field = assertField(collection,name,type);
        switch( isOptional ) {
            case OPTIONAL:
                assertThat(field.usage()).isLessThan(1.0f);
                break;
            case REQUIRED:
                assertThat(field.usage()).isEqualTo(1.0f);
                break;
        }
    }

    protected void assertNoField(EntityCollection collection, String name) {

    }
}

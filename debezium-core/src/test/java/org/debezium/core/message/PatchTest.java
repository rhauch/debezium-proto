/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.message;

import static org.fest.assertions.Assertions.assertThat;

import org.debezium.Testing;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.ZoneId;
import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.fest.assertions.Fail;
import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class PatchTest implements Testing {
    
    private static final DatabaseId DBID = Identifier.of("testdb");
    private static final EntityType TYPE = Identifier.of(DBID,"ents");
    private static final ZoneId ZONE = Identifier.zone(TYPE,"zoneA");
    private static final EntityId ENTITY_ID = Identifier.of(ZONE,"ent1");

    @Test
    public void shouldBuildPatchToCreateNewEntity() {
        Document initial = Document.create("firstName", "Jackie", "lastName", "Jones");
        Patch<EntityId> patch = Patch.create(ENTITY_ID, initial);
        assertThat(patch.isCreation()).isTrue();
    }

    @Test
    public void shouldBuildAndApplyCreationPatch() {
        //Testing.Print.enable();
        
        Document initial = Document.create("firstName", "Jackie", "lastName", "Jones");
        Patch<EntityId> patch = Patch.create(ENTITY_ID, initial);
        assertThat(patch.isCreation()).isTrue();
        
        Document result = Document.create();
        patch.apply(result, (op)->Fail.fail("Unable to apply op"));
        Testing.print("patch = " + patch);
        Testing.print("result = " + result);
        assertThat(result.getString("firstName")).isEqualTo("Jackie");
        assertThat(result.getString("lastName")).isEqualTo("Jones");
        assertThat(Message.getId(result)).isEqualTo(ENTITY_ID);
    }

    @Test
    public void shouldBuildAndApplyPatchToAddSimpleFieldsToEntity() {
        //Testing.Print.enable();
        
        Patch<EntityId> patch = Patch.edit(ENTITY_ID)
                .add("firstName", Value.create("Jackie"))
                .add("lastName", Value.create("Jones"))
                .end();
        assertThat(patch.isCreation()).isFalse();

        // Apply the patch ....
        Document doc = Document.create();
        Message.addId(doc, ENTITY_ID);
        patch.apply(doc, (op)->Fail.fail("Unable to apply patch: " + op));
        Testing.print("patch = " + patch);
        Testing.print("doc = " + doc);
        
        assertThat(doc.getString("firstName")).isEqualTo("Jackie");
        assertThat(doc.getString("lastName")).isEqualTo("Jones");
        assertThat(Message.getId(doc)).isEqualTo(ENTITY_ID);
    }

    @Test
    public void shouldBuildAndApplyPatchToAddSimpleAndDocumentFieldsToEntity() {
        //Testing.Print.enable();
        
        Patch<EntityId> patch = Patch.edit(ENTITY_ID)
                .add("firstName", Value.create("Jackie"))
                .add("lastName", Value.create("Jones"))
                .add("address", Value.create(Document.create("street","123 Main","city","Springfield")))
                .end();
        assertThat(patch.isCreation()).isFalse();

        // Apply the patch ....
        Document doc = Document.create();
        Message.addId(doc, ENTITY_ID);
        patch.apply(doc, (op)->Fail.fail("Unable to apply patch: " + op));
        Testing.print("patch = " + patch);
        Testing.print("doc = " + doc);
        
        assertThat(Message.getId(doc)).isEqualTo(ENTITY_ID);
        assertThat(doc.getString("firstName")).isEqualTo("Jackie");
        assertThat(doc.getString("lastName")).isEqualTo("Jones");
        Document address = doc.getDocument("address");
        assertThat(address.getString("street")).isEqualTo("123 Main");
        assertThat(address.getString("city")).isEqualTo("Springfield");
    }

    @Test
    public void shouldBuildAndApplyPatchToAddFieldToNestedDocument() {
        //Testing.Print.enable();
        
        Patch<EntityId> patch = Patch.edit(ENTITY_ID)
                .add("firstName", Value.create("Jackie"))
                .add("lastName", Value.create("Jones"))
                .add("address", Value.create(Document.create("street","123 Main","city","Springfield")))
                .add("address/country",Value.create("Atlantis"))
                .add("address/location/lat",Value.create(48.8582))
                .add("address/location/long",Value.create(-2.2945))
                .add("address/tags/-",Value.create("home"))
                .add("address/tags/-",Value.create("work"))
                .add("address/tags/0",Value.create("other"))
                .end();
        assertThat(patch.isCreation()).isFalse();

        // Apply the patch ....
        Document doc = Document.create();
        Message.addId(doc, ENTITY_ID);
        patch.apply(doc, (op)->Fail.fail("Unable to apply patch: " + op));
        Testing.print("patch = " + patch);
        Testing.print("doc = " + doc);
        
        assertThat(Message.getId(doc)).isEqualTo(ENTITY_ID);
        assertThat(doc.getString("firstName")).isEqualTo("Jackie");
        assertThat(doc.getString("lastName")).isEqualTo("Jones");
        Document address = doc.getDocument("address");
        assertThat(address.getString("street")).isEqualTo("123 Main");
        assertThat(address.getString("city")).isEqualTo("Springfield");
        assertThat(address.getString("country")).isEqualTo("Atlantis");
        Document location = address.getDocument("location");
        assertThat(location.getDouble("lat")).isEqualTo(48.8582);
        assertThat(location.getDouble("long")).isEqualTo(-2.2945);
        Array tags = address.getArray("tags");
        assertThat(tags.get(0).asString()).isEqualTo("other");
        assertThat(tags.get(1).asString()).isEqualTo("work");
        assertThat(tags.size()).isEqualTo(2);
    }
    
    @Test
    public void shouldBuildAndApplyPatchToReplaceFields() {
        //Testing.Print.enable();
        
        Patch<EntityId> patch = Patch.edit(ENTITY_ID)
                .add("firstName", Value.create("Jackie"))
                .add("lastName", Value.create("Jones"))
                .add("address", Value.create(Document.create("street","123 Main","city","Springfield")))
                .add("address/country",Value.create("Atlantis"))
                .add("address/location/lat",Value.create(48.8582))
                .add("address/location/long",Value.create(-2.2945))
                .add("address/tags/-",Value.create("home"))
                .add("address/tags/-",Value.create("work"))
                .replace("address/tags/0",Value.create("other"))
                .replace("address/country",Value.create("France"))
                .replace("lastName", Value.create("Thompson"))
                .replace("other/nickname", Value.create("JT"))
                .replace("other/quotes/woah", Value.create("crikey"))
                .end();
        assertThat(patch.isCreation()).isFalse();

        // Apply the patch ....
        Document doc = Document.create();
        Message.addId(doc, ENTITY_ID);
        patch.apply(doc, (op)->Fail.fail("Unable to apply patch: " + op));
        Testing.print("patch = " + patch);
        Testing.print("doc = " + doc);
        
        assertThat(Message.getId(doc)).isEqualTo(ENTITY_ID);
        assertThat(doc.getString("firstName")).isEqualTo("Jackie");
        assertThat(doc.getString("lastName")).isEqualTo("Thompson");
        Document address = doc.getDocument("address");
        assertThat(address.getString("street")).isEqualTo("123 Main");
        assertThat(address.getString("city")).isEqualTo("Springfield");
        assertThat(address.getString("country")).isEqualTo("France");
        Document location = address.getDocument("location");
        assertThat(location.getDouble("lat")).isEqualTo(48.8582);
        assertThat(location.getDouble("long")).isEqualTo(-2.2945);
        Array tags = address.getArray("tags");
        assertThat(tags.get(0).asString()).isEqualTo("other");
        assertThat(tags.get(1).asString()).isEqualTo("work");
        assertThat(tags.size()).isEqualTo(2);
        Document other = doc.getDocument("other");
        Document quotes = other.getDocument("quotes");
        assertThat(quotes.getString("woah")).isEqualTo("crikey");
    }
    
}

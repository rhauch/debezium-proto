/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.component;

import java.util.Optional;
import java.util.function.BiConsumer;

import org.debezium.core.component.EntityCollection.FieldType;
import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Path;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Message;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Editor;

/**
 * @author Randall Hauch
 *
 */
public class SchemaEditor {
    
    /**
     * An editor for a field definition.
     */
    public static interface FieldEditor {
        /**
         * Set the description for this field.
         * @param desc the description; may be null
         * @return this editor instance for method chaining; never null
         */
        FieldEditor description(String desc);
        
        /**
         * Set the type for this field.
         * @param type the desired field type; may be null
         * @return this editor instance for method chaining; never null
         */
        FieldEditor type(FieldType type);
        
        /**
         * Set the type for this field using the given prototype value.
         * @param prototypeValue a prototype value from which the field type is to be inferred; may be null
         * @return this editor instance for method chaining; never null
         */
        default FieldEditor type(Value prototypeValue) {
            Optional<FieldType> inferred = FieldType.inferFrom(prototypeValue);
            return inferred.isPresent() ? type(inferred.get()) : type((FieldType)null);
        }
        
        /**
         * Set whether this field is optional.
         * @param optional true if this field should be optional, or false if it is required
         * @return this editor instance for method chaining; never null
         */
        FieldEditor optional(boolean optional);
        
        
        /**
         * Set whether this field is an array of values.
         * @param array true if this field should allow multiple values, or false if at most a single value is allowed
         * @return this editor instance for method chaining; never null
         * @see #arrayConstraints()
         */
        FieldEditor array(boolean array);
        
        /**
         * Get the editor for the array constraints.
         * @return the array constraints; never null
         * @see #array(boolean)
         */
        ArrayEditor<FieldEditor> arrayConstraints();
        
        /**
         * Get the editor for the string constraints.
         * @return the string constraints; never null
         */
        StringConstraintsEditor<FieldEditor> stringConstraints();
        
        /**
         * Get the editor for the numeric constraints.
         * @return the numeric constraints; never null
         */
        NumberConstraintsEditor<FieldEditor> numberConstraints();
        
        /**
         * Get the editor for the location constraints.
         * @return the location constraints; never null
         */
        LocationConstraintsEditor<FieldEditor> locationConstraints();
    }
    
    
    public static interface Constraints<ReturnType> {
        ReturnType apply();
    }
    
    public static interface Clearable<ReturnType> {
        ReturnType clear();
    }
    
    public static interface EnumeratedEditor<ReturnType> {
        ReturnType allowedValues(Array values);
        
        default ReturnType allowedValues(Value firstValue) {
            return allowedValues(Array.create(firstValue));
        }
        
        default ReturnType allowedValues(Value firstValue, Value secondValue, Value... additionalValues) {
            return allowedValues(Array.create(firstValue, secondValue, additionalValues));
        }
        
        default ReturnType allowedValues(Iterable<Value> values) {
            return allowedValues(Array.create(values));
        }
    }
    
    public static interface StringConstraintsEditor<ReturnType> extends Constraints<ReturnType>,
            EnumeratedEditor<StringConstraintsEditor<ReturnType>>, Clearable<StringConstraintsEditor<ReturnType>> {
        StringConstraintsEditor<ReturnType> pattern(String regex);
        
        StringConstraintsEditor<ReturnType> minLength(int minimumLength, boolean inclusive);
        
        StringConstraintsEditor<ReturnType> maxLength(int maximumLength, boolean inclusive);
    }
    
    public static interface NumberConstraintsEditor<ReturnType> extends Constraints<ReturnType>,
            Clearable<NumberConstraintsEditor<ReturnType>> {
        NumberConstraintsEditor<ReturnType> minValue(int minimumValue, boolean inclusive);
        
        NumberConstraintsEditor<ReturnType> maxValue(int maximumValue, boolean inclusive);
        
        NumberConstraintsEditor<ReturnType> multipleOf(Number value);
    }
    
    public static interface ArrayEditor<ReturnType> extends Constraints<ReturnType>, Clearable<ArrayEditor<ReturnType>> {
        ArrayEditor<ReturnType> minItems(int minimumLength, boolean inclusive);
        
        ArrayEditor<ReturnType> maxItems(int maximumLength, boolean inclusive);
    }
    
    public static interface LocationConstraintsEditor<ReturnType> extends Constraints<ReturnType>,
            Clearable<LocationConstraintsEditor<ReturnType>> {
    }
    
    /**
     * Obtain an editor for the field definition with the given name.
     * 
     * @param patch the patch editor for modifying the entity type representation; may not be null
     * @param sourceFieldName the name of the field to be copied; may not be null
     * @param targetFieldName the name of the new field that results from the copy; may not be null
     */
    public static void copyField(Editor<Patch<EntityType>> patch, String sourceFieldName, String targetFieldName) {
        patch.copy(sourceFieldName, targetFieldName);
    }
    
    /**
     * Obtain an editor for the field definition with the given name.
     * 
     * @param patch the patch editor for modifying the entity type representation; may not be null
     * @param fieldName the name of the field; may not be null
     * @return the field editor; never null
     */
    public static FieldEditor editField(Editor<Patch<EntityType>> patch, String fieldName) {
        return editField(patch, EntityCollection.pathToField(fieldName));
    }
    
    /**
     * Obtain an editor for the field definition with the given name.
     * 
     * @param patch the patch editor for modifying the entity type representation; may not be null
     * @param pathToField the path to the field definition within the entity type representation; may not be null
     * @return the field editor; never null
     */
    private static FieldEditor editField(Editor<Patch<EntityType>> patch, Path pathToField) {
        return new BasicFieldEditor(patch, pathToField, false);
    }
    
    /**
     * Obtain an editor that will create a new field definition with the given name.
     * 
     * @param patch the patch editor for modifying the entity type representation; may not be null
     * @param fieldName the name of the field; may not be null
     * @return the field editor; never null
     */
    public static FieldEditor createField(Editor<Patch<EntityType>> patch, String fieldName) {
        return createField(patch, EntityCollection.pathToField(fieldName));
    }
    
    /**
     * Obtain an editor that will create a new field definition with the given name.
     * 
     * @param patch the patch editor for modifying the entity type representation; may not be null
     * @param pathToField the path to the field definition within the entity type representation; may not be null
     * @return the field editor; never null
     */
    private static FieldEditor createField(Editor<Patch<EntityType>> patch, Path pathToField) {
        return new BasicFieldEditor(patch, pathToField, true);
    }
    
    
    protected static class BasicFieldEditor implements FieldEditor {
        private final Patch.Editor<?> patch;
        private final Path pathPrefix;
        private final boolean isNew;
        
        protected BasicFieldEditor(Patch.Editor<?> patch, Path pathPrefix, boolean isNew) {
            this.patch = patch;
            this.pathPrefix = pathPrefix == null ? Path.root() : pathPrefix;
            this.isNew = isNew;
        }
        
        @Override
        public FieldEditor type(FieldType type) {
            patch.replace(pathFor("type"), Value.create(type.toString()));
            if ( !isNew ) {
                // None of these changes the array constraints, since that's orthogonal to the type ...
                switch (type) {
                    case STRING:
                        // Remove all but the string constraints ...
                        numberConstraints().clear();
                        locationConstraints().clear();
                        break;
                    case UUID:
                    case BOOLEAN:
                        // Remove all constraints ...
                        stringConstraints().clear();
                        numberConstraints().clear();
                        locationConstraints().clear();
                        break;
                    case DECIMAL:
                    case DOUBLE:
                    case FLOAT:
                    case INTEGER:
                    case LONG:
                    case BIG_INTEGER:
                    case NUMBER:
                    case TIMESTAMP:
                        // Remove all but the number constraints ...
                        stringConstraints().clear();
                        locationConstraints().clear();
                        break;
                    case LOCATION:
                        // Remove all but the location constraints ...
                        stringConstraints().clear();
                        numberConstraints().clear();
                        break;
                    case BINARY:
                        // Remove all constraints ...
                        stringConstraints().clear();
                        numberConstraints().clear();
                        locationConstraints().clear();
                        break;
                }
            }
            return this;
        }
        
        @Override
        public FieldEditor description(String desc) {
            replaceOrRemove(pathFor("description"), Value.create(desc));
            return this;
        }
        
        @Override
        public FieldEditor array(boolean array) {
            replaceOrRemove(pathFor("array"), Value.create(array));
            return this;
        }
        
        @Override
        public FieldEditor optional(boolean optional) {
            replaceOrRemove(pathFor("optional"), Value.create(optional));
            return this;
        }
        
        @Override
        public LocationConstraintsEditor<FieldEditor> locationConstraints() {
            FieldEditor result = this;
            return new LocationConstraintsEditor<FieldEditor>() {
                @Override
                public FieldEditor apply() {
                    return result;
                }
                
                @Override
                public LocationConstraintsEditor<FieldEditor> clear() {
                    return this;
                }
            };
        }
        
        @Override
        public StringConstraintsEditor<FieldEditor> stringConstraints() {
            FieldEditor result = this;
            return new StringConstraintsEditor<FieldEditor>() {
                @Override
                public FieldEditor apply() {
                    numberConstraints().clear();
                    locationConstraints().clear();
                    return result;
                }
                
                @Override
                public StringConstraintsEditor<FieldEditor> clear() {
                    remove("pattern");
                    remove("minLength");
                    remove("minLengthInclusive");
                    remove("maxLength");
                    remove("maxLengthInclusive");
                    remove("allowedValues");
                    return this;
                }
                
                @Override
                public StringConstraintsEditor<FieldEditor> pattern(String regex) {
                    replaceOrRemove("pattern", Value.create(regex));
                    return this;
                }
                
                @Override
                public StringConstraintsEditor<FieldEditor> minLength(int minimumLength, boolean inclusive) {
                    replaceOrRemove("minLength", Value.create(minimumLength));
                    replaceOrRemove("minLengthInclusive", Value.create(inclusive));
                    return this;
                }
                
                @Override
                public StringConstraintsEditor<FieldEditor> maxLength(int maximumLength, boolean inclusive) {
                    replaceOrRemove("maxLength", Value.create(maximumLength));
                    replaceOrRemove("maxLengthInclusive", Value.create(inclusive));
                    return this;
                }
                
                @Override
                public StringConstraintsEditor<FieldEditor> allowedValues(Array values) {
                    replaceOrRemove("allowedValues", Value.create(values));
                    return this;
                }
            };
        }
        
        @Override
        public NumberConstraintsEditor<FieldEditor> numberConstraints() {
            FieldEditor result = this;
            return new NumberConstraintsEditor<SchemaEditor.FieldEditor>() {
                @Override
                public FieldEditor apply() {
                    return result;
                }
                
                @Override
                public NumberConstraintsEditor<FieldEditor> clear() {
                    remove("minValue");
                    remove("minValueInclusive");
                    remove("maxValue");
                    remove("maxValueInclusive");
                    remove("multipleOf");
                    return this;
                }
                
                @Override
                public NumberConstraintsEditor<FieldEditor> minValue(int minValue, boolean inclusive) {
                    replaceOrRemove("minValue", Value.create(minValue));
                    replaceOrRemove("minValueInclusive", Value.create(inclusive));
                    return this;
                }
                
                @Override
                public NumberConstraintsEditor<FieldEditor> maxValue(int maxValue, boolean inclusive) {
                    replaceOrRemove("maxValue", Value.create(maxValue));
                    replaceOrRemove("maxValueInclusive", Value.create(inclusive));
                    return this;
                }
                
                @Override
                public NumberConstraintsEditor<FieldEditor> multipleOf(Number value) {
                    replaceOrRemove("multipleOf", Value.create(value));
                    return this;
                }
            };
        }
        
        @Override
        public ArrayEditor<FieldEditor> arrayConstraints() {
            FieldEditor result = this;
            return new ArrayEditor<SchemaEditor.FieldEditor>() {
                @Override
                public FieldEditor apply() {
                    return result;
                }
                
                @Override
                public ArrayEditor<FieldEditor> clear() {
                    remove("minItems");
                    remove("minItemsInclusive");
                    remove("maxItems");
                    remove("maxItemsInclusive");
                    return this;
                }
                
                @Override
                public ArrayEditor<FieldEditor> minItems(int minimumLength, boolean inclusive) {
                    replaceOrRemove("minItems", Value.create(minimumLength));
                    replaceOrRemove("minItemsInclusive", Value.create(inclusive));
                    return this;
                }
                
                @Override
                public ArrayEditor<FieldEditor> maxItems(int maximumLength, boolean inclusive) {
                    replaceOrRemove("maxItems", Value.create(maximumLength));
                    replaceOrRemove("maxItemsInclusive", Value.create(inclusive));
                    return this;
                }
            };
        }
        
        private void replaceOrRemove(String path, Value value) {
            if (value == null || value.isNull()) {
                patch.remove(path);
            } else {
                patch.replace(path, value);
            }
        }
        
        private void remove(String path) {
            patch.remove(path);
        }
        
        private String pathFor(String relPath) {
            return pathPrefix.append(relPath).toString();
        }
    }
    
    public static void onEachEntityType(Document schema, DatabaseId dbId, BiConsumer<EntityType, Document> consumer) {
        Document collections = schema.getDocument("collections");
        if (collections != null) {
            collections.forEach(field -> {
                Value value = field.getValue();
                if (value != null && value.isDocument()) {
                    consumer.accept(Identifier.of(dbId, field.getName()), value.asDocument());
                }
            });
        }
    }
    
    public static Document getOrCreateComponent(SchemaComponentId componentId, Document schema) {
        Document component = null;
        switch (componentId.type()) {
            case ENTITY_TYPE:
                EntityType type = (EntityType) componentId;
                Document collections = schema.getOrCreateDocument("collections");
                component = collections.getOrCreateDocument(type.entityTypeName());
                break;
        }
        return component;
    }
    
    public static Optional<? extends SchemaComponent<? extends SchemaComponentId>> getComponent(SchemaComponentId componentId, Document schema) {
        Document component = null;
        switch (componentId.type()) {
            case ENTITY_TYPE:
                EntityType type = (EntityType) componentId;
                Document collections = schema.getDocument("collections");
                if (collections != null) {
                    component = collections.getDocument(type.entityTypeName());
                    if ( component != null ) {
                        return Optional.of(EntityCollection.with(type, component));
                    }
                }
                break;
        }
        return Optional.empty();
    }
    
    public static void setLearning(Document schema, boolean enabled) {
        if (enabled) {
            schema.setBoolean(Message.Field.LEARNING, true);
        } else {
            schema.remove(Message.Field.LEARNING);
        }
    }
    
    public static boolean isLearningEnabled(Document schema) {
        return schema.getBoolean(Message.Field.LEARNING, false);
    }
    
    private SchemaEditor() {
    }
    
}

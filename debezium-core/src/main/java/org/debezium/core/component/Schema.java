/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.component;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Path;
import org.debezium.core.doc.Value;
import org.debezium.core.doc.Value.Type;
import org.debezium.core.message.Message;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Editor;

/**
 * @author Randall Hauch
 *
 */
public class Schema {
    
    /**
     * The types of values within an entity's field.
     */
    public static enum FieldType {
        /**
         * Any string value
         */
        STRING(Type.STRING),
        /**
         * A boolean value. This corresponds to Java's {@link Boolean boolean} type.
         */
        BOOLEAN(Type.BOOLEAN, Type.STRING),
        /**
         * A signed 32 bit two's complement integer. This corresponds to Java's {@link Integer integer} type.
         */
        INTEGER(Type.INTEGER, Type.STRING),
        /**
         * A signed 64 bit two's complement integer. This corresponds to Java's {@link Long long} type.
         */
        LONG(Type.LONG, Type.STRING),
        /**
         * A single-precision 32-bit IEEE 754 floating point. This corresponds to Java's {@link Float float} type.
         */
        FLOAT(Type.FLOAT, Type.STRING),
        /**
         * A double-precision 64-bit IEEE 754 floating point. This corresponds to Java's {@link Double double} type.
         */
        DOUBLE(Type.DOUBLE, Type.STRING),
        /**
         * An arbitrary-precision integer. This corresponds to Java's {@link BigInteger} type.
         */
        BIG_INTEGER(Type.BIG_INTEGER, Type.STRING),
        /**
         * An arbitrary-precision floating point. This corresponds to Java's {@link BigDecimal} type.
         */
        DECIMAL(Type.DECIMAL, Type.STRING),
        /**
         * An arbitrary number that can be an {@link #INTEGER}, {@link #LONG}, {@link #FLOAT}, {@link #DOUBLE},
         * {@link #BIG_INTEGER}, or {@link #DECIMAL}.
         */
        NUMBER(Type.INTEGER, Type.LONG, Type.FLOAT, Type.DOUBLE, Type.DECIMAL, Type.BIG_INTEGER, Type.STRING),
        /**
         * An instant in time. This corresponds to {@link Instant} type.
         */
        TIMESTAMP(Type.STRING, Type.LONG),
        /**
         * An arbitrary location (TBD).
         */
        LOCATION(Type.DOCUMENT, Type.STRING),
        /**
         * A {@link UUID} value.
         */
        UUID(Type.STRING), BINARY(Type.BINARY);
        
        private final Type preferredJsonType;
        private final Set<Type> jsonTypes;
        
        private FieldType(Type t1) {
            preferredJsonType = t1;
            jsonTypes = Collections.unmodifiableSet(EnumSet.of(t1));
        }
        
        private FieldType(Type t1, Type... additional) {
            preferredJsonType = t1;
            jsonTypes = Collections.unmodifiableSet(EnumSet.of(t1, additional));
        }
        
        /**
         * Get the preferred {@link Type JSON value type} for this field type.
         * @return the preferred JSON representation type; never null
         */
        public Type preferredJsonType() {
            return preferredJsonType;
        }
        
        /**
         * Get the set of {@link Type JSON value types} that this field type can be represented with.
         * @return the set of JSON representation types; never null and never empty
         * @see #jsonTypeIncludes(Type)
         */
        public Set<Type> jsonTypes() {
            return jsonTypes;
        }
        
        /**
         * Determine whether this field type can be represented as the supplied JSON {@link Type JSON value type}.
         * @param type the JSON value type
         * @return true if a field with this type can be represented in a JSON field using the given JSON value type, or false
         * otherwise
         * @see #jsonTypes()
         */
        public boolean jsonTypeIncludes(Type type) {
            return jsonTypes.contains(type);
        }
        
        /**
         * Determine the best field type to represent values of this and the supplied field type.
         * @param other the other field type
         * @return the field type that can best represent both types; never null
         */
        public FieldType union(FieldType other) {
            if (this == other || other == null) return this;
            switch (this) {
                case DOUBLE:
                    if (other == FLOAT || other == LONG || other == INTEGER) return DOUBLE;
                    if (other == DECIMAL || other == NUMBER) return other;
                    if (other == TIMESTAMP) return NUMBER;
                    return STRING;
                case FLOAT:
                    if (other == DOUBLE || other == LONG) return DOUBLE;
                    if (other == INTEGER) return FLOAT;
                    if (other == DECIMAL || other == NUMBER) return other;
                    if (other == TIMESTAMP) return NUMBER;
                    return STRING;
                case INTEGER:
                    if (other == LONG) return LONG;
                    if (other == FLOAT || other == DOUBLE || other == DECIMAL || other == NUMBER) return other;
                    if (other == TIMESTAMP) return NUMBER;
                    return STRING;
                case LONG:
                    if (other == INTEGER) return LONG;
                    if (other == FLOAT || other == DOUBLE) return DOUBLE;
                    if (other == DECIMAL || other == NUMBER) return other;
                    if (other == TIMESTAMP) return LONG;
                    return STRING;
                case DECIMAL:
                    if (other == NUMBER || other == TIMESTAMP) return NUMBER;
                    return STRING;
                case BIG_INTEGER:
                    if (other == INTEGER || other == LONG) return BIG_INTEGER;
                    if (other == DECIMAL || other == FLOAT || other == DOUBLE) return DECIMAL;
                    if (other == NUMBER || other == TIMESTAMP) return BIG_INTEGER;
                    return STRING;
                case NUMBER:
                    if (other == INTEGER || other == LONG || other == FLOAT || other == DOUBLE) return DECIMAL;
                    if (other == DECIMAL || other == TIMESTAMP) return NUMBER;
                    return STRING;
                case STRING:
                case UUID:
                case LOCATION:
                case BOOLEAN:
                    // The only non-match with these is STRING ...
                    return STRING;
                case TIMESTAMP:
                    if (other == LONG) return TIMESTAMP;
                    return STRING;
                case BINARY:
                    return BINARY;
            }
            return STRING;
        }
        
        /**
         * Infer the best field type to represent the given literal value.
         * @param value the value; may be null or may be {@link Value#nullValue()}
         * @return the field type if one can be inferred; never null but possibly empty
         */
        public static Optional<FieldType> inferFrom(Value value) {
            if (value == null || value.isNull()) return Optional.empty();
            if (value.isBoolean()) return Optional.of(FieldType.BOOLEAN);
            if (value.isInteger()) return Optional.of(FieldType.INTEGER);
            if (value.isLong()) return Optional.of(FieldType.LONG);
            if (value.isFloat()) return Optional.of(FieldType.FLOAT);
            if (value.isDouble()) return Optional.of(FieldType.DOUBLE);
            if (value.isString()) {
                String str = value.asString();
                if (isUuid(str)) return Optional.of(FieldType.UUID);
                if (isTimestamp(str)) return Optional.of(FieldType.TIMESTAMP);
                if (isLocation(str)) return Optional.of(FieldType.LOCATION);
                return Optional.of(FieldType.STRING);
            }
            if (value.isBigInteger()) return Optional.of(FieldType.BIG_INTEGER);
            if (value.isBigDecimal()) return Optional.of(FieldType.DECIMAL);
            if (value.isBinary()) return Optional.of(FieldType.BINARY);
            if (value.isNumber()) return Optional.of(FieldType.NUMBER);
            return Optional.empty();
        }
    }
    
    protected static boolean isUuid(String value) {
        try {
            UUID.fromString(value);
            return true;
        } catch (IllegalArgumentException e) {
        }
        return false;
    }
    
    protected static boolean isTimestamp(String value) {
        try {
            Instant.parse(value);
            return true;
        } catch (DateTimeParseException e) {
        }
        return false;
    }
    
    protected static boolean isLocation(String value) {
        return false;
    }
    
    protected static Optional<UUID> parseUuid(String value) {
        try {
            return Optional.of(UUID.fromString(value));
        } catch (IllegalArgumentException e) {
        }
        return Optional.empty();
    }
    
    protected static Optional<Long> parseTimestamp(String value) {
        try {
            Instant instant = Instant.parse(value);
            return Optional.of(new Long(instant.toEpochMilli()));
        } catch (DateTimeParseException e) {
        }
        return Optional.empty();
    }
    
    /**
     * A viewer of a field definition.
     */
    public static interface FieldViewer {
        /**
         * Return whether this field definition has at least one specification.
         * @return false if this field definition is empty, or true otherwise
         */
        boolean isDefined();

        /**
         * Get the description of this field.
         * @return the optional description; never null but possibly empty
         */
        Optional<String> description();

        /**
         * Get the type for this field.
         * @return the type, if known; never null but possibly empty
         */
        Optional<FieldType> type();
        
        /**
         * Determine if this field is optional.
         * @return true if this is an optional field, or false otherwise
         */
        boolean isOptional();

        /**
         * Determine if this field is an array of multiple values.
         * @return true if this is an array field, or false otherwise
         */
        boolean isArray();
        
        /**
         * Get the constraints for string-based types.
         * @return the string constraints; never null (even if the {@link #type()} is not a string type
         */
        StringConstraints stringConstraints();
        
        /**
         * Get the constraints for numeric types.
         * @return the numeric constraints; never null (even if the {@link #type()} is not a numeric type
         */
        NumberConstraints numberConstraints();
        
        /**
         * Get the constraints for location fields.
         * @return the location constraints; never null (even if the {@link #type()} is not a location type
         */
        LocationConstraints locationConstraints();
    }
    
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
    
    public static interface Enumerated {
        /**
         * Get the optional set of allowed values.
         * @return the array of allowed values; never null but possibly empty
         */
        Optional<Array> allowedValues();
    }
    
    /**
     * The constraints for string values.
     */
    public static interface StringConstraints extends Enumerated {
        /**
         * Get the optional regular expression pattern that constraints the allowed values.
         * @return the regular expression pattern; never null but possibly empty
         */
        Optional<String> pattern();
        
        /**
         * Get the optional minimum length of string values.
         * @return the minimum length; never null but possibly empty
         * @see #minLengthInclusive()
         */
        Optional<Integer> minLength();

        /**
         * Get whether the {@link #minLength() minimum length constraint} is inclusive.
         * @return true if inclusive, or false otherwise
         * @see #minLength()
         */
        boolean minLengthInclusive();
        
        /**
         * Get the optional maximum length of string values.
         * @return the maximum length; never null but possibly empty
         * @see #maxLengthInclusive()
         */
        Optional<Integer> maxLength();
        
        /**
         * Get whether the {@link #maxLength() maximum length constraint} is inclusive.
         * @return true if inclusive, or false otherwise
         * @see #maxLength()
         */
        boolean maxLengthInclusive();
    }
    
    /**
     * The constraints for numeric values.
     */
    public static interface NumberConstraints {
        Optional<Number> minValue();
        
        boolean minValueInclusive();
        
        Optional<Number> maxValue();
        
        boolean maxValueInclusive();
    }
    
    public static interface LocationConstraints {
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
     * Obtain a viewer for the field definition.
     * 
     * @param entityTypeDefinition the entity type document; may not be null
     * @param fieldName the name of the field in the entity type; may not be null
     * @return the field definition viewer; never null
     */
    public static FieldViewer viewField(Document entityTypeDefinition, String fieldName) {
        Document fieldDoc = entityTypeDefinition.getDocument(fieldName);
        if (fieldDoc == null) fieldDoc = Document.create();
        return new BasicFieldViewer(fieldDoc);
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
        return editField(patch, Path.parse(fieldName));
    }
    
    /**
     * Obtain an editor for the field definition with the given name.
     * 
     * @param patch the patch editor for modifying the entity type representation; may not be null
     * @param pathToField the path to the field definition within the entity type representation; may not be null
     * @return the field editor; never null
     */
    public static FieldEditor editField(Editor<Patch<EntityType>> patch, Path pathToField) {
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
        return createField(patch, Path.parse(fieldName));
    }
    
    /**
     * Obtain an editor that will create a new field definition with the given name.
     * 
     * @param patch the patch editor for modifying the entity type representation; may not be null
     * @param pathToField the path to the field definition within the entity type representation; may not be null
     * @return the field editor; never null
     */
    public static FieldEditor createField(Editor<Patch<EntityType>> patch, Path pathToField) {
        return new BasicFieldEditor(patch, pathToField, true);
    }
    
    protected static class BasicFieldViewer implements FieldViewer {
        private final Document field;
        
        protected BasicFieldViewer(Document fieldDoc) {
            this.field = fieldDoc;
        }
        
        @Override
        public boolean isDefined() {
            return !field.isEmpty();
        }
        
        @Override
        public Optional<String> description() {
            return Optional.ofNullable(field.getString("description"));
        }
        
        @Override
        public boolean isArray() {
            return field.getBoolean("array", false);
        }
        
        @Override
        public boolean isOptional() {
            return field.getBoolean("optional", false);
        }
        
        @Override
        public Optional<FieldType> type() {
            return Optional.ofNullable(FieldType.valueOf(field.getString("type")));
        }
        
        @Override
        public LocationConstraints locationConstraints() {
            return new LocationConstraints() {
            };
        }
        
        @Override
        public NumberConstraints numberConstraints() {
            return new NumberConstraints() {
                @Override
                public Optional<Number> minValue() {
                    return Optional.ofNullable(field.getNumber("minValue"));
                }
                
                @Override
                public boolean minValueInclusive() {
                    return field.getBoolean("minValueInclusive", true);
                }
                
                @Override
                public Optional<Number> maxValue() {
                    return Optional.ofNullable(field.getNumber("maxValue"));
                }
                
                @Override
                public boolean maxValueInclusive() {
                    return field.getBoolean("maxValueInclusive", true);
                }
            };
        }
        
        @Override
        public StringConstraints stringConstraints() {
            return new StringConstraints() {
                @Override
                public Optional<Integer> minLength() {
                    return Optional.ofNullable(field.getInteger("minLength"));
                }
                
                @Override
                public boolean minLengthInclusive() {
                    return field.getBoolean("minLengthInclusive", true);
                }
                
                @Override
                public Optional<Integer> maxLength() {
                    return Optional.ofNullable(field.getInteger("maxLength"));
                }
                
                @Override
                public boolean maxLengthInclusive() {
                    return field.getBoolean("maxLengthInclusive", true);
                }
                
                @Override
                public Optional<String> pattern() {
                    return Optional.ofNullable(field.getString("pattern"));
                }
                
                @Override
                public Optional<Array> allowedValues() {
                    return Optional.ofNullable(field.getArray("allowedValues"));
                }
            };
        }
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
            return new NumberConstraintsEditor<Schema.FieldEditor>() {
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
            return new ArrayEditor<Schema.FieldEditor>() {
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
            collections.forEach((field) -> {
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
    
    public static Document getComponent(SchemaComponentId componentId, Document schema) {
        Document component = null;
        switch (componentId.type()) {
            case ENTITY_TYPE:
                EntityType type = (EntityType) componentId;
                Document collections = schema.getDocument("collections");
                if (collections != null) {
                    component = collections.getDocument(type.entityTypeName());
                }
                break;
        }
        return component;
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
    
    private Schema() {
    }
    
}

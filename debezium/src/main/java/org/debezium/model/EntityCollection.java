/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.debezium.annotation.Immutable;
import org.debezium.message.Array;
import org.debezium.message.Document;
import org.debezium.message.Document.Field;
import org.debezium.message.Message;
import org.debezium.message.Path;
import org.debezium.message.Value;
import org.debezium.message.Value.Type;

/**
 * @author Randall Hauch
 *
 */
@Immutable
public final class EntityCollection implements SchemaComponent<EntityType> {

    public static EntityCollection with(EntityType id, Document doc) {
        return new EntityCollection(id, doc);
    }

    public static final class FieldName {
        public static final String TYPE = "type";
        public static final String DESCRIPTION = "description";
        public static final String MINIMUM_VALUE = "minValue";
        public static final String MINIMUM_VALUE_INCLUSIVE = "minValueInclusive";
        public static final String MAXIMUM_VALUE = "maxValue";
        public static final String MAXIMUM_VALUE_INCLUSIVE = "maxValueInclusive";
        public static final String MINIMUM_LENGTH = "minLength";
        public static final String MINIMUM_LENGTH_INCLUSIVE = "minLengthInclusive";
        public static final String MAXIMUM_LENGTH = "maxLength";
        public static final String MAXIMUM_LENGTH_INCLUSIVE = "maxLengthInclusive";
        public static final String PATTERN = "pattern";
        public static final String ALLOWED_VALUES = "allowedValues";
        public static final String USAGE = "$usage";
    }
    
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
        UUID(Type.STRING), BINARY(Type.BINARY),
        /**
         * A {@link Document} value.
         */
        DOCUMENT(Type.DOCUMENT);

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
         * 
         * @return the preferred JSON representation type; never null
         */
        public Type preferredJsonType() {
            return preferredJsonType;
        }

        /**
         * Get the set of {@link Type JSON value types} that this field type can be represented with.
         * 
         * @return the set of JSON representation types; never null and never empty
         * @see #jsonTypeIncludes(Type)
         */
        public Set<Type> jsonTypes() {
            return jsonTypes;
        }

        /**
         * Determine whether this field type can be represented as the supplied JSON {@link Type JSON value type}.
         * 
         * @param type the JSON value type
         * @return true if a field with this type can be represented in a JSON field using the given JSON value type, or false
         *         otherwise
         * @see #jsonTypes()
         */
        public boolean jsonTypeIncludes(Type type) {
            return jsonTypes.contains(type);
        }

        /**
         * Determine the best field type to represent values of this and the supplied field type.
         * 
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
                case DOCUMENT:
                case BOOLEAN:
                    // The only non-match with these is STRING ...
                    return STRING;
                case TIMESTAMP:
                    if (other == LONG) return TIMESTAMP;
                    return STRING;
                case BINARY:
                    // The only type compatible with BINARY is ...
                    return BINARY;
            }
            return STRING;
        }

        /**
         * Infer the best field type to represent the given literal value.
         * 
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

    public static interface Enumerated {
        /**
         * Get the optional set of allowed values.
         * 
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
         * 
         * @return the regular expression pattern; never null but possibly empty
         */
        Optional<String> pattern();

        /**
         * Get the optional minimum length of string values.
         * 
         * @return the minimum length; never null but possibly empty
         * @see #minLengthInclusive()
         */
        Optional<Integer> minLength();

        /**
         * Get whether the {@link #minLength() minimum length constraint} is inclusive.
         * 
         * @return true if inclusive, or false otherwise
         * @see #minLength()
         */
        boolean minLengthInclusive();

        /**
         * Get the optional maximum length of string values.
         * 
         * @return the maximum length; never null but possibly empty
         * @see #maxLengthInclusive()
         */
        Optional<Integer> maxLength();

        /**
         * Get whether the {@link #maxLength() maximum length constraint} is inclusive.
         * 
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

    /**
     * A viewer of a field definition.
     */
    public static interface FieldDefinition {

        /**
         * Get the name of this field.
         * 
         * @return the field's name; never null
         */
        String name();

        /**
         * Determine if this field definition is empty and has no specifications.
         * 
         * @return {@code true} if this definition is empty, or false otherwise
         */
        boolean isEmpty();

        /**
         * Get the description of this field.
         * 
         * @return the optional description; never null but possibly empty
         */
        Optional<String> description();

        /**
         * Get the type for this field.
         * 
         * @return the type, if known; never null but possibly empty
         */
        Optional<FieldType> type();

        /**
         * Determine the approximate fraction of entities that contain this field.
         * 
         * @return the fraction of entities that contain this field, ranging from 0.0 (inclusive) to 1.0 (inclusive)
         */
        float usage();

        /**
         * Determine whether this field appears within at least the specified fraction of all entities. This is a convenience
         * method that, by default, is equivalent to {@code usage() >= fractionalLowerLimt}.
         * 
         * @param fractionalLowerLimit the minimum fractional {@link #usage() usage} limit (from 0.0 to 1.0 inclusive) for this
         *            field
         * @return true if at least the specified fraction of all entities of this type contain this field
         */
        default boolean isUsageAtLeast(float fractionalLowerLimit) {
            return usage() >= fractionalLowerLimit;
        }

        /**
         * Determine if this field is an array of multiple values.
         * 
         * @return true if this is an array field, or false otherwise
         */
        boolean isArray();

        /**
         * Get the constraints for string-based types.
         * 
         * @return the string constraints; never null (even if the {@link #type()} is not a string type
         */
        StringConstraints stringConstraints();

        /**
         * Get the constraints for numeric types.
         * 
         * @return the numeric constraints; never null (even if the {@link #type()} is not a numeric type
         */
        NumberConstraints numberConstraints();

        /**
         * Get the constraints for location fields.
         * 
         * @return the location constraints; never null (even if the {@link #type()} is not a location type
         */
        LocationConstraints locationConstraints();
    }

    private static class BasicField implements FieldDefinition {
        private final Document field;
        private final String name;

        protected BasicField(String name, Document fieldDoc) {
            this.name = name;
            this.field = fieldDoc;
            assert this.field != null;
            assert this.name != null;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isEmpty() {
            return field.isEmpty();
        }

        @Override
        public Optional<String> description() {
            return Optional.ofNullable(field.getString(FieldName.DESCRIPTION));
        }

        @Override
        public boolean isArray() {
            return field.getBoolean("array", false);
        }
        
        @Override
        public float usage() {
            return field.getFloat(FieldName.USAGE, 1.0f);
        }

        @Override
        public Optional<FieldType> type() {
            return Optional.ofNullable(FieldType.valueOf(field.getString(FieldName.TYPE)));
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
                    return Optional.ofNullable(field.getNumber(FieldName.MINIMUM_VALUE));
                }

                @Override
                public boolean minValueInclusive() {
                    return field.getBoolean(FieldName.MINIMUM_VALUE_INCLUSIVE, true);
                }

                @Override
                public Optional<Number> maxValue() {
                    return Optional.ofNullable(field.getNumber(FieldName.MAXIMUM_VALUE));
                }

                @Override
                public boolean maxValueInclusive() {
                    return field.getBoolean(FieldName.MAXIMUM_VALUE_INCLUSIVE, true);
                }
            };
        }

        @Override
        public StringConstraints stringConstraints() {
            return new StringConstraints() {
                @Override
                public Optional<Integer> minLength() {
                    return Optional.ofNullable(field.getInteger(FieldName.MINIMUM_LENGTH));
                }

                @Override
                public boolean minLengthInclusive() {
                    return field.getBoolean(FieldName.MINIMUM_LENGTH_INCLUSIVE, true);
                }

                @Override
                public Optional<Integer> maxLength() {
                    return Optional.ofNullable(field.getInteger(FieldName.MAXIMUM_LENGTH));
                }

                @Override
                public boolean maxLengthInclusive() {
                    return field.getBoolean(FieldName.MAXIMUM_LENGTH_INCLUSIVE, true);
                }

                @Override
                public Optional<String> pattern() {
                    return Optional.ofNullable(field.getString(FieldName.PATTERN));
                }

                @Override
                public Optional<Array> allowedValues() {
                    return Optional.ofNullable(field.getArray(FieldName.ALLOWED_VALUES));
                }
            };
        }
    }

    static Path pathToField(String fieldName) {
        return FIELDS_PATH.append(fieldName);
    }

    static Path pathToField(Path fieldPath) {
        if (fieldPath.isRoot()) return FIELDS_PATH;
        if (fieldPath.isSingle()) return FIELDS_PATH.append(fieldPath);
        Path result = Path.root();
        for (String segment : fieldPath) {
            result = result.append(FIELDS_NAME);
            result = result.append(segment);
        }
        return result;
    }

    static Path fieldsPath() {
        return FIELDS_PATH;
    }

    private static final String FIELDS_NAME = Message.Field.FIELDS;
    private static final Path FIELDS_PATH = Path.parse(FIELDS_NAME);

    private final EntityType id;
    private final Document doc;

    protected EntityCollection(EntityType id, Document doc) {
        this.id = id;
        this.doc = doc;
    }

    @Override
    public EntityType id() {
        return id;
    }

    @Override
    public Document document() {
        return doc;
    }

    public Stream<FieldDefinition> fields() {
        return doc.children(FIELDS_PATH).filter(this::isDocument).map(this::toFieldDefinition);
    }

    public Optional<FieldDefinition> field(String name) {
        Document fields = doc.getDocument(FIELDS_NAME);
        Value value = fields != null ? fields.get(name) : null;
        return Value.isNull(value) ? Optional.empty() : Optional.of(toFieldDefinition(name, value));
    }

    public Optional<FieldDefinition> field(Path path) {
        if (path.isSingle()) return field(path.lastSegment().get());
        Optional<Value> value = doc.find(pathToField(path));
        if (value.isPresent() && value.get().isNotNull()) {
            String fieldName = path.lastSegment().get();
            FieldDefinition defn = toFieldDefinition(fieldName, value.get());
            return Optional.of(defn);
        }
        return Optional.empty();
    }

    protected boolean isDocument(Field field) {
        return field != null && field.getValue().isDocument();
    }

    protected FieldDefinition toFieldDefinition(Field field) {
        return toFieldDefinition(field.getName().toString(), field.getValue());
    }

    protected FieldDefinition toFieldDefinition(String name, Value value) {
        return new BasicField(name, value.asDocument());
    }
    
    @Override
    public String toString() {
        return document().toString();
    }

}

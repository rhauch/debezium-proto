/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.debezium.message;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class ReplaceOperation extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ReplaceOperation\",\"namespace\":\"org.debezium.message\",\"fields\":[{\"name\":\"path\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence path;
  @Deprecated public java.lang.CharSequence value;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public ReplaceOperation() {}

  /**
   * All-args constructor.
   */
  public ReplaceOperation(java.lang.CharSequence path, java.lang.CharSequence value) {
    this.path = path;
    this.value = value;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return path;
    case 1: return value;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: path = (java.lang.CharSequence)value$; break;
    case 1: value = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'path' field.
   */
  public java.lang.CharSequence getPath() {
    return path;
  }

  /**
   * Sets the value of the 'path' field.
   * @param value the value to set.
   */
  public void setPath(java.lang.CharSequence value) {
    this.path = value;
  }

  /**
   * Gets the value of the 'value' field.
   */
  public java.lang.CharSequence getValue() {
    return value;
  }

  /**
   * Sets the value of the 'value' field.
   * @param value the value to set.
   */
  public void setValue(java.lang.CharSequence value) {
    this.value = value;
  }

  /** Creates a new ReplaceOperation RecordBuilder */
  public static org.debezium.message.ReplaceOperation.Builder newBuilder() {
    return new org.debezium.message.ReplaceOperation.Builder();
  }
  
  /** Creates a new ReplaceOperation RecordBuilder by copying an existing Builder */
  public static org.debezium.message.ReplaceOperation.Builder newBuilder(org.debezium.message.ReplaceOperation.Builder other) {
    return new org.debezium.message.ReplaceOperation.Builder(other);
  }
  
  /** Creates a new ReplaceOperation RecordBuilder by copying an existing ReplaceOperation instance */
  public static org.debezium.message.ReplaceOperation.Builder newBuilder(org.debezium.message.ReplaceOperation other) {
    return new org.debezium.message.ReplaceOperation.Builder(other);
  }
  
  /**
   * RecordBuilder for ReplaceOperation instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ReplaceOperation>
    implements org.apache.avro.data.RecordBuilder<ReplaceOperation> {

    private java.lang.CharSequence path;
    private java.lang.CharSequence value;

    /** Creates a new Builder */
    private Builder() {
      super(org.debezium.message.ReplaceOperation.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(org.debezium.message.ReplaceOperation.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.path)) {
        this.path = data().deepCopy(fields()[0].schema(), other.path);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.value)) {
        this.value = data().deepCopy(fields()[1].schema(), other.value);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing ReplaceOperation instance */
    private Builder(org.debezium.message.ReplaceOperation other) {
            super(org.debezium.message.ReplaceOperation.SCHEMA$);
      if (isValidValue(fields()[0], other.path)) {
        this.path = data().deepCopy(fields()[0].schema(), other.path);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.value)) {
        this.value = data().deepCopy(fields()[1].schema(), other.value);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'path' field */
    public java.lang.CharSequence getPath() {
      return path;
    }
    
    /** Sets the value of the 'path' field */
    public org.debezium.message.ReplaceOperation.Builder setPath(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.path = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'path' field has been set */
    public boolean hasPath() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'path' field */
    public org.debezium.message.ReplaceOperation.Builder clearPath() {
      path = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'value' field */
    public java.lang.CharSequence getValue() {
      return value;
    }
    
    /** Sets the value of the 'value' field */
    public org.debezium.message.ReplaceOperation.Builder setValue(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.value = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'value' field has been set */
    public boolean hasValue() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'value' field */
    public org.debezium.message.ReplaceOperation.Builder clearValue() {
      value = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public ReplaceOperation build() {
      try {
        ReplaceOperation record = new ReplaceOperation();
        record.path = fieldSetFlags()[0] ? this.path : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.value = fieldSetFlags()[1] ? this.value : (java.lang.CharSequence) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
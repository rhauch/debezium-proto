/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.message;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Path;
import org.debezium.core.doc.Value;
import org.debezium.core.function.Predicates;
import org.debezium.core.util.Iterators;
import org.debezium.core.util.MathOps;

/**
 * A set of {@link Operation operations} applied to a single identified object. A patch is created by {@link #create creating} an {@link Editor editor} for the target, recording one or more
 * operations that should be applied to the target, and then call {@link Editor#end()}.
 * <p>
 * Creating a patch does not imply the actual target is changed. Instead, the patch represents a set of changes that <i>can</i> be made to the target. Only when the patch is applied will the changes
 * be made.
 * <p>
 * A patch can be made to be conditional via the {@link Require} operation, captured via the editor's {@link Editor#require(String, Value)} method. When a patch is applied, the patch will be rejected
 * if any require operation fails.
 * <p>
 * The application of a patch to a target will be idempotent, meaning that the patch can be applied to the target object once or repeatedly in-sequence with no difference in effect.
 * <p>
 * This class is based upon the <a href="http://tools.ietf.org/html/rfc6902">JavaScript Object Notation (JSON) Patch [RFC6902]</a> Internet Standards Track document. The primary difference is that
 * this patch definition attempts to be fully idempotent, which JSON Patch may not be since operations such as "add" fail if there is already a value at the given path.
 * 
 * @author Randall Hauch
 * @param <IdType> the type of identifier that should be referenced by the patch
 */
public final class Patch<IdType extends Identifier> implements Iterable<Patch.Operation> {
    
    /**
     * The type of action for an {@link Operation}.
     */
    public static enum Action {
        ADD("add"), REMOVE("remove"), REPLACE("replace"), MOVE("move"), COPY("copy"), REQUIRE("test"), INCREMENT("inc");
        private static Map<String, Action> actionsByLowercase = new HashMap<>();
        static {
            for (Action action : Action.values()) {
                actionsByLowercase.put(action.lowercase(), action);
            }
        }
        private final String lowercase;
        
        private Action(String lowercase) {
            this.lowercase = lowercase;
        }
        
        public String lowercase() {
            return this.lowercase;
        }
        
        public static Action fromLowercase(String action) {
            return actionsByLowercase.get(action);
        }
    }
    
    /**
     * An operation within a patch.
     */
    public static interface Operation extends BiFunction<Document,Consumer<Path>,Boolean> {
        /**
         * Get the type of action performed by this operation.
         * 
         * @return the action; never null
         */
        Action action();
        
        /**
         * Get a JSON-Patch document representation of this operation.
         * 
         * @return the document; never null
         */
        Document asDocument();
        
        /**
         * Get a JSON-Patch document representation of this operation wrapped in a {@link Value}.
         * 
         * @return the document as a {@link Value}; never null
         */
        default Value asValue() {
            return Value.create(asDocument());
        }
        
        /**
         * Get a failure description for this operation.
         * @return the failure description; never null
         */
        String failureDescription();
    }
    
    /**
     * An operation that adds a value to the target at a given path. The behavior of the operation depends on what the path
     * references within the target when the patch is applied:
     * <ul>
     * <li>If the path specifies an array index, a new value is inserted into the array at the specified index.</li>
     * <li>If the path specifies an object member that does not already exist, a new member is added to the object.</li>
     * <li>If the path specifies an object member that does exist, that member's value is replaced.</li>
     * </ul>
     * When the operation is applied, the path MUST reference one of the following within the target:
     * <ul>
     * <li>The root of the target, whereupon the specified value becomes the entire content of the target document.</li>
     * <li>A non-existent member in the target, whereupon the supplied value is added to the target at the indicated path.</li>
     * <li>An existing member in the target, whereupon the supplied value replaces the existing value.</li>
     * <li>An element to add to an existing array, whereupon the supplied value is added to the array at the indicated location in the array specified by the 0-based index. Any elements at or above
     * the specified index are shifted one position to the right. If the specified index is greater than the number of elements in the array, if the index is a single "-" character, or if no index is
     * given, then the value will be appended to the array.</li>
     * </ul>
     * Because this operation is designed to add to existing objects and arrays, the specified path in the target will
     * often not exist. Additionally, the parent or ancestor objects need not exist in the target, as any missing ancestors
     * will be added as required.
     */
    public static interface Add extends Operation {
        /**
         * Get the path at which the value should be added.
         * 
         * @return the path; never null
         */
        String path();
        
        /**
         * The value that should be added.
         * 
         * @return the value; never null
         */
        Value value();
        
        @Override
        default Action action() {
            return Action.ADD;
        }
    }
    
    /**
     * An operation that removes from the target a member at the given path. If the target does not contain a member at the
     * path when this patch is applied, then this operation has no effect.
     */
    public static interface Remove extends Operation {
        /**
         * Get the path at which the existing member should be removed.
         * 
         * @return the path; never null
         */
        String path();
        
        @Override
        default Action action() {
            return Action.REMOVE;
        }
    }
    
    /**
     * An operation that replaces the value in an existing member at the given path in the target. Functionally, this is
     * identical to a {@link Remove} operation at the same path followed by an {@link Add} operation at the same path with
     * the new value. If the target does not contain a member at the path when this patch is applied, then this operation
     * is equivalent to an {@link Add}.
     */
    public static interface Replace extends Operation {
        /**
         * Get the path at which the existing value should be replaced.
         * 
         * @return the path; never null
         */
        String path();
        
        /**
         * The new value that should replace the existing value.
         * 
         * @return the new value; never null
         */
        Value value();
        
        @Override
        default Action action() {
            return Action.REPLACE;
        }
    }
    
    /**
     * An operation that moves the value from one member to another within the same target. Functionally, this is
     * identical to a {@link Remove} operation at the "from" path followed by an {@link Add} operation at the "to" path with
     * the same value that was removed. If the target does not contain a member at the "from" path when this
     * patch is applied, then this operation will have no effect.
     */
    public static interface Move extends Operation {
        /**
         * Get the path from which the value should moved.
         * 
         * @return the old path for the value; never null
         */
        String fromPath();
        
        /**
         * Get the path to which the value should moved.
         * 
         * @return the new path for the value; never null
         */
        String toPath();
        
        @Override
        default Action action() {
            return Action.MOVE;
        }
    }
    
    /**
     * An operation that copies the value from one member to another within the same target. Functionally, this is
     * identical to an {@link Add} operation at the "to" path with the same value as the member at the "from" path.
     * If the target does not contain a member at the "from" path when this patch is applied, then this operation
     * will have no effect.
     */
    public static interface Copy extends Operation {
        /**
         * Get the path from which the value should copied.
         * 
         * @return the path of the original value; never null
         */
        String fromPath();
        
        /**
         * Get the path at which the copy of the value should placed.
         * 
         * @return the path where the copy is to be placed; never null
         */
        String toPath();
        
        @Override
        default Action action() {
            return Action.COPY;
        }
    }
    
    /**
     * An operation that requires a value for a member at a given path within the target matches a supplied value.
     * The operation succeeds only if the values are logically equivalent, and fails otherwise. A null required value
     * implies that the path does not exist in the target.
     * <p>
     * If a test is included in a {@link Patch}, then the path will only be applied if all tests succeed. Thus, test operations make it possible to have conditional patches. Note that a patch with
     * only tests serves no purpose.
     * </p>
     */
    public static interface Require extends Operation {
        /**
         * Get the path at which the value is expected to match the {@link #requiredValue() required value}.
         * 
         * @return the path; never null
         */
        String path();
        
        /**
         * The value that is expected at the {@link #path() path}.
         * 
         * @return the required value; if null, then the value is expected not to exist
         */
        Value requiredValue();
        
        @Override
        default Action action() {
            return Action.REQUIRE;
        }
    }
    
    /**
     * An operation that increments the numeric value in the target at a given path. If the existing value does not exist
     * or is not a number, the operation will fail.
     */
    public static interface Increment extends Operation {
        /**
         * Get the path at which the value should be added.
         * 
         * @return the path; never null
         */
        String path();
        
        /**
         * The value that should be added.
         * 
         * @return the value; never null
         */
        Number value();
        
        @Override
        default Action action() {
            return Action.INCREMENT;
        }
    }
    
    /**
     * An interface for editing a target object and creating a patch that represents those changes.
     * 
     * @param <P> the type of object returned by the editor when complete.
     */
    public static interface Editor<P> {
        
        /**
         * Add a requirement to the patch that the value at the given path matches an expected value.
         * 
         * @param path the path; may not be null
         * @param expectedValue the expected value; may be null if the target should not have a value at the specified path
         * @return this editor instance to easily chain together multiple method invocations on the editor; never null
         * @throws IllegalArgumentException if {@code path} is null
         * @see Require
         */
        Editor<P> require(String path, Value expectedValue);
        
        /**
         * Remove from the target the member at the given path.
         * 
         * @param path the path; may not be null
         * @return this editor instance to easily chain together multiple method invocations on the editor; never null
         * @throws IllegalArgumentException if {@code path} is null
         * @see Remove
         */
        Editor<P> remove(String path);
        
        /**
         * Add to the target the value at the given path.
         * 
         * @param path the path; may not be null
         * @param value the value to be added; may not be null
         * @return this editor instance to easily chain together multiple method invocations on the editor; never null
         * @throws IllegalArgumentException if either {@code path} or {@code value} is null
         * @see Add
         */
        Editor<P> add(String path, Value value);
        
        /**
         * Replace the value in the target at the given path with the new value.
         * 
         * @param path the path; may not be null
         * @param newValue the value to replace the old value (if one exists); if null then this is equivalent to calling {@link #remove(String)}.
         * @return this editor instance to easily chain together multiple method invocations on the editor; never null
         * @throws IllegalArgumentException if {@code path} is null
         * @see Replace
         */
        Editor<P> replace(String path, Value newValue);
        
        /**
         * Move the value in the target from one path to another.
         * 
         * @param fromPath the path in the target of the existing value; may not be null
         * @param toPath the new path for the value in the target; may not be null
         * @return this editor instance to easily chain together multiple method invocations on the editor; never null
         * @throws IllegalArgumentException if either {@code fromPath} or {@code toPath} is null
         * @see Move
         */
        Editor<P> move(String fromPath, String toPath);
        
        /**
         * Copy into a new path the value in the target at one path.
         * 
         * @param fromPath the path in the target of the existing value to be copied; may not be null
         * @param toPath the new path where the copy of the value is to be placed in the target; may not be null
         * @return this editor instance to easily chain together multiple method invocations on the editor; never null
         * @throws IllegalArgumentException if either {@code fromPath} or {@code toPath} is null
         * @see Copy
         */
        Editor<P> copy(String fromPath, String toPath);
        
        /**
         * Copy into a new path the value in the target at one path.
         * 
         * @param path the path; may not be null
         * @param increment the increment by which the numeric value should be changed; may be positive or negative
         * @return this editor instance to easily chain together multiple method invocations on the editor; never null
         * @throws IllegalArgumentException if either {@code path} or {@code increment} is null
         */
        Editor<P> increment(String path, Number increment);
        
        /**
         * Complete the editing of the identified object.
         * 
         * @return the patch containing the operations; never null, although the patch may be empty
         */
        P end();
        
        /**
         * Complete the editing of the identified object if and only if there is at least one operation.
         * 
         * @return the patch containing the 1+ operations, or null if there are no operations and the patch would be ineffective
         */
        Optional<P> endIfChanged();
    }
    
    public static <T extends Identifier> Patch<T> destroy(T target) {
        return edit(target).remove("/").end();
    }
    
    public static <T extends Identifier> Patch<T> create(T target, Document value) {
        return edit(target).add("/", Value.create(value)).end();
    }
    
    public static <T extends Identifier> Patch<T> read(T target) {
        return edit(target).end();
    }
    
    /**
     * Create an editor for the target with the given identifier. The resulting editor will create the {@link Patch} when {@link Editor#end()} is called.
     * 
     * @param id the identifier of the target
     * @return the editor that can create the patch; never null
     * @param <T> the type of identifier (or target) that should be edit
     */
    public static <T extends Identifier> Editor<Patch<T>> edit(T id) {
        return new Editor<Patch<T>>() {
            private List<Operation> ops = new LinkedList<>();
            
            @Override
            public Editor<Patch<T>> add(String path, Value value) {
                ops.add(new AddOp(path, value));
                return this;
            }
            
            @Override
            public Editor<Patch<T>> remove(String path) {
                ops.add(new RemoveOp(path));
                return this;
            }
            
            @Override
            public Editor<Patch<T>> replace(String path, Value newValue) {
                ops.add(new ReplaceOp(path, newValue));
                return this;
            }
            
            @Override
            public Editor<Patch<T>> copy(String fromPath, String toPath) {
                ops.add(new CopyOp(fromPath, toPath));
                return this;
            }
            
            @Override
            public Editor<Patch<T>> move(String fromPath, String toPath) {
                ops.add(new MoveOp(fromPath, toPath));
                return this;
            }
            
            @Override
            public Editor<Patch<T>> require(String path, Value expectedValue) {
                ops.add(new RequireOp(path, expectedValue));
                return this;
            }
            
            @Override
            public Editor<Patch<T>> increment(String path, Number increment) {
                ops.add(new IncrementOp(path, increment));
                return this;
            }
            
            @Override
            public Patch<T> end() {
                return new Patch<T>(id, ops);
            }
            
            @Override
            public Optional<Patch<T>> endIfChanged() {
                return ops.isEmpty() ? Optional.empty() : Optional.of(end());
            }
            
            @Override
            public String toString() {
                return "Patch editor for '" + id + "'";
            }
        };
    }
    
    protected static final class AddOp implements Add {
        private final String path;
        private final Value value;
        
        protected AddOp(String path, Value value) {
            this.path = path;
            this.value = value;
        }
        
        @Override
        public String path() {
            return path;
        }
        
        @Override
        public Value value() {
            return value;
        }
        
        @Override
        public String toString() {
            return "[ add @ '" + path + "' " + value + " ]";
        }
        
        @Override
        public Document asDocument() {
            return Document.create("op", action().lowercase(), "path", path(), "value", value());
        }
        
        @Override
        public String failureDescription() {
            return "Unable to add the value " + value + " at '" + path + "'";
        }
        
        @Override
        public Boolean apply(Document doc, Consumer<Path> invalid) {
            return doc.set(Path.parse(this.path), true, value(),invalid).isPresent();
        }
    }
    
    protected static final class RemoveOp implements Remove {
        private final String path;
        
        protected RemoveOp(String path) {
            this.path = path;
        }
        
        @Override
        public String path() {
            return path;
        }
        
        @Override
        public String toString() {
            return "[ remove @ '" + path + "' ]";
        }
        
        @Override
        public String failureDescription() {
            return "Unable to remove the value at '" + path + "'";
        }
        
        @Override
        public Document asDocument() {
            return Document.create("op", action().lowercase(), "path", path());
        }
        
        @Override
        public Boolean apply(Document doc, Consumer<Path> invalid) {
            Path path = Path.parse(this.path);
            if ( path.isRoot() ) {
                // The whole document is to be removed ...
                if ( doc.isEmpty() ) {
                    return Boolean.FALSE;   // already empty, so not modified
                }
                doc.clear();
                return Boolean.TRUE;
            }
            AtomicBoolean result = new AtomicBoolean(false);
            // If the parent path does exist, remove the child from the parent ...
            doc.find(path.parent().get(),(missingPath,missingIndex)->Optional.empty(),invalid).ifPresent((parent)->{
                if ( parent.isArray() ) {
                    Path.Segments.asInteger(path.lastSegment()).ifPresent((index)->{
                        parent.asArray().remove(index.intValue());
                        result.set(true);
                    });
                } else if ( parent.isDocument() ) {
                    result.set( parent.asDocument().remove(path.lastSegment()) != null );
                }
            });
            return Boolean.valueOf(result.get());
        }
    }
    
    protected static final class RequireOp implements Require {
        private final String path;
        private final Value value;
        
        protected RequireOp(String path, Value requiredValue) {
            this.path = path;
            this.value = requiredValue;
        }
        
        @Override
        public String path() {
            return path;
        }
        
        @Override
        public Value requiredValue() {
            return value;
        }
        
        @Override
        public String toString() {
            return "[ require @ '" + path + "' = " + value + " ]";
        }
        
        @Override
        public String failureDescription() {
            return "Failed to find required value " + value + " at '" + path + "'";
        }
        
        @Override
        public Document asDocument() {
            return Document.create("op", action().lowercase(), "path", path(), "value", requiredValue());
        }
        
        @Override
        public Boolean apply(Document doc, Consumer<Path> invalid) {
            Path path = Path.parse(this.path);
            Optional<Value> result = doc.find(path, (missingPath,missingIndex)->Optional.empty(),invalid);
            if ( !result.isPresent() || !result.get().equals(requiredValue()) ) {
                invalid.accept(path);
            }
            return Boolean.FALSE;   // this never modifies the document
        }
    }
    
    protected static final class ReplaceOp implements Replace {
        private final String path;
        private final Value value;
        
        protected ReplaceOp(String path, Value value) {
            this.path = path;
            this.value = value;
        }
        
        @Override
        public String path() {
            return path;
        }
        
        @Override
        public Value value() {
            return value;
        }
        
        @Override
        public String toString() {
            return "[ replace @ '" + path + "' with " + value + " ]";
        }
        
        @Override
        public String failureDescription() {
            return "Unable to replace the value at '" + path + "' with " + value;
        }
        
        @Override
        public Document asDocument() {
            return Document.create("op", action().lowercase(), "path", path(), "value", value());
        }
        
        @Override
        public Boolean apply(Document doc, Consumer<Path> invalid) {
            return doc.set(Path.parse(this.path), true, value(), invalid).isPresent();
        }
    }
    
    protected static final class CopyOp implements Copy {
        private final String fromPath;
        private final String toPath;
        
        protected CopyOp(String fromPath, String toPath) {
            this.fromPath = fromPath;
            this.toPath = toPath;
        }
        
        @Override
        public String fromPath() {
            return fromPath;
        }
        
        @Override
        public String toPath() {
            return toPath;
        }
        
        @Override
        public String toString() {
            return "[ copy @ '" + fromPath + "' to " + toPath + " ]";
        }
        
        @Override
        public String failureDescription() {
            return "Unable to copy the value from '" + fromPath + "' to '" + toPath + "'";
        }
        
        @Override
        public Document asDocument() {
            return Document.create("op", action().lowercase(), "fromPath", fromPath(), "toPath", toPath());
        }
        @Override
        public Boolean apply(Document doc, Consumer<Path> invalid) {
            if ( fromPath.equals(toPath)) {
                // Nothing to change ...
                return Boolean.FALSE;
            }
            
            // Silently look for the 'from' value ...
            Path fromPath = Path.parse(this.fromPath);
            Optional<Value> from = doc.find(fromPath, (missingPath,missingIndex)->Optional.empty(),(invalidPath)->{});
            if ( from.isPresent() ) {
                Path toPath = Path.parse(this.toPath);
                return doc.set(toPath, true, from.get(), invalid).isPresent();
            }
            return Boolean.FALSE;
        }
    }
    
    protected static final class MoveOp implements Move {
        private final String fromPath;
        private final String toPath;
        
        protected MoveOp(String fromPath, String toPath) {
            this.fromPath = fromPath;
            this.toPath = toPath;
        }
        
        @Override
        public String fromPath() {
            return fromPath;
        }
        
        @Override
        public String toPath() {
            return toPath;
        }
        
        @Override
        public String toString() {
            return "[ move @ '" + fromPath + "' to " + toPath + " ]";
        }
        
        @Override
        public String failureDescription() {
            return "Unable to move the value from '" + fromPath + "' to '" + toPath + "'";
        }
        
        @Override
        public Document asDocument() {
            return Document.create("op", action().lowercase(), "fromPath", fromPath(), "toPath", toPath());
        }
        
        @Override
        public Boolean apply(Document doc, Consumer<Path> invalid) {
            if ( fromPath.equals(toPath)) {
                // Nothing to change ...
                return Boolean.FALSE;
            }
            
            // Silently look for the 'from' value ...
            Path fromPath = Path.parse(this.fromPath);
            Optional<Path> fromParentPath = fromPath.parent();
            if ( fromParentPath.isPresent() ) {
                Optional<Value> fromParent = doc.find(fromParentPath.get(),
                                                      (missingPath,missingIndex)->Optional.empty(),(invalidPath)->{});
                if ( fromParent.isPresent() ) {
                    Value parent = fromParent.get();
                    if ( parent.isDocument() ) {
                        Value valueToMove = parent.asDocument().remove(fromPath.lastSegment());
                        Path toPath = Path.parse(this.toPath);
                        return doc.set(toPath, true, valueToMove, invalid).isPresent();
                    }
                }
            }
            return Boolean.FALSE;
        }
    }
    
    protected static final class IncrementOp implements Increment {
        private final String path;
        private final Number value;
        
        protected IncrementOp(String path, Number value) {
            this.path = path;
            this.value = value;
        }
        
        @Override
        public String path() {
            return path;
        }
        
        @Override
        public Number value() {
            return value;
        }
        
        @Override
        public String toString() {
            return "[ incr @ '" + path + "' by " + value + " ]";
        }
        
        @Override
        public String failureDescription() {
            return "Unable to increment by " + value + " the value at '" + path + "'";
        }
        
        @Override
        public Document asDocument() {
            return Document.create("op", action().lowercase(), "path", path(), "value", value());
        }

        @Override
        public Boolean apply(Document doc, Consumer<Path> invalid) {
            Path path = Path.parse(this.path);
            Optional<Value> existing = doc.find(path,(missingPath,missingIndex)->Optional.empty(),invalid);
            Value incremented = null;
            if ( existing.isPresent() ) {
                incremented = Value.create(MathOps.add(existing.get().asNumber(),value()));
            } else {
                incremented = Value.create(value());
            }
            doc.set(path, true, incremented, invalid);
            return Boolean.TRUE;
        }
    }
    
    private final IdType id;
    private final List<Operation> ops;
    
    protected Patch(IdType id, List<Operation> ops) {
        this.id = id;
        this.ops = ops;
    }
    
    public IdType target() {
        return id;
    }
    
    @Override
    public Iterator<Operation> iterator() {
        return Iterators.readOnly(ops.iterator());
    }
    
    public Stream<Operation> stream() {
        return ops.stream();
    }
    
    public int operationCount() {
        return ops.size();
    }
    
    @Override
    public String toString() {
        return id.toString() + " : { " + ops.stream().map(Object::toString).collect(Collectors.joining(", ")) + " }";
    }
    
    public boolean isEmpty() {
        return ops.isEmpty();
    }
    
    public boolean isReadRequest() {
        return isEmpty();
    }
    
    public boolean isCreation() {
        if (ops.size() != 1) return false;
        return ops.stream().anyMatch(Patch::isCreationOperation);
    }
    
    public boolean isDeletion() {
        if (ops.size() != 1) return false;
        return ops.stream().anyMatch(Patch::isDeletionOperation);
    }
    
    private static boolean isCreationOperation( Operation op ) {
        if ( op instanceof Add ) {
            Add add = (Add)op;
            return add.path().equals("/") && add.value().isDocument();
        }
        return false;
    }
    
    private static boolean isDeletionOperation( Operation op ) {
        return op instanceof Remove && ((Remove) op).path().equals("/");
    }
    
    public void ifCreation( Consumer<Document> consumer ) {
        if ( isCreation() ) {
            Add addOp = (Add) ops.get(0);
            consumer.accept(addOp.asValue().asDocument());
        }
    }
    
    public Document asDocument() {
        Document doc = Document.create();
        doc.putAll(id.fields());
        doc.setArray("ops",Array.create(ops.stream().map(Operation::asValue).collect(Collectors.toList())));
        return doc;
    }
    
    public static Patch<DatabaseId> forDatabase(Document doc) {
        return from(doc);
    }
    
    public static Patch<EntityType> forEntityType(Document doc) {
        return from(doc);
    }
    
    public static Patch<EntityId> forEntity(Document doc) {
        return from(doc);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <IdType extends Identifier> Patch<IdType> from(Document doc) {
        Identifier id = Message.getId(doc);
        if (id == null) return null;
        Array ops = doc.getArray("ops");
        List<Operation> operations = ops.streamValues().filter(Value::isDocument)
                                                       .map(Patch::toOperation)
                                                       .filter(Predicates.notNull())
                                                       .collect(Collectors.toList());
        return new Patch(id, operations);
    }
    
    private static Operation toOperation(Value value) {
        Document doc = value.asDocument();
        switch (Action.fromLowercase(doc.getString("op"))) {
            case ADD:
                return new AddOp(doc.getString("path"), doc.get("value"));
            case REMOVE:
                return new RemoveOp(doc.getString("path"));
            case MOVE:
                return new MoveOp(doc.getString("fromPath"), doc.getString("toPath"));
            case COPY:
                return new CopyOp(doc.getString("fromPath"), doc.getString("toPath"));
            case REPLACE:
                return new ReplaceOp(doc.getString("path"), doc.get("value"));
            case REQUIRE:
                return new RequireOp(doc.getString("path"), doc.get("value"));
            case INCREMENT:
                return new IncrementOp(doc.getString("path"), doc.get("value").asNumber());
        }
        return null;
    }
    
    /**
     * Apply all of this patch's operations to the supplied document, using the supplied consumer function when any operation
     * fails because of a bad path.
     * @param document the document that should be patched; never null
     * @param failed the function that is called for each failed operation
     * @return true if any of the operations modified the document, or false if the document is unchanged
     */
    public boolean apply(Document document, Consumer<Operation> failed ) {
        if ( isEmpty() ) return false;
        if ( isCreation() ) {
            document.putAll(((Add)ops.get(0)).value().asDocument());
            Message.addId(document, id);
            return true;
        }
        if ( isDeletion() ) {
            return false;
        }
        return ops.stream().map((op)->op.apply(document,(invalidPath)->failed.accept(op))).count() > 0;
    }
}

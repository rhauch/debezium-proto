/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.message;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.debezium.annotation.Immutable;
import org.debezium.function.Predicates;
import org.debezium.message.Message.Field;
import org.debezium.model.DatabaseId;
import org.debezium.model.EntityId;
import org.debezium.model.EntityType;
import org.debezium.model.Identifier;
import org.debezium.util.Iterators;
import org.debezium.util.MathOps;

/**
 * A set of {@link Operation operations} applied to a single identified object.
 * <p>
 * A patch is created by {@link #create creating} an {@link Editor editor} for the target, recording one or more operations that
 * should be applied to the target, and then call {@link Editor#end()}.
 * <p>
 * Creating a patch does not imply the actual target is changed. Instead, the patch represents a set of changes that <i>can</i> be
 * made to the target. Only when the patch is applied will the changes be made.
 * <p>
 * A patch can be made to be conditional via the {@link Require} operation, captured via the editor's
 * {@link Editor#require(String, Value)} method. When a patch is applied, the patch will be rejected if any require operation
 * fails.
 * <p>
 * The application of a patch to a target will be idempotent, meaning that the patch can be applied to the target object once or
 * repeatedly in-sequence with no difference in effect.
 * <p>
 * This class is based upon the <a href="http://tools.ietf.org/html/rfc6902">JavaScript Object Notation (JSON) Patch [RFC6902]</a>
 * Internet Standards Track document. The primary difference is that this patch definition attempts to be fully idempotent, which
 * JSON Patch may not be since operations such as "add" fail if there is already a value at the given path.
 * 
 * @author Randall Hauch
 * @param <IdType> the type of identifier that should be referenced by the patch
 */
@Immutable
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
    public static interface Operation extends BiFunction<Document, Consumer<Path>, Boolean> {
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
         * 
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
     * <li>An element to add to an existing array, whereupon the supplied value is added to the array at the indicated location in
     * the array specified by the 0-based index. Any elements at or above the specified index are shifted one position to the
     * right. If the specified index is greater than the number of elements in the array, if the index is a single "-" character,
     * or if no index is given, then the value will be appended to the array.</li>
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
     * If a test is included in a {@link Patch}, then the path will only be applied if all tests succeed. Thus, test operations
     * make it possible to have conditional patches. Note that a patch with only tests serves no purpose.
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
         * @param newValue the value to replace the old value (if one exists); if null then this is equivalent to calling
         *            {@link #remove(String)}.
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
        default Optional<P> endIfChanged() {
            return endIfChanged(null);
        }

        /**
         * Complete the editing of the identified object if and only if there is at least one operation.
         * 
         * @param significant function that determines if the given operation is significant enough to change the target; may be null if all operations are significant
         * @return the patch containing the 1+ operations, or null if there are no operations and the patch would be ineffective
         */
        Optional<P> endIfChanged( Predicate<Operation> significant);
    }

    /**
     * Create a patch that destroys the identified target.
     * 
     * @param target the target identifier; may not be null
     * @return the patch; never null
     */
    public static <T extends Identifier> Patch<T> destroy(T target) {
        return edit(target).remove("/").end();
    }

    /**
     * Create a patch that overwrites the identified target with the given representation.
     * 
     * @param target the target identifier; may not be null
     * @param value the new representation for the target; may not be null
     * @return the patch; never null
     */
    public static <T extends Identifier> Patch<T> replace(T target, Document value) {
        assert value != null;
        return edit(target).add("/", Value.create(value)).end();
    }

    /**
     * Create a patch that creates the identified target with the given representation.
     * 
     * @param target the target identifier; may not be null
     * @param value the new representation for the target; may not be null
     * @return the patch; never null
     */
    public static <T extends Identifier> Patch<T> create(T target, Document value) {
        assert value != null;
        return edit(target).add("/", Value.create(value)).end();
    }

    /**
     * Create a patch that creates the identified target with an empty representation.
     * 
     * @param target the target identifier; may not be null
     * @return the patch; never null
     */
    public static <T extends Identifier> Patch<T> create(T target) {
        return create(target, Document.create());
    }

    /**
     * Create a patch that reads the identified target.
     * 
     * @param target the target identifier; may not be null
     * @return the patch; never null
     */
    public static <T extends Identifier> Patch<T> read(T target) {
        return edit(target).end();
    }

    /**
     * Create an editor for the target with the given identifier. The resulting editor will create the {@link Patch} when
     * {@link Editor#end()} is called.
     * 
     * @param id the identifier of the target
     * @return the editor that can create the patch; never null
     * @param <T> the type of identifier (or target) that should be edit
     */
    public static <T extends Identifier> Editor<Patch<T>> edit(T id) {
        return new PatchEditor<T>(id);
    }

    protected static final class PatchEditor<T extends Identifier> implements Editor<Patch<T>> {
        private T id;
        private List<Operation> ops = new LinkedList<>();

        protected PatchEditor(T id) {
            this(id, null);
        }

        protected PatchEditor(T id, Collection<Operation> initialOps) {
            this.id = id;
            if (initialOps != null) ops.addAll(initialOps);
        }

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
        public Optional<Patch<T>> endIfChanged(Predicate<Operation> significant) {
            if ( ops.isEmpty() ) return Optional.empty();
            if ( significant != null ) {
                if ( !ops.stream().anyMatch(significant) ) {
                    // This patch contains no significant operations, so there's nothing to do ...
                    return Optional.empty();
                }
            }
            return Optional.of(end());
        }

        @Override
        public String toString() {
            return "Patch editor for '" + id + "'";
        }
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
            return doc.set(Path.parse(this.path), true, value(), invalid).isPresent();
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
            if (path.isRoot()) {
                // The whole document is to be removed ...
                if (doc.isEmpty()) {
                    return Boolean.FALSE; // already empty, so not modified
                }
                doc.clear();
                return Boolean.TRUE;
            }
            AtomicBoolean result = new AtomicBoolean(false);
            // If the parent path does exist, remove the child from the parent ...
            doc.find(path.parent().get(), (missingPath, missingIndex) -> Optional.empty(), invalid).ifPresent((parent) -> {
                if (parent.isArray()) {
                    Path.Segments.asInteger(path.lastSegment()).ifPresent((index) -> {
                        parent.asArray().remove(index.intValue());
                        result.set(true);
                    });
                } else if (parent.isDocument()) {
                    result.set(parent.asDocument().remove(path.lastSegment()) != null);
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
            Optional<Value> result = doc.find(path, (missingPath, missingIndex) -> Optional.empty(), invalid);
            if (!result.isPresent() || !result.get().equals(requiredValue())) {
                invalid.accept(path);
            }
            return Boolean.FALSE; // this never modifies the document
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
            if (fromPath.equals(toPath)) {
                // Nothing to change ...
                return Boolean.FALSE;
            }

            // Silently look for the 'from' value ...
            Path fromPath = Path.parse(this.fromPath);
            Optional<Value> from = doc.find(fromPath, (missingPath, missingIndex) -> Optional.empty(), (invalidPath) -> {
            });
            if (from.isPresent()) {
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
            if (fromPath.equals(toPath)) {
                // Nothing to change ...
                return Boolean.FALSE;
            }

            // Silently look for the 'from' value ...
            Path fromPath = Path.parse(this.fromPath);
            Optional<Path> fromParentPath = fromPath.parent();
            if (fromParentPath.isPresent()) {
                Optional<Value> fromParent = doc.find(fromParentPath.get(),
                                                      (missingPath, missingIndex) -> Optional.empty(), (invalidPath) -> {
                                                      });
                if (fromParent.isPresent()) {
                    Value parent = fromParent.get();
                    if (parent.isDocument()) {
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
            Optional<Value> existing = doc.find(path, (missingPath, missingIndex) -> Optional.empty(), invalid);
            Value incremented = null;
            if (existing.isPresent()) {
                incremented = Value.create(MathOps.add(existing.get().asNumber(), value()));
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
        assert id != null;
        assert ops != null;
        this.id = id;
        this.ops = ops;
    }

    /**
     * Get the identifier of the target for this patch.
     * 
     * @return the target's identifier; never null
     */
    public IdType target() {
        return id;
    }

    @Override
    public Iterator<Operation> iterator() {
        return Iterators.readOnly(ops.iterator());
    }

    /**
     * Get the stream of operations in this patch.
     * 
     * @return the stream of operations; never null
     */
    public Stream<Operation> stream() {
        return ops.stream();
    }

    /**
     * Get the number of operations in this patch.
     * 
     * @return the number of operations; never negative
     */
    public int operationCount() {
        return ops.size();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof Patch) {
            Patch<?> that = (Patch<?>) obj;
            return this.target().equals(that.target()) && this.ops.equals(that.ops);
        }
        return false;
    }

    @Override
    public String toString() {
        return id.toString() + " : { " + ops.stream().map(Object::toString).collect(Collectors.joining(", ")) + " }";
    }

    /**
     * Determine if this patch has no operations.
     * 
     * @return {@code true} if this patch has no operations, or {@code false} otherwise
     */
    public boolean isEmpty() {
        return ops.isEmpty();
    }

    /**
     * Determine if this patch reads the target.
     * 
     * @return {@code true} if this patch reads the target, or {@code false} otherwise
     */
    public boolean isReadRequest() {
        return isEmpty();
    }

    /**
     * Determine if this patch only {@link Add adds} a document at the "/" path on the target.
     * 
     * @return {@code true} if this patch creates the target, or {@code false} otherwise
     */
    public boolean isCreation() {
        if (ops.size() != 1) return false;
        return ops.stream().anyMatch(Patch::isCreationOperation);
    }

    /**
     * Determine if this patch performs a deletion of the target, which is true if an only if the patch contains a single
     * {@link Remove remove operation} of the "/" path.
     * 
     * @return {@code true} if this patch deletes the target, or {@code false} otherwise
     */
    public boolean isDeletion() {
        if (ops.size() != 1) return false;
        return ops.stream().anyMatch(Patch::isDeletionOperation);
    }

    private static boolean isCreationOperation(Operation op) {
        if (op instanceof Add) {
            Add add = (Add) op;
            return add.path().equals("/") && add.value().isDocument();
        }
        return false;
    }

    private static boolean isDeletionOperation(Operation op) {
        return op instanceof Remove && ((Remove) op).path().equals("/");
    }

    /**
     * Consume the newly created document if and only if this patch is a {@link #isCreation() creation operation}.
     * 
     * @param consumer the function that should consume the document if this patch creates the target.
     */
    public void ifCreation(Consumer<Document> consumer) {
        if (isCreation()) {
            Add addOp = (Add) ops.get(0);
            consumer.accept(addOp.value().asDocument());
        }
    }

    /**
     * Obtain the {@link Document} representation of this patch, which includes the fields for the {@link #target()}'s identifier.
     * 
     * @return the document representation; never null
     */
    public Document asDocument() {
        Document doc = Document.create();
        doc.putAll(id.fields());
        doc.setArray("ops", Array.create(ops.stream().map(Operation::asValue).collect(Collectors.toList())));
        return doc;
    }

    /**
     * Obtain the {@link Array} representation of this patch, which does <em>not</em> include the fields for the {@link #target()}
     * 's identifier.
     * 
     * @return the document representation; never null
     */
    public Array asArray() {
        return Array.create(ops.stream().map(Operation::asValue).collect(Collectors.toList()));
    }

    /**
     * Reconstruct the patch in the {@link Field#OPS "ops"} field within the specified document, where the identifier
     * of the database target is given by the {@value Field#DATABASE_ID} field value.
     * 
     * @param doc the document; may not be null
     * @return the patch, or null if the document did not contain a patch
     */
    public static Patch<DatabaseId> forDatabase(Document doc) {
        return from(doc);
    }

    /**
     * Reconstruct the patch in the {@link Field#OPS "ops"} field within the specified document, where the identifier
     * of the target entity type is given by the {@value Field#DATABASE_ID} and {@value Field#COLLECTION} field values.
     * 
     * @param doc the document; may not be null
     * @return the patch, or null if the document did not contain a patch
     */
    public static Patch<EntityType> forEntityType(Document doc) {
        return from(doc);
    }

    /**
     * Reconstruct the patch in the {@link Field#OPS "ops"} field within the specified document, where the identifier
     * of the entity target is given by the {@value Field#DATABASE_ID}, {@value Field#COLLECTION}, {@value Field#ZONE_ID} and
     * {@value Field#ENTITY} field values.
     * 
     * @param doc the document; may not be null
     * @return the patch, or null if the document did not contain a patch
     */
    public static Patch<EntityId> forEntity(Document doc) {
        return from(doc, null);
    }

    /**
     * Reconstruct the patch in the {@link Field#OPS "ops"} field within the specified document, where the identifier
     * of the database, collection, zone or entity target is given by the {@value Field#DATABASE_ID}, {@value Field#COLLECTION},
     * {@value Field#ZONE_ID} and {@value Field#ENTITY} field values.
     * 
     * @param doc the document; may not be null
     * @return the patch, or null if the document did not contain a patch
     */
    public static <IdType extends Identifier> Patch<IdType> from(Document doc) {
        return from(doc, null);
    }

    /**
     * Reconstruct the patch in the {@link Field#OPS "ops"} field within the specified document, and optionally the identifier
     * of the database, collection, zone or entity target given by the {@value Field#DATABASE_ID}, {@value Field#COLLECTION},
     * {@value Field#ZONE_ID} and {@value Field#ENTITY} field values.
     * 
     * @param doc the document; may not be null
     * @param databaseId the database identifier; may be null if the document contains the {@link Field#DATABASE_ID "databaseId"}
     *            field
     * @return the patch, or null if the document did not contain a patch
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <IdType extends Identifier> Patch<IdType> from(Document doc, DatabaseId databaseId) {
        Identifier id = Message.getId(doc, databaseId);
        if (id == null) return null;
        Array ops = doc.getArray(Field.OPS);
        if (ops == null) return null;
        List<Operation> operations = ops.streamValues().filter(Value::isDocument)
                                        .map(Patch::toOperation)
                                        .filter(Predicates.notNull())
                                        .collect(Collectors.toList());
        return new Patch(id, operations);
    }

    /**
     * Reconstruct the patch from the operations and a given identifier.
     * 
     * @param id the identifier of the patch's target; may not be null
     * @param ops the array containing the operations in the patch; may not be null
     * @return the patch, or null if the document did not contain a patch
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <IdType extends Identifier> Patch<IdType> from(Identifier id, Array ops) {
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
     * 
     * @param document the document that should be patched; never null
     * @param failed the function that is called for each failed operation because the path did not exist; may be null
     * @return true if any of the operations modified the document, or false if the document is unchanged
     */
    public boolean apply(Document document, BiConsumer<Path,Operation> failed) {
        if (isEmpty()) return false;
        if (isCreation()) {
            Add add = (Add) ops.stream().findFirst().get();
            document.putAll(add.value().asDocument());
            Message.addId(document, id);
            return true;
        }
        if (isDeletion()) {
            return false;
        }
        return ops.stream().map(op -> op.apply(document, invalidPath -> {
            if ( failed != null ) failed.accept(invalidPath,op);
        })).count() > 0;
    }

    /**
     * Obtain an {@link Editor} for this patch, which always leaves this patch unmodified and instead always results in a
     * new updated patch object.
     * 
     * @return the patch editor; never null
     */
    public Editor<Patch<IdType>> edit() {
        return new PatchEditor<IdType>(this.id, this.ops);
    }
    
    /**
     * Return a copy of this patch except with only those operations that satisfy the supplied filter.
     * @param operationFilter the filter on operations; may not be null
     * @return a copy of this patch with filtered operations, or this patch if all operations satisfied the filter; never null
     */
    public Patch<IdType> filter( Predicate<Operation> operationFilter ) {
        List<Operation> filtered = this.ops.stream().filter(operationFilter).collect(Collectors.toList());
        if ( filtered.size() == this.ops.size() ) return this;
        return new Patch<IdType>(this.id,filtered);
    }
    
    /**
     * Return a new patch that combines this patch's operations with those from the supplied patch. Any superfluous operations
     * may be removed.
     * @param otherPatch the patch to add to this patch's operations
     * @return the new patch; never null, but possibly this patch if the other patch is null or empty
     */
    public Patch<IdType> append( Patch<IdType> otherPatch ) {
        if ( otherPatch == null ) return this;
        if ( !Objects.equals(this.target(),otherPatch.target())) {
            throw new IllegalArgumentException("Unable to merge patches with different targets");
        }
        if ( otherPatch.isEmpty() ) return this;
        List<Operation> ops = new LinkedList<>(this.ops);
        ops.addAll(otherPatch.ops);
        return new Patch<>(this.id,ops);
    }
}

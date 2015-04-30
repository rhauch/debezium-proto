/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.samza;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.TaskCoordinator.RequestScope;
import org.apache.samza.task.WindowableTask;
import org.debezium.Testing;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.DocumentReader;
import org.debezium.core.doc.Path;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Message;
import org.debezium.core.util.Collect;
import org.fest.assertions.Fail;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public abstract class AbstractServiceTest implements Testing {
    
    private String systemName = "testStreams";
    private String offset = "0";
    
    public String random() {
        return UUID.randomUUID().toString();
    }
    
    protected Document readMessage(String streamName, String filename) throws IOException {
        String json = Testing.Files.readResourceAsString(streamName + "/" + filename);
        Document message = DocumentReader.defaultReader().read(json);
        return message;
    }
    
    protected OutputMessages processMessage(StreamTask service, Document message) {
        Testing.debug("processing message:");
        Testing.debug(message);
        return process(service, random(), message);
    }

    protected OutputMessages process(StreamTask service, Object key, Object message) {
        OutputMessages output = new OutputMessages();
        try {
            service.process(input(key, message), output, coordinator());
        } catch (Throwable t) {
            Fail.fail("Error invoking 'process' on service", t);
        }
        return output;
    }

    protected OutputMessages window(WindowableTask service) {
        OutputMessages output = new OutputMessages();
        try {
            service.window(output, coordinator());
        } catch (Throwable t) {
            Fail.fail("Error invoking 'window' on service", t);
        }
        return output;
    }

    protected IncomingMessageEnvelope input(Object key, Object message) {
        return input("inputTopic", key, message);
    }
    
    protected IncomingMessageEnvelope input(String topic, Object key, Object message) {
        SystemStreamPartition stream = new SystemStreamPartition(systemName, topic, new Partition(1));
        return new IncomingMessageEnvelope(stream, offset, key, message);
    }
    
    protected MessageCollector collect(Consumer<OutgoingMessageEnvelope> consumer) {
        return new MessageCollector() {
            @Override
            public void send(OutgoingMessageEnvelope envelope) {
                consumer.accept(envelope);
            }
        };
    }
    
    protected TaskCoordinator coordinator() {
        return coordinator(null, null);
    }
    
    protected TaskCoordinator coordinator(Consumer<RequestScope> commitFunction) {
        return coordinator(commitFunction, null);
    }
    
    protected TaskCoordinator coordinator(Consumer<RequestScope> commitFunction, Consumer<RequestScope> shutdownFunction) {
        return new TaskCoordinator() {
            @Override
            public void commit(RequestScope scope) {
                if (commitFunction != null) commitFunction.accept(scope);
            }
            
            @Override
            public void shutdown(RequestScope scope) {
                if (shutdownFunction != null) shutdownFunction.accept(scope);
            }
        };
    }
    
    protected static class OutputMessages implements MessageCollector, Iterable<OutgoingMessageEnvelope> {
        private final LinkedList<OutgoingMessageEnvelope> received = new LinkedList<>();
        
        @Override
        public void send(OutgoingMessageEnvelope envelope) {
            if (envelope != null) received.add(envelope);
        }
        
        public boolean isEmpty() {
            return received.isEmpty();
        }
        
        public int count() {
            return received.size();
        }
        
        @Override
        public Iterator<OutgoingMessageEnvelope> iterator() {
            return received.iterator();
        }
        
        public OutgoingMessageEnvelope removeFirst() {
            return received.removeFirst();
        }
    }
    
    public static interface OutputMessageValidator {
        OutputMessageValidator hasSystem(String systemName);
        
        OutputMessageValidator hasStream(String streamName);
        
        OutputMessageValidator hasMessage(Object expectedMessage);
        
        OutputMessageValidator hasMessage(Document expectedMessage);
        
        default OutputMessageValidator isAggregateOf(Document firstPartial, Document... additionalPartials) {
            return isAggregateOf(Collect.arrayListOf(firstPartial, additionalPartials));
        }
        
        default OutputMessageValidator isAggregateOf(Iterable<Document> partials ) {
            return isAggregateOf(Collect.arrayListOf(partials));
        }
        
        OutputMessageValidator isAggregateOf(List<Document> partials );
        
        DocumentValidator<OutputMessageValidator> hasMessage();
        
        OutputMessageValidator hasPartitionKey(Object partitionKey);
        
        default OutputMessageValidator hasPartitionKey(Identifier partitionKey) {
            return hasPartitionKey(partitionKey.asString());
        }
        
        OutputMessageValidator hasKey(Object key);
        
        default OutputMessageValidator hasKey(Identifier key) {
            return hasKey(key.asString());
        }

        default OutputMessageValidator isPart(int part, int totalParts) {
            return hasMessage().isPart(part, totalParts).back();
        }
    }
    
    public static interface DocumentValidator<T> {
        default DocumentValidator<T> with(String path, Document expectedValue) {
            return with(path, Value.create(expectedValue));
        }
        
        default DocumentValidator<T> with(String path, Array expectedValue) {
            return with(path, Value.create(expectedValue));
        }
        
        default DocumentValidator<T> with(String path, String expectedValue) {
            return with(path, Value.create(expectedValue));
        }
        
        DocumentValidator<T> isPart(int part, int totalParts);
        
        DocumentValidator<T> with(String path, Value expectedValue);
        
        T back();
    }
    
    protected OutputMessageValidator assertNextMessage(OutputMessages messages) {
        OutgoingMessageEnvelope env = messages.removeFirst();
        Testing.print(env.getMessage());
        return new OutputMessageValidator() {
            @Override
            public OutputMessageValidator hasKey(Object key) {
                assertThat(env.getKey()).isEqualTo(key);
                return this;
            }
            
            @Override
            public OutputMessageValidator hasPartitionKey(Object partitionKey) {
                assertThat(env.getPartitionKey()).isEqualTo(partitionKey);
                return this;
            }
            
            @Override
            public OutputMessageValidator hasStream(String streamName) {
                assertThat(env.getSystemStream().getStream()).isEqualTo(streamName);
                return this;
            }
            
            @Override
            public OutputMessageValidator hasSystem(String systemName) {
                assertThat(env.getSystemStream().getSystem()).isEqualTo(systemName);
                return this;
            }
            
            @Override
            public OutputMessageValidator hasMessage(Object expectedMessage) {
                assertThat(env.getMessage()).isEqualTo(expectedMessage);
                return this;
            }
            
            @Override
            public OutputMessageValidator hasMessage(Document expectedMessage) {
                Document actual = (Document)env.getMessage();
                Document actualWithoutParts = actual.clone();
                boolean removeParts = false;
                if ( actualWithoutParts.has(Message.Field.PARTS) && !expectedMessage.has(Message.Field.PARTS)) {
                    actualWithoutParts.remove(Message.Field.PARTS);
                    expectedMessage.remove(Message.Field.PARTS);
                    removeParts = true;
                }
                if ( removeParts || (actualWithoutParts.has(Message.Field.PART) && !expectedMessage.has(Message.Field.PART))) {
                    actualWithoutParts.remove(Message.Field.PART);
                    expectedMessage.remove(Message.Field.PART);
                }
                if ( actualWithoutParts.has(Message.Field.ENDED) && !expectedMessage.has(Message.Field.ENDED) ) {
                    actualWithoutParts.remove(Message.Field.ENDED);
                }
                assertThat((Object)actualWithoutParts).isEqualTo(expectedMessage);
                return this;
            }
            
            @Override
            public DocumentValidator<OutputMessageValidator> hasMessage() {
                OutputMessageValidator result = this;
                Document msg = (Document) env.getMessage();
                return new DocumentValidator<OutputMessageValidator>() {
                    @Override
                    public OutputMessageValidator back() {
                        return result;
                    }
                    
                    @Override
                    public DocumentValidator<OutputMessageValidator> with(String path, Value expectedValue) {
                        Optional<Value> result = msg.find(Path.parse(path));
                        assertThat(result.isPresent()).isTrue();
                        assertThat(result.get()).isEqualTo(expectedValue);
                        return this;
                    }
                    
                    @Override
                    public DocumentValidator<OutputMessageValidator> isPart(int part, int totalParts) {
                        assertThat(Message.getPart(msg)).isEqualTo(part);
                        assertThat(Message.getParts(msg)).isEqualTo(totalParts);
                        return this;
                    }
                };
            }
            
            @Override
            public OutputMessageValidator isAggregateOf(List<Document> partials) {
                Document aggregate = (Document) env.getMessage();
                Testing.debug("aggreagate: " + aggregate);
                Message.forEachPartialResponse(aggregate, (id, part, request, response) -> {
                    Document expected = partials.get(part - 1);
                    assertThat(response.equals(expected)).isTrue();
                });
                return this;
            }
        };
    }
    
    protected void assertNoMoreMessages(OutputMessages messages) {
        assertThat(messages.isEmpty()).isEqualTo(true);
    }
    
//    protected void assertNextMessage(OutputMessages messages, int part, int totalParts, Identifier key, Document message) {
//        message.setNumber(Message.Field.PART, part);
//        message.setNumber(Message.Field.PARTS, totalParts);
//        assertNextMessage(messages, null, key.asString(), message);
//    }
//
//    protected void assertNextMessage(OutputMessages messages, Object partitionKey, Object key, Object message) {
//        OutgoingMessageEnvelope env = messages.removeFirst();
//        if (partitionKey != null) assertThat(env.getPartitionKey()).isEqualTo(partitionKey);
//        assertThat(env.getKey()).isEqualTo(key);
//        assertThat(env.getMessage()).isEqualTo(message);
//    }
//
//    protected void assertNextMessage(OutputMessages messages, Object partitionKey, Object key, Consumer<Document> messageValidator) {
//        OutgoingMessageEnvelope env = messages.removeFirst();
//        if (partitionKey != null) assertThat(env.getPartitionKey()).isEqualTo(partitionKey);
//        assertThat(env.getKey()).isEqualTo(key);
//        messageValidator.accept((Document) env.getMessage());
//    }
//
//    protected void assertNextAggregateMessage(OutputMessages messages, Object partitionKey, Object key, Document firstMessage,
//                                              Document... additionalMessages) {
//        List<Document> partials = Collect.arrayListOf(firstMessage, additionalMessages);
//        OutgoingMessageEnvelope env = messages.removeFirst();
//        if (partitionKey != null) assertThat(env.getPartitionKey()).isEqualTo(partitionKey);
//        assertThat(env.getKey()).isEqualTo(key);
//        Document aggregate = (Document) env.getMessage();
//        Testing.debug("aggreagate: " + aggregate);
//        Message.forEachPartialResponse(aggregate, (id, part, request, response) -> {
//            Document expected = partials.get(part - 1);
//            assertThat(response.equals(expected)).isTrue();
//        });
//    }
//
//    protected void assertNextAggregateMessage(OutputMessages messages, Object partitionKey, Object key, Iterable<Document> msgs) {
//        List<Document> partials = Collect.arrayListOf(msgs);
//        OutgoingMessageEnvelope env = messages.removeFirst();
//        if (partitionKey != null) assertThat(env.getPartitionKey()).isEqualTo(partitionKey);
//        assertThat(env.getKey()).isEqualTo(key);
//        Document aggregate = (Document) env.getMessage();
//        Testing.debug("aggreagate: " + aggregate);
//        Message.forEachPartialResponse(aggregate, (id, part, request, response) -> {
//            Document expected = partials.get(part - 1);
//            assertThat(response.equals(expected)).isTrue();
//        });
//    }
    
    protected static MapConfig testConfig() {
        return new MapConfig();
    }
    
    protected static MapConfig testConfig(Map<String, String> config) {
        return new MapConfig(config);
    }
    
    protected static TaskContext testContext() {
        return new TaskContext() {
            
            @Override
            public Object getStore(String name) {
                return new MemoryKeyValueStore<Object, Object>(name);
            }
            
            @Override
            public Set<SystemStreamPartition> getSystemStreamPartitions() {
                return null;
            }
            
            @Override
            public org.apache.samza.container.TaskName getTaskName() {
                return null;
            }
            
            @Override
            public MetricsRegistry getMetricsRegistry() {
                return null;
            }
        };
    }
    
}

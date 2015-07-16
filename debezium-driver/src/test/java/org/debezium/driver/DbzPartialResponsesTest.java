/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.debezium.core.message.Topic;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import static org.junit.Assert.fail;

/**
 * @author Randall Hauch
 *
 */
public class DbzPartialResponsesTest extends AbstractDbzNodeTest {

    private DbzPartialResponses responses;

    @Override
    protected void addServices(DbzNode node) {
        createTopics(Topic.PARTIAL_RESPONSES);
        responses = new DbzPartialResponses();
        node.add(responses);
    }

    @Test
    public void shouldImmediatelyPropagateExceptionThrownFromSubmitBlock() {
        try {
            responses.submit(Boolean.class, requestId -> {
                throw new DebeziumAuthorizationException();
            }).onResponse(10, TimeUnit.SECONDS, response -> {
                fail("Should not have gotten a response");
                return Boolean.FALSE;
            }).onTimeout(() -> {
                fail("Should not have timed out, either");
                return Boolean.FALSE;
            });
            fail("Should not have completed the whole operation");
        } catch (DebeziumAuthorizationException e) {
            // expected
        }
    }

    @Test
    public void shouldImmediatelyPropagateExceptionThrownFromOnResponseBlock() {
        try {
            AtomicReference<Document> requestDoc = new AtomicReference<>();
            responses.submit(Boolean.class, requestId -> {
                Document request = Document.create("key", "key1");
                Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), "jsmith");
                node.send(Topic.PARTIAL_RESPONSES, "key1", request);
                requestDoc.set(request);
            }).onResponse(10, TimeUnit.SECONDS, response -> {
                RequestId id = RequestId.from(response);
                assertThat(response.equals(requestDoc.get())).isTrue();
                assertThat(id).isNotNull();
                throw new DebeziumAuthorizationException();
            }).onTimeout(() -> {
                fail("Should not have timed out, either");
                return Boolean.FALSE;
            });
            fail("Should not have completed the whole operation");
        } catch (DebeziumAuthorizationException e) {
            // expected
        }
    }

    @Test
    public void shouldImmediatelyPropagateExceptionThrownFromOnTimeoutBlock() {
        try {
            // Submit nothing, so the 'onResponse' function is never called ...
            responses.submit(Boolean.class, requestId -> {
            }).onResponse(5, TimeUnit.MILLISECONDS, response -> {
                return true;
            }).onTimeout(() -> {
                throw new DebeziumAuthorizationException();
            });
            fail("Should not have completed the whole operation");
        } catch (DebeziumAuthorizationException e) {
            // expected
        }
    }

    @Test
    public void shouldSubmitRequestWithOnePartDirectlyToPartialResponsesStream() {
        AtomicReference<Document> requestDoc = new AtomicReference<>();
        boolean result = responses.submit(Boolean.class, requestId -> {
            Document request = Document.create("key", "key1");
            Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), "jsmith");
            node.send(Topic.PARTIAL_RESPONSES, "key1", request);
            requestDoc.set(request);
        }).onResponse(10, TimeUnit.SECONDS, response -> {
            RequestId id = RequestId.from(response);
            assertThat(response.equals(requestDoc.get())).isTrue();
            assertThat(id).isNotNull();
            return Boolean.TRUE;
        }).onTimeout(() -> {
            return Boolean.FALSE;
        });
        assertThat(result).isTrue();
    }

    @Test
    public void shouldSubmitRequestWithMultiplePartsDirectlyToPartialResponsesStream() {
        Map<Integer, Document> responseByPart = new HashMap<>();
        responses.submit(3, requestId -> {
            Document request = Document.create("key", "key1");
            Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), "jsmith");
            Message.setParts(request, 2, 3);
            request.set("key", "key2");
            node.send(Topic.PARTIAL_RESPONSES, "key2", request.clone());
            Message.setParts(request, 3, 3);
            request.set("key", "key3");
            node.send(Topic.PARTIAL_RESPONSES, "key3", request.clone());
            Message.setParts(request, 1, 3);
            request.set("key", "key1");
            node.send(Topic.PARTIAL_RESPONSES, "key1", request.clone());
        }).onEachResponse(10, TimeUnit.SECONDS, response -> {
            int part = Message.getPart(response);
            responseByPart.put(part, response);
        }).onTimeout(() -> {
            fail("Should not have timed out");
            return null;
        });
        assertThat(responseByPart.get(1).getString("key")).isEqualTo("key1");
        assertThat(responseByPart.get(2).getString("key")).isEqualTo("key2");
        assertThat(responseByPart.get(3).getString("key")).isEqualTo("key3");
    }

    @Test
    public void shouldImmediatelyReturnWhenSubmittingRequestWithNoParts() {
        // <--- Note the '0' as first argument means 'onEachResponse' will not be called and it will just return!
        responses.submit(0, requestId -> {
            Document request = Document.create("key", "key1");
            Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), "jsmith");
            Message.setParts(request, 1, 1);
            node.send(Topic.PARTIAL_RESPONSES, "key1", request);
        }).onEachResponse(10, TimeUnit.SECONDS, response -> {
            fail("Should not have found a response");
        }).onTimeout(() -> {
            fail("Should not have timed out");
            return null;
        });
    }

}

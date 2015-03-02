/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.util.ArrayList;
import java.util.List;

import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.debezium.core.message.Topic;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class ResponseAccumulatorServiceTest extends AbstractServiceTest {
    
    private static final String CLIENT_ID = "some-unique-client";
    private static final String RESPONSE_ID = "some-response";
    private static final String USER = "jane.smith";
    private static final long REQUEST_ID = 1234L;
    private static final EntityId ID1 = Identifier.of("db", "collection", "ent1");
    private static final EntityId ID2 = Identifier.of("db", "collection", "ent2");
    
    private ResponseAccumulatorService service;
    
    @Before
    public void beforeEach() {
        service = new ResponseAccumulatorService();
        service.init(testConfig(), testContext());
    }
    
    @Test
    public void shouldForwardResponseWithSinglePart() {
        Document msg = Document.create();
        Message.addId(msg, ID1);
        Message.addHeaders(msg, CLIENT_ID, REQUEST_ID, USER);
        Message.setParts(msg, 1, 1);
        OutputMessages output = process(service, RESPONSE_ID, msg);
        assertNextMessage(output).hasStream(Topic.COMPLETE_RESPONSES)
                                 .hasPartitionKey(CLIENT_ID)
                                 .hasKey(RESPONSE_ID)
                                 .hasMessage(msg);
        assertNoMoreMessages(output);
    }
    
    @Test
    public void shouldStoreFirstResponseForMultipleParts() {
        // Fire the first of 2 messages, so nothing will be output ...
        Document msg1 = Document.create();
        Message.addId(msg1, ID1);
        Message.addHeaders(msg1, CLIENT_ID, REQUEST_ID, USER);
        Message.setParts(msg1, 1, 2);
        OutputMessages output = process(service, RESPONSE_ID, msg1);
        assertNoMoreMessages(output);
        
        // Fire the second of 2 messages, so now both will be output ...
        Document msg2 = Document.create();
        Message.addId(msg2, ID2);
        Message.addHeaders(msg2, CLIENT_ID, REQUEST_ID, USER);
        Message.setParts(msg2, 2, 2);
        output = process(service, RESPONSE_ID, msg2);
        assertNextMessage(output).hasStream(Topic.COMPLETE_RESPONSES)
                                 .hasPartitionKey(CLIENT_ID)
                                 .hasKey(RESPONSE_ID)
                                 .isAggregateOf(msg1, msg2);
        assertNoMoreMessages(output);
    }
    
    @Test
    public void shouldStoreFirstResponseForMultiplePartsNotInNaturalOrder() {
        // Testing.Debug.enable();
        
        // Create the two messages ...
        Document msg1 = Document.create();
        Message.addId(msg1, ID2);
        Message.addHeaders(msg1, CLIENT_ID, REQUEST_ID, USER);
        Message.setParts(msg1, 2, 2);
        
        Document msg2 = Document.create();
        Message.addId(msg2, ID1);
        Message.addHeaders(msg2, CLIENT_ID, REQUEST_ID, USER);
        Message.setParts(msg2, 1, 2);
        
        // Fire the 2 messages individually ...
        OutputMessages output1 = process(service, RESPONSE_ID, msg1);
        OutputMessages output2 = process(service, RESPONSE_ID, msg2);
        
        // Check the output of the first call ...
        assertNoMoreMessages(output1);
        
        // Check the output of the second call ...
        assertNextMessage(output2).hasStream(Topic.COMPLETE_RESPONSES)
                                  .hasPartitionKey(CLIENT_ID)
                                  .hasKey(RESPONSE_ID)
                                  .isAggregateOf(msg2, msg1);
        assertNoMoreMessages(output2);
    }
    
    @Test
    public void shouldStoreAndForwardResponsesForManyParts() {
        // Testing.Debug.enable();
        int numParts = 10;
        
        // Create the two messages ...
        List<Document> msgs = new ArrayList<>();
        for (int i = 0; i != numParts; ++i) {
            EntityId id = Identifier.of("db", "collection", "ent" + (i + 1));
            Document msg = Document.create();
            Message.addId(msg, id);
            Message.addHeaders(msg, CLIENT_ID, REQUEST_ID, USER);
            Message.setParts(msg, i + 1, numParts);
            msgs.add(msg);
        }
        
        // Fire the 2 messages individually ...
        List<OutputMessages> outputs = new ArrayList<>();
        msgs.forEach((msg) -> {
            OutputMessages output = process(service, RESPONSE_ID, msg);
            outputs.add(output);
        });
        
        // Check the output of all but the last call ...
        int indexOfLastPart = numParts - 1;
        for (int i = 0; i != indexOfLastPart; ++i) {
            assertNoMoreMessages(outputs.get(i));
        }
        
        // Check the output of the last call ...
        OutputMessages output = outputs.get(indexOfLastPart);
        assertNextMessage(output).hasStream(Topic.COMPLETE_RESPONSES)
                                 .hasPartitionKey(CLIENT_ID)
                                 .hasKey(RESPONSE_ID)
                                 .isAggregateOf(msgs);
        assertNoMoreMessages(output);
    }
    
}

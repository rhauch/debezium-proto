/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;

import org.debezium.Testing;
import org.junit.Test;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class DocumentSerdesTest implements Testing {

    private static final DocumentSerdes SERDES = new DocumentSerdes();

    @Test
    public void shouldConvertFromBytesToDocument1() throws IOException {
        readAsStringAndBytes("json/sample1.json");
    }

    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocument2() throws IOException {
        readAsStringAndBytes("json/sample2.json");
    }

    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocument3() throws IOException {
        readAsStringAndBytes("json/sample3.json");
    }

    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocumentForResponse1() throws IOException {
        readAsStringAndBytes("json/response1.json");
    }

    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocumentForResponse2() throws IOException {
        readAsStringAndBytes("json/response2.json");
    }

    @Test
    public void shouldUseSerdeToConvertMultipleContacts() throws IOException {
        // Testing.Print.enable();
        List<Document> docs = readResources("schema-learning/",
                                            "contacts-step1.json", "contacts-step2.json", "contacts-step3.json",
                                            "contacts-step4.json", "contacts-step5.json",
                                            "complex-contacts-step1.json", "complex-contacts-step2.json",
                                            "complex-contacts-step3.json", "complex-contacts-step4.json",
                                            "complex-contacts-step5.json");
        docs.addAll(readResources("entity-updates/", "large.json", "medium.json", "single.json", "small.json"));
        MetricRegistry registry = new MetricRegistry();
        Histogram hist = registry.histogram("messageSize");
        docs.forEach(doc -> roundTrip(doc, hist::update));
        print(hist.getSnapshot());
    }

    protected void print(Snapshot snapshot) {
        Testing.print("Count:      " + snapshot.size());
        Testing.print("Minimum:    " + snapshot.getMin());
        Testing.print("Maximum:    " + snapshot.getMax());
        Testing.print("Mean:       " + snapshot.getMean());
        Testing.print("Median:     " + snapshot.getMedian());
        Testing.print("75p:        " + snapshot.get75thPercentile());
        Testing.print("95p:        " + snapshot.get95thPercentile());
        Testing.print("98p:        " + snapshot.get98thPercentile());
        Testing.print("99p:        " + snapshot.get99thPercentile());
        Testing.print("999p:       " + snapshot.get999thPercentile());
    }

    protected void readAsStringAndBytes(String resourceFile) throws IOException {
        String content = Testing.Files.readResourceAsString(resourceFile);
        Document doc = DocumentReader.defaultReader().read(content);
        roundTrip(doc, size -> System.out.println("message size " + size + " bytes: \n" + doc));
    }

    protected void roundTrip(Document doc, IntConsumer sizeAccumulator) {
        byte[] bytes = SERDES.serialize("topicA", doc);
        if (sizeAccumulator != null) sizeAccumulator.accept(bytes.length);
        Document reconstituted = SERDES.deserialize("topicA", bytes);
        assertThat((Object) reconstituted).isEqualTo(doc);
    }

    protected List<Document> readResources(String prefix, String... resources) throws IOException {
        List<Document> documents = new ArrayList<>();
        for (String resource : resources) {
            String content = Testing.Files.readResourceAsString(prefix + resource);
            Array array = null;
            try {
                Document doc = DocumentReader.defaultReader().read(content);
                array = doc.getArray("entityChanges");
            } catch (IOException e) {
                array = ArrayReader.defaultReader().readArray(content);
            }
            array.forEach(entry -> documents.add(entry.getValue().asDocument()));
        }
        return documents;
    }

}

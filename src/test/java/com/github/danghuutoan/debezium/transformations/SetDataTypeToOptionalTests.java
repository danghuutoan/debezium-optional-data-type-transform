/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.danghuutoan.debezium.transformations;

import static org.fest.assertions.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;

import java.text.ParseException;
import java.text.SimpleDateFormat;  
/**
 * @author Jiri Pechanec
 */
public class SetDataTypeToOptionalTests {

    private static final String HANDLE_DELETES = "delete.handling.mode";
    private static final String ADD_HEADERS = "add.headers";
    private static final String TIMESTAMP_TEST_FIELD_NAME = "timeStampField";
    private static final String DATE_TEST_FIELD_NAME = "dateField";   

    final Schema recordSchema = SchemaBuilder.struct()
            .field("id", Schema.INT8_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field(TIMESTAMP_TEST_FIELD_NAME, Timestamp.builder())
            .field(DATE_TEST_FIELD_NAME, Date.builder())
            .build();

    final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
            .build();

    final Envelope envelope = Envelope.defineSchema()
            .withName("dummy.Envelope")
            .withRecord(recordSchema)
            .withSource(sourceSchema)
            .build();

    // private SourceRecord createDeleteRecord() {
    //     final Schema deleteSourceSchema = SchemaBuilder.struct()
    //             .field("lsn", SchemaBuilder.int32())
    //             .field("version", SchemaBuilder.string())
    //             .build();

    //     Envelope deleteEnvelope = Envelope.defineSchema()
    //             .withName("dummy.Envelope")
    //             .withRecord(recordSchema)
    //             .withSource(deleteSourceSchema)
    //             .build();

    //     final Struct before = new Struct(recordSchema);
    //     final Struct source = new Struct(deleteSourceSchema);

    //     before.put("id", (byte) 1);
    //     before.put("name", "myRecord");
    //     source.put("lsn", 1234);
    //     source.put("version", "version!");
    //     final Struct payload = deleteEnvelope.delete(before, source, Instant.now());
    //     return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    // }

    private SourceRecord createCreateRecord() throws ParseException {
        final Struct after = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);
        after.put("id", (byte) 1);
        after.put("name", "myRecord");
        after.put(TIMESTAMP_TEST_FIELD_NAME, new SimpleDateFormat("dd/MM/yyyy").parse("20/02/1992"));
        after.put(DATE_TEST_FIELD_NAME, new SimpleDateFormat("dd/MM/yyyy").parse("20/02/1992"));
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
        final Struct payload = envelope.create(after, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    // private SourceRecord createUpdateRecord() {
    //     final Struct before = new Struct(recordSchema);
    //     final Struct after = new Struct(recordSchema);
    //     final Struct source = new Struct(sourceSchema);
    //     final Struct transaction = new Struct(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);

    //     before.put("id", (byte) 1);
    //     before.put("name", "myRecord");
    //     after.put("id", (byte) 1);
    //     after.put("name", "updatedRecord");
    //     source.put("lsn", 1234);
    //     transaction.put("id", "571");
    //     transaction.put("total_order", 42L);
    //     transaction.put("data_collection_order", 42L);
    //     final Struct payload = envelope.update(before, after, source, Instant.now());
    //     payload.put("transaction", transaction);
    //     return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    // }

    @Test
    public void testShouldSupportTimestampType() throws ParseException {
        try (final SetDataTypeToOptional<SourceRecord> transform = new SetDataTypeToOptional<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            props.put(ADD_HEADERS, "op");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            Field afterField = unwrapped.valueSchema().field("after");
            assertThat(afterField.schema().field(TIMESTAMP_TEST_FIELD_NAME).schema().isOptional()).isEqualTo(true);
            assertThat(afterField.schema().field(TIMESTAMP_TEST_FIELD_NAME).schema().defaultValue()).isEqualTo(null);
        }
    }

    @Test
    public void testShouldSupportDateType() throws ParseException {
        try (final SetDataTypeToOptional<SourceRecord> transform = new SetDataTypeToOptional<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES, "rewrite");
            props.put(ADD_HEADERS, "op");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            Field afterField = unwrapped.valueSchema().field("after");
            assertThat(afterField.schema().field(DATE_TEST_FIELD_NAME).schema().isOptional()).isEqualTo(true);
            assertThat(afterField.schema().field(DATE_TEST_FIELD_NAME).schema().defaultValue()).isEqualTo(null);
        }
    }
}

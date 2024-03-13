package com.scylladb.cdc.debezium.connector.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ScyllaExtractFlattenedNewRecordStateTest {

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        xform.configure(Collections.emptyMap());
    }

    private ScyllaExtractFlattenedNewRecordState<SinkRecord> xform = new ScyllaExtractFlattenedNewRecordState<>();

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void testJavaSet() {

        final Schema keys = SchemaBuilder.struct().field("serviceid", Schema.STRING_SCHEMA)
                .field("servicename", Schema.STRING_SCHEMA).build();

        final Struct keysV = new Struct(keys);
        keysV.put("serviceid","serviceid_01");
        keysV.put("servicename","Payments");

        final Struct keysV1 = new Struct(keys);
        keysV1.put("serviceid","serviceid_02");
        keysV1.put("servicename","Webhook");
        Map params = new HashMap();
        params.put(keysV,true);
        params.put(keysV1,true);
        final Schema elements = SchemaBuilder.map(keys,Schema.BOOLEAN_SCHEMA)
                .build();

        final Schema serviceSchema = SchemaBuilder.struct().name("services")
                    .field("mode", Schema.STRING_SCHEMA)
                    .field("elements", elements)
                    .build();
        final Struct serviceV = new Struct(serviceSchema);
        serviceV.put("mode","OVERWRITE");
        serviceV.put("elements", params);

        Schema schema = SchemaBuilder.struct()
                .field("organization.Value", serviceSchema)
                .name("schema").build();
            final Struct value = new Struct(schema);
            value.put("organization.Value",serviceV);


            final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
            final SinkRecord transformedRecord = xform.apply(record);

            final Struct updatedValue = (Struct) transformedRecord.value();
            assertEquals(1, updatedValue.schema().fields().size());
            assertEquals(2, updatedValue.getArray("organization.Value").size());
//        List<Struct> services = updatedValue.getArray("organization.Value");
//        System.out.println(services.toString());
//        System.out.println(services.get(0).get("servicename"));
       // assertEquals(2, );
           // assertEquals(0, updatedValue.getArray("arr2").size());

    }
}
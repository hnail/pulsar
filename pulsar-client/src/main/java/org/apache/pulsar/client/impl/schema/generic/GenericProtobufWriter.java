package org.apache.pulsar.client.impl.schema.generic;

import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaWriter;

public class GenericProtobufWriter implements SchemaWriter<GenericRecord> {
    @Override
    public byte[] write(GenericRecord message) {
        return ((GenericProtobufRecord)message).getProtobufRecord().toByteArray();
    }
}

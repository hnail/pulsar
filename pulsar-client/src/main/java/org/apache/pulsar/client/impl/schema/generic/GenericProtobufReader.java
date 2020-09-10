package org.apache.pulsar.client.impl.schema.generic;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

public class GenericProtobufReader implements SchemaReader<GenericRecord> {

    private static final Logger log = LoggerFactory.getLogger(GenericProtobufReader.class);

    private Descriptors.Descriptor descriptor;

    private byte[] schemaVersion;
    private List<Field> fields;


    public GenericProtobufReader(Descriptors.Descriptor descriptor) {
        this(descriptor, null);
    }


    public GenericProtobufReader(Descriptors.Descriptor descriptor, byte[] schemaVersion) {
        try {
            this.schemaVersion = schemaVersion;
            this.descriptor = descriptor;
            this.fields = descriptor.getFields()
                    .stream()
                    .map(f -> new Field(f.getName(), f.getIndex()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("GenericProtobufReader init error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public GenericProtobufRecord read(byte[] bytes, int offset, int length) {
        try {
            return new GenericProtobufRecord(schemaVersion, descriptor, fields, DynamicMessage.parseFrom(descriptor, bytes));
        } catch (InvalidProtocolBufferException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public GenericProtobufRecord read(InputStream inputStream) {
        try {
            return new GenericProtobufRecord(schemaVersion, descriptor, fields, DynamicMessage.parseFrom(descriptor, inputStream));
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }

}

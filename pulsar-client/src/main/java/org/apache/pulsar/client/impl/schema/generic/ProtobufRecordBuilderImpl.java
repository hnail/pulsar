package org.apache.pulsar.client.impl.schema.generic;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;

public class ProtobufRecordBuilderImpl implements GenericRecordBuilder {

    private GenericProtobufSchema genericSchema;
    private DynamicMessage.Builder builder;

    private Descriptors.Descriptor msgDesc;


    public ProtobufRecordBuilderImpl(GenericProtobufSchema genericSchema) {
        this.genericSchema = genericSchema;
        this.msgDesc = genericSchema.getProtobufSchema();
        builder = DynamicMessage.newBuilder(msgDesc);
    }

    @Override
    public GenericRecordBuilder set(String fieldName, Object value) {
        builder.setField(msgDesc.findFieldByName(fieldName), value);
        return this;
    }

    @Override
    public GenericRecordBuilder set(Field field, Object value) {
        builder.setField(msgDesc.findFieldByName(field.getName()), value);
        return this;
    }

    @Override
    public GenericRecordBuilder clear(String fieldName) {
        builder.clearField(msgDesc.findFieldByName(fieldName));
        return this;
    }

    @Override
    public GenericRecordBuilder clear(Field field) {
        builder.clearField(msgDesc.findFieldByName(field.getName()));
        return this;
    }

    @Override
    public GenericRecord build() {
        return new GenericProtobufRecord(
                null,
                genericSchema.getProtobufSchema(),
                genericSchema.getFields(),
                builder.build()
        );

    }
}

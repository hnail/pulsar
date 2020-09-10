package org.apache.pulsar.client.impl.schema.generic;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.pulsar.client.api.schema.Field;

import java.util.List;

public class GenericProtobufRecord<T extends DynamicMessage> extends VersionedGenericRecord {

    private DynamicMessage record;

    private Descriptors.Descriptor msgDesc;

    protected GenericProtobufRecord(byte[] schemaVersion, Descriptors.Descriptor msgDesc, List<Field> fields, DynamicMessage record) {
        super(schemaVersion, fields);
        this.msgDesc = msgDesc;
        this.record = record;
    }

    @Override
    public Object getField(String fieldName) {
        return record.getField(msgDesc.findFieldByName(fieldName));
    }


    public DynamicMessage getProtobufRecord() {
        return record;
    }
}

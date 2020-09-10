package org.apache.pulsar.client.impl.schema.generic;

import com.google.protobuf.Descriptors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class GenericProtobufSchema extends AbstractGenericSchema {

    Descriptors.Descriptor descriptor;

    public GenericProtobufSchema(SchemaInfo schemaInfo) {
        this(schemaInfo, true);
    }

    public GenericProtobufSchema(SchemaInfo schemaInfo,
                                 boolean useProvidedSchemaAsReaderSchema) {
        super(schemaInfo, useProvidedSchemaAsReaderSchema);
        this.descriptor = parseProtobufSchema(schemaInfo);
        this.fields = descriptor.getFields()
                .stream()
                .map(f -> new Field(f.getName(), f.getIndex()))
                .collect(Collectors.toList());
        setReader(new GenericProtobufReader(descriptor));
        setWriter(new GenericProtobufWriter());
    }


    @Override
    public List<Field> getFields() {
        return fields;
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        return new ProtobufRecordBuilderImpl(this);
    }


    @Override
    protected SchemaReader<GenericRecord> loadReader(BytesSchemaVersion schemaVersion) {
        SchemaInfo schemaInfo = getSchemaInfoByVersion(schemaVersion.get());
        if (schemaInfo != null) {
            log.info("Load schema reader for version({}), schema is : {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                    schemaInfo);
            Descriptors.Descriptor recordDescriptor = parseProtobufSchema(schemaInfo);
            Descriptors.Descriptor readerSchemaDescriptor = useProvidedSchemaAsReaderSchema ? descriptor : recordDescriptor;
            return new GenericProtobufReader(
                    readerSchemaDescriptor,
                    schemaVersion.get());
        } else {
            log.warn("No schema found for version({}), use latest schema : {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                    this.schemaInfo);
            return reader;
        }
    }


    protected static Descriptors.Descriptor parseProtobufSchema(SchemaInfo schemaInfo) {
        return SchemaUtils.ProtobufSchemaSerializer.deserialize(new String(schemaInfo.getSchema(), UTF_8));
    }


    public static GenericSchema of(SchemaInfo schemaInfo) {
        return new GenericProtobufSchema(schemaInfo, true);
    }


    public static GenericSchema of(SchemaInfo schemaInfo, boolean useProvidedSchemaAsReaderSchema) {
        return new GenericProtobufSchema(schemaInfo, useProvidedSchemaAsReaderSchema);
    }


    public Descriptors.Descriptor getProtobufSchema() {
        return descriptor;
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

}

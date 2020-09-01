package org.apache.pulsar.sql.presto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.decoder.json.DefaultJsonFieldDecoder;
import io.prestosql.decoder.json.JsonFieldDecoder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.*;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.sql.presto.decoder.avro.PulsarAvroRowDecoder;
import org.apache.pulsar.sql.presto.decoder.json.PulsarJsonRowDecoder;
import org.apache.pulsar.sql.presto.decoder.primitive.PulsarPrimitiveRowDecoder;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

public class PulsarDispatchingRowDecoderFactory {

    private static final Logger log = Logger.get(PulsarDispatchingRowDecoderFactory.class);

    private TypeManager typeManager;

    @Inject
    public PulsarDispatchingRowDecoderFactory(TypeManager typeManager) {
        this.typeManager = typeManager;
    }


    public RowDecoder create(SchemaInfo schemaInfo, Set<DecoderColumnHandle> columns) {

        if (SchemaType.AVRO.equals(schemaInfo.getType())) {

            byte[] dataSchema = schemaInfo.getSchema();
            Schema parsedSchema = (new Schema.Parser()).parse(new String(dataSchema, StandardCharsets.UTF_8));
            return new PulsarAvroRowDecoder(new GenericDatumReader<GenericRecord>(parsedSchema), columns);

        } else if (SchemaType.JSON.equals(schemaInfo.getType())) {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<DecoderColumnHandle, JsonFieldDecoder> fieldDecoders = columns.stream().collect(toImmutableMap(identity(), DefaultJsonFieldDecoder::new));
            return new PulsarJsonRowDecoder(objectMapper, fieldDecoders);
        } else if (schemaInfo.getType().isPrimitive()) {
            if (columns.size() == 1) {
                return new PulsarPrimitiveRowDecoder(columns.iterator().next());
            } else {
                throw new RuntimeException("Primitive type must has only one  ColumnHandle ");
            }
        } else {
            return null;
        }
    }


    public List<ColumnMetadata> extractColumnMetadata(SchemaInfo schemaInfo, PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {

        String schemaJson = new String(schemaInfo.getSchema());
        if (StringUtils.isBlank(schemaJson)) {
            throw new PrestoException(NOT_SUPPORTED, "Topic "
                    + " does not have a valid schema");
        }

        Schema schema;
        try {
            schema = PulsarConnectorUtils.parseSchema(schemaJson);
        } catch (SchemaParseException ex) {
            throw new PrestoException(NOT_SUPPORTED, "Topic "
                    + " does not have a valid schema");
        }

        if (schemaInfo.getType().isStruct()) {
            switch (schemaInfo.getType()) {
                case JSON:
                    return schema.getFields().stream()
                            .map(field ->
                                    new PulsarColumnMetadata(field.name(), parseJsonPrestoType(field.name(), field.schema()), field.schema().toString(), null, false, false,
                                            handleKeyValueType, new PulsarColumnMetadata.DecoderExtraInfo(field.name(), null, null))

                            ).collect(toList());
                case PROTOBUF:
                    throw new UnsupportedOperationException(" PROTOBUF schema haven't support ");
                case AVRO:
                    return schema.getFields().stream()
                            .map(field ->
                                    new PulsarColumnMetadata(field.name(), parseAvroPrestoType(field.name(), field.schema()), field.schema().toString(), null, false, false,
                                            handleKeyValueType, new PulsarColumnMetadata.DecoderExtraInfo(field.name(), null, null))

                            ).collect(toList());
                default:
                    throw new UnsupportedOperationException(format(" Schema type '%s' schema haven't support ", schemaInfo.getType()));

            }
        } else if (schemaInfo.getType().isPrimitive()) {
            ColumnMetadata valueColumn = new PulsarColumnMetadata(
                    PulsarColumnMetadata.getColumnName(handleKeyValueType, "__value__"),
                    parsePrimitivePrestoType("__value__", schemaInfo.getType()),
                    "The value of the message with primitive type schema", null, false, false,
                    handleKeyValueType, new PulsarColumnMetadata.DecoderExtraInfo("__value__", null, null));
            return Arrays.asList(valueColumn);
        }


        return null;
    }


    private Type parsePrimitivePrestoType(String fieldName, SchemaType pulsarType) {

        switch (pulsarType) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case INT8:
                return TinyintType.TINYINT;
            case INT16:
                return SmallintType.SMALLINT;
            case INT32:
                return IntegerType.INTEGER;
            case INT64:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case NONE:
            case BYTES:
                return VarbinaryType.VARBINARY;
            case STRING:
                return VarcharType.VARCHAR;
            case DATE:
                return DateType.DATE;
            case TIME:
                return TimeType.TIME;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP;
            default:
                log.error("Cannot convert type: %s", pulsarType);
                return null;
        }

    }


    private Type parseJsonPrestoType(String fieldname, Schema schema) {
        Schema.Type type = schema.getType();
        switch (type) {
            case STRING:
            case ENUM:
                return VarcharType.VARCHAR;
            case NULL:
                throw new UnsupportedOperationException(format("Cannot convert from Schema type '%s' (%s) to Presto type", schema.getType(), schema.getFullName()));
            case FIXED:
            case BYTES:
                return VarbinaryType.VARBINARY;
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case ARRAY:
                return VarcharType.VARCHAR;
            case MAP: //@see https://docs.oracle.com/database/nosql-12.1.3.0/GettingStartedGuide/avroschemas.html
                return VarcharType.VARCHAR;
            case RECORD:
                return VarcharType.VARCHAR;
            case UNION:// TODO https://github.com/prestosql/presto/pull/3483
                for (Schema nestType : schema.getTypes()) {
                    if (isPrimitiveType(nestType.getType())) {
                        if (nestType.getType() != Schema.Type.NULL) {
                            return parseJsonPrestoType(fieldname, nestType);
                        }
                    } else {
                        return parseJsonPrestoType(fieldname, nestType);
                    }
                }
                return RowType.from(null);
            default:
                throw new UnsupportedOperationException(format("Cannot convert from Schema type '%s' (%s) to Presto type", schema.getType(), schema.getFullName()));
        }
    }

    private Type parseAvroPrestoType(String fieldname, Schema schema) {
        Schema.Type type = schema.getType();
        LogicalType logicalType = schema.getLogicalType();
        switch (type) {
            case STRING:
            case ENUM:
                return createUnboundedVarcharType();
            case NULL:
                throw new UnsupportedOperationException();
            case FIXED:
            case BYTES:
                return VarbinaryType.VARBINARY;
            case INT:
                if (logicalType == LogicalTypes.timeMillis()) {
                    return TIME;
                } else if (logicalType == LogicalTypes.date()) {
                    return DATE;
                }
                return IntegerType.INTEGER;
            case LONG:
                if (logicalType == LogicalTypes.timestampMillis()) {
                    return TimestampType.TIMESTAMP;
                }
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case ARRAY:
                return new ArrayType(parseAvroPrestoType(fieldname, schema.getElementType()));
            case MAP: //@see https://docs.oracle.com/database/nosql-12.1.3.0/GettingStartedGuide/avroschemas.html
                TypeSignature valueType = parseAvroPrestoType(fieldname, schema.getValueType()).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.typeParameter(VarcharType.VARCHAR.getTypeSignature()), TypeSignatureParameter.typeParameter(valueType)));
            case RECORD:
                if (schema.getFields().size() > 0) {
                    return RowType.from(schema.getFields().stream()
                            .map(field -> new RowType.Field(Optional.of(field.name()), parseAvroPrestoType(field.name(), field.schema())))
                            .collect(toImmutableList()));
                } else {
                    return createUnboundedVarcharType(); //  https://avro.apache.org/docs/1.8.1/spec.html#Logical+Types
                }
            case UNION:// TODO https://github.com/prestosql/presto/pull/3483
                for (Schema nestType : schema.getTypes()) {
                    if (isPrimitiveType(nestType.getType())) {
                        if (nestType.getType() != Schema.Type.NULL) {
                            return parseAvroPrestoType(fieldname, nestType);
                        }
                    } else {
                        return parseAvroPrestoType(fieldname, nestType);
                    }
                }
                return RowType.from(null);
            default:
                throw new UnsupportedOperationException(format("Cannot convert from Schema type '%s' (%s) to Presto type", schema.getType(), schema.getFullName()));
        }
    }

    boolean isPrimitiveType(Schema.Type type) {
        return Schema.Type.NULL == type
                || Schema.Type.BOOLEAN == type
                || Schema.Type.INT == type
                || Schema.Type.LONG == type
                || Schema.Type.FLOAT == type
                || Schema.Type.DOUBLE == type
                || Schema.Type.BYTES == type
                || Schema.Type.STRING == type;

    }


}
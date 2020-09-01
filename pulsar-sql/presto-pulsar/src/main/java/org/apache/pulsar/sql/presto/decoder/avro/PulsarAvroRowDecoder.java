package org.apache.pulsar.sql.presto.decoder.avro;

import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.decoder.avro.AvroColumnDecoder;
import io.prestosql.spi.PrestoException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class PulsarAvroRowDecoder implements RowDecoder {

    public static final String NAME = "avro";
    private final DatumReader<GenericRecord> avroRecordReader;
    private final Map<DecoderColumnHandle, AvroColumnDecoder> columnDecoders;

    public PulsarAvroRowDecoder(DatumReader<GenericRecord> avroRecordReader, Set<DecoderColumnHandle> columns) {
        this.avroRecordReader = requireNonNull(avroRecordReader, "avroRecordReader is null");
        requireNonNull(columns, "columns is null");
        columnDecoders = columns.stream()
                .collect(toImmutableMap(identity(), this::createColumnDecoder));
    }

    private AvroColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle) {
        return new AvroColumnDecoder(columnHandle);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap) {
        GenericRecord avroRecord;

        try {
            avroRecord = avroRecordReader.read(null, DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(data), null));
        } catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Decoding Avro record failed.", e);
        }
        return Optional.of(columnDecoders.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().decodeField(avroRecord))));
    }


}

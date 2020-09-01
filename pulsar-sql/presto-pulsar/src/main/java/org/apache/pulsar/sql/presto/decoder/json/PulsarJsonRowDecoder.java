package org.apache.pulsar.sql.presto.decoder.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.decoder.json.JsonFieldDecoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PulsarJsonRowDecoder implements RowDecoder {


    public static final String NAME = "json";

    private final ObjectMapper objectMapper;
    private final Map<DecoderColumnHandle, JsonFieldDecoder> fieldDecoders;

    public PulsarJsonRowDecoder(ObjectMapper objectMapper, Map<DecoderColumnHandle, JsonFieldDecoder> fieldDecoders) {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.fieldDecoders = ImmutableMap.copyOf(fieldDecoders);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data,
                                                                            Map<String, String> dataMap) {
        JsonNode tree;
        try {
            tree = objectMapper.readTree(data);
        } catch (Exception e) {
            return Optional.empty();
        }

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = new HashMap<>();

        for (Map.Entry<DecoderColumnHandle, JsonFieldDecoder> entry : fieldDecoders.entrySet()) {
            DecoderColumnHandle columnHandle = entry.getKey();
            JsonFieldDecoder decoder = entry.getValue();
            JsonNode node = locateNode(tree, columnHandle);
            decodedRow.put(columnHandle, decoder.decode(node));
        }

        return Optional.of(decodedRow);
    }

    private static JsonNode locateNode(JsonNode tree, DecoderColumnHandle columnHandle) {
        String mapping = columnHandle.getMapping();
        checkState(mapping != null, "No mapping for %s", columnHandle.getName());

        JsonNode currentNode = tree;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(mapping)) {
            if (!currentNode.has(pathElement)) {
                return MissingNode.getInstance();
            }
            currentNode = currentNode.path(pathElement);
        }
        return currentNode;
    }
}

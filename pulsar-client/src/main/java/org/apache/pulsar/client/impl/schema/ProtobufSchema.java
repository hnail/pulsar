/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.impl.schema.reader.ProtobufReader;
import org.apache.pulsar.client.impl.schema.writer.ProtobufWriter;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A schema implementation to deal with protobuf generated messages.
 */
public class ProtobufSchema<T extends GeneratedMessageV3> extends StructSchema<T> {

    public static final String PARSING_INFO_PROPERTY = "__PARSING_INFO__";

    private Descriptors.Descriptor schema;

    @Getter
    @AllArgsConstructor
    public static class ProtoBufParsingInfo {
        private final int number;
        private final String name;
        private final String type;
        private final String label;
        // For future nested fields
        private final Map <String, Object> definition;
    }


    private static <T> Descriptors.Descriptor createProtobufSchema(Class<T> pojo) {
        try {
            Method method = pojo.getMethod("getDescriptor");
            Descriptors.Descriptor  descriptor = (Descriptors.Descriptor) method.invoke(null);
            return descriptor;
        } catch (IllegalAccessException| InvocationTargetException|NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private ProtobufSchema(SchemaInfo schemaInfo, T protoMessageInstance) {
        super(schemaInfo);
        setReader(new ProtobufReader<>(protoMessageInstance));
        setWriter(new ProtobufWriter<>());
        // update properties with protobuf related properties
        Map<String, String> allProperties = new HashMap<>();
        allProperties.putAll(schemaInfo.getProperties());
        // set protobuf parsing info
        allProperties.put(PARSING_INFO_PROPERTY, getParsingInfo(protoMessageInstance));
        schemaInfo.setProperties(allProperties);
    }

    private String getParsingInfo(T protoMessageInstance) {
        List<ProtoBufParsingInfo> protoBufParsingInfos = new LinkedList<>();
        protoMessageInstance.getDescriptorForType().getFields().forEach(new Consumer<Descriptors.FieldDescriptor>() {
            @Override
            public void accept(Descriptors.FieldDescriptor fieldDescriptor) {
                protoBufParsingInfos.add(new ProtoBufParsingInfo(fieldDescriptor.getNumber(),
                        fieldDescriptor.getName(), fieldDescriptor.getType().name(),
                        fieldDescriptor.toProto().getLabel().name(), null));
            }
        });

        try {
            return new ObjectMapper().writeValueAsString(protoBufParsingInfos);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    public Descriptors.Descriptor getProtobufSchema() {
        return schema;
    }

    @Override
    protected SchemaReader<T> loadReader(BytesSchemaVersion schemaVersion) {
        throw new RuntimeException("ProtobufSchema don't support schema versioning");
    }

    public static <T extends GeneratedMessageV3> ProtobufSchema<T> of(Class<T> pojo) {
        return of(pojo, new HashMap<>());
    }

    public static <T> ProtobufSchema ofGenericClass(Class<T> pojo, Map<String, String> properties) {
        SchemaDefinition<T> schemaDefinition = SchemaDefinition.<T>builder().withPojo(pojo).withProperties(properties).build();
        return ProtobufSchema.of(schemaDefinition);
    }

    public static <T> ProtobufSchema of(SchemaDefinition<T> schemaDefinition) {
        Class<T> pojo = schemaDefinition.getPojo();

        if (!GeneratedMessageV3.class.isAssignableFrom(pojo)) {
            throw new IllegalArgumentException(GeneratedMessageV3.class.getName()
                    + " is not assignable from " + pojo.getName());
        }
        Descriptors.Descriptor descriptor = createProtobufSchema(schemaDefinition.getPojo());

        SchemaInfo schemaInfo = SchemaInfo.builder()
                    .schema(SchemaUtils.ProtobufSchemaSerializer.serialize(descriptor).getBytes(UTF_8))
                    .type(SchemaType.PROTOBUF)
                    .name("")
                    .properties(schemaDefinition.getProperties())
                    .build();
        try {
            return new ProtobufSchema(schemaInfo,
                (GeneratedMessageV3) pojo.getMethod("getDefaultInstance").invoke(null));
        }catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static <T extends GeneratedMessageV3> ProtobufSchema<T> of(
            Class pojo, Map<String, String> properties){
        return ofGenericClass(pojo, properties);
    }
}

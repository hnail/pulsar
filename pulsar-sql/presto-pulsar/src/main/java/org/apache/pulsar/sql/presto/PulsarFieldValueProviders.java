package org.apache.pulsar.sql.presto;

import io.prestosql.decoder.FieldValueProvider;

public class PulsarFieldValueProviders {

    public static FieldValueProvider doubleValueProvider(double value) {
        return new FieldValueProvider() {
            @Override
            public double getDouble() {
                return value;
            }

            @Override
            public boolean isNull() {
                return false;
            }
        };
    }
}

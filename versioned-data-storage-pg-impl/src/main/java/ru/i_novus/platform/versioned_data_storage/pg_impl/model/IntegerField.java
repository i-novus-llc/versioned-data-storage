package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.IntegerFieldValue;

import java.math.BigInteger;

/**
 * @author lgalimova
 * @since 23.03.2018
 */
public class IntegerField extends Field<BigInteger> {
    public static final String TYPE = "bigint";

    public IntegerField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public FieldValue valueOf(BigInteger value) {
        return new IntegerFieldValue(getName(), value);
    }
}

package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.IntegerFieldValue;

/**
 * @author lgalimova
 * @since 23.03.2018
 */
public class IntegerField extends Field {
    public static final String TYPE = "integer";

    public IntegerField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public FieldValue valueOf(Object value) {
        return new IntegerFieldValue(getName(), (Integer)value);
    }
}

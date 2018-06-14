package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.FloatFieldValue;

/**
 * @author lgalimova
 * @since 23.03.2018
 */
public class FloatField extends Field<Float> {
    public static final String TYPE = "numeric";

    public FloatField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public FieldValue valueOf(Float value) {
        return new FloatFieldValue(getName(), value);
    }
}

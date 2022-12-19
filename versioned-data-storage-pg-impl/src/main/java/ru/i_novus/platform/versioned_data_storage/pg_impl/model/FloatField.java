package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.FloatFieldValue;

/**
 * @author lgalimova
 * @since 23.03.2018
 */
public class FloatField extends Field<Number> {

    public static final String TYPE = "numeric";

    public FloatField() {
        // Nothing to do.
    }

    public FloatField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public FieldValue valueOf(Number value) {
        return new FloatFieldValue(getName(), value);
    }
}

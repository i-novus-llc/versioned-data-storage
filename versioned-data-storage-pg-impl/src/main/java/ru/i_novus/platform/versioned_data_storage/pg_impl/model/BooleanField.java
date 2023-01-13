package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.BooleanFieldValue;

/**
 * @author lgalimova
 * @since 23.03.2018
 */
public class BooleanField extends Field<Boolean> {

    public static final String TYPE = "boolean";

    public BooleanField() {
        // Nothing to do.
    }

    public BooleanField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class getFieldValueClass() {
        return BooleanFieldValue.class;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public FieldValue valueOf(Boolean value) {
        return new BooleanFieldValue(getName(), value);
    }
}

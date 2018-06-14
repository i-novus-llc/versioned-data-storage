package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.BooleanFieldValue;

/**
 * @author lgalimova
 * @since 23.03.2018
 */
public class BooleanField extends Field {
    public static final String TYPE = "boolean";

    public BooleanField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public FieldValue valueOf(Object value) {
        if (value instanceof String) {
            return new BooleanFieldValue(getName(), Boolean.valueOf((String)value));
        } else {
            return new BooleanFieldValue(getName(), (Boolean) value);
        }
    }
}

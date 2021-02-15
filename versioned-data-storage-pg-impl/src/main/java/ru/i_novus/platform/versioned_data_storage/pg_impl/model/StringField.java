package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.StringFieldValue;

/**
 * @author lgalimova
 * @since 23.03.2018
 */
public class StringField extends Field<String> {

    public static final String TYPE = "character varying";

    public StringField() {
        // Nothing to do.
    }

    public StringField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public FieldValue valueOf(String value) {
        return new StringFieldValue(getName(), value);
    }
}

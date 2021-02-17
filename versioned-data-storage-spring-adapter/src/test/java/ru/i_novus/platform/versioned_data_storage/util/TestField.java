package ru.i_novus.platform.versioned_data_storage.util;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

public class TestField extends Field<Short> {

    public static final String TYPE = "smallint";

    public TestField() {
        // Nothing to do.
    }

    public TestField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public FieldValue valueOf(Short value) {
        return new TestFieldValue(getName(), value);
    }
}

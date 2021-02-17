package ru.i_novus.platform.versioned_data_storage.util;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

public class TestFieldValue extends FieldValue<Short> {

    public TestFieldValue() {
    }

    public TestFieldValue(String field, Short value) {
        super(field, value);
    }
}

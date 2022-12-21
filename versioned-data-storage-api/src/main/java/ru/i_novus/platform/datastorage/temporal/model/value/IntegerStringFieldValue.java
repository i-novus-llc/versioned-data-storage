package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class IntegerStringFieldValue extends FieldValue<String> {

    public IntegerStringFieldValue() {
    }

    public IntegerStringFieldValue(String field, String value) {
        super(field, value);
    }
}

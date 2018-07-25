package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class StringFieldValue extends FieldValue<String> {

    public StringFieldValue() {
    }

    public StringFieldValue(String field, String value) {
        super(field, value);
    }
}

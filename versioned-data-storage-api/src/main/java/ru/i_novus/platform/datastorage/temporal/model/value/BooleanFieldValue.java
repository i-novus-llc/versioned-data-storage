package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class BooleanFieldValue extends FieldValue<Boolean> {

    public BooleanFieldValue() {
    }

    public BooleanFieldValue(String field, Boolean value) {
        super(field, value);
    }
}

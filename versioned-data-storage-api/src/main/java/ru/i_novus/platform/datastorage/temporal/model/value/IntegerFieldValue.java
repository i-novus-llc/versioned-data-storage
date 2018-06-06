package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class IntegerFieldValue extends FieldValue<Number> {

    public IntegerFieldValue(String field, Number value) {
        super(field, value);
    }
}

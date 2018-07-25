package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class TreeFieldValue extends FieldValue<String> {

    public TreeFieldValue() {
    }

    public TreeFieldValue(String field, String value) {
        super(field, value);
    }
}

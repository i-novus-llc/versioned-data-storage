package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

import java.time.LocalDate;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class DateFieldValue extends FieldValue<LocalDate> {

    public DateFieldValue() {
    }

    public DateFieldValue(String field, LocalDate value) {
        super(field, value);
    }
}

package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

import java.util.Date;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class DateFieldValue extends FieldValue<Date> {

    public DateFieldValue(String field, Date value) {
        super(field, value);
    }
}

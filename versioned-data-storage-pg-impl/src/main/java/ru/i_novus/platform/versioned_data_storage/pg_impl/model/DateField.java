package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.DateFieldValue;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class DateField extends Field {
    public static final String TYPE = "date";

    public DateField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public FieldValue valueOf(Object value) {
        return new DateFieldValue(getName(), (Date) value);
    }
}

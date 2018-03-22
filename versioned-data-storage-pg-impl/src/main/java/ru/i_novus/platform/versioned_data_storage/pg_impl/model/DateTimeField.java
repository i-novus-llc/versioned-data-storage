package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class DateTimeField extends Field {
    @Override
    public String getType() {
        return "timestamp";
    }
}

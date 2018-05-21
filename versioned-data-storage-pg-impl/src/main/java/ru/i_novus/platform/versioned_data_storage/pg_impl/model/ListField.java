package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;

/**
 * @author lgalimova
 * @since 21.05.2018
 */
public class ListField extends Field {
    public static final String TYPE = "jsonb";

    public ListField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }
}

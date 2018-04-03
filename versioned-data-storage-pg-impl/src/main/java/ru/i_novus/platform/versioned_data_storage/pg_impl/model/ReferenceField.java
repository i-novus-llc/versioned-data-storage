package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class ReferenceField extends Field {
    public static final String TYPE = "jsonb";

    public ReferenceField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }
}

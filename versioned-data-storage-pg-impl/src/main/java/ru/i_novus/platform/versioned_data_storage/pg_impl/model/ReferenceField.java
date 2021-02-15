package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.Reference;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class ReferenceField extends Field<Reference> {

    public static final String TYPE = "jsonb";

    public ReferenceField() {
        // Nothing to do.
    }

    public ReferenceField(String name) {
        super(name);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public FieldValue valueOf(Reference value) {
        return new ReferenceFieldValue(getName(), value);
    }
}

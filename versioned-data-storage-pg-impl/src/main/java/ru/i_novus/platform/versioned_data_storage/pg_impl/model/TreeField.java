package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.TreeFieldValue;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * @author lgalimova
 * @since 16.05.2018
 */
public class TreeField extends Field<String> {
    public static final String TYPE = "ltree";

    private String path;

    public TreeField(String name) {
        super(name);
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public FieldValue valueOf(String value) {
        return new TreeFieldValue(getName(), value);
    }
}

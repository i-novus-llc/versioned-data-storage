package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import ru.i_novus.platform.datastorage.temporal.enums.FieldType;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.service.FieldFactory;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class FieldFactoryImpl implements FieldFactory {

    public Field createField(String name, FieldType type) {
        switch (type) {
            case BOOLEAN:
                return new BooleanField(name);
            case DATE:
                return new DateField(name);
            case FLOAT:
                return new FloatField(name);
            case INTEGER:
                return new IntegerField(name);
            case REFERENCE:
                ReferenceField ref = new ReferenceField(name);
                ref.setSearchEnabled(true);
                return ref;
            case TREE:
                Field tree = new TreeField(name);
                tree.setSearchEnabled(true);
                return tree;
            default:
                return new StringField(name);
        }

    }

    @Override
    public Field createUniqueField(String name, FieldType type) {
        Field field = createField(name, type);
        field.setUnique(true);
        return field;
    }

    @Override
    public Field createSearchField(String name, FieldType type) {
        Field field = createField(name, type);
        field.setSearchEnabled(true);
        return field;
    }
}

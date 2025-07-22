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

    @Override
    public Field<?> createField(String name, FieldType type) {

        return switch (type) {
            case BOOLEAN -> new BooleanField(name);
            case DATE -> new DateField(name);
            case FLOAT -> new FloatField(name);
            case INTEGER -> new IntegerField(name);
            case REFERENCE -> createReferenceField(name);
            case TREE -> createTreeField(name);
            default -> new StringField(name);
        };
    }

    private static ReferenceField createReferenceField(String name) {

        final ReferenceField field = new ReferenceField(name);
        field.setSearchEnabled(true);

        return field;
    }

    private static Field<?> createTreeField(String name) {

        final Field<?> field = new TreeField(name);
        field.setSearchEnabled(true);

        return field;
    }

    @Override
    public Field<?> createUniqueField(String name, FieldType type) {

        final Field<?> field = createField(name, type);
        field.setUnique(true);

        return field;
    }

    @Override
    public Field<?> createSearchField(String name, FieldType type) {

        final Field<?> field = createField(name, type);
        field.setSearchEnabled(true);

        return field;
    }
}

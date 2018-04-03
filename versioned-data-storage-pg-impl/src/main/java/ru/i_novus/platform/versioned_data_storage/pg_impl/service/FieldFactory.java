package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;

import java.util.Arrays;
import java.util.Date;

/**
 * @author lgalimova
 * @since 02.04.2018
 */
public class FieldFactory {
    public Field getField(String name, String type) {
        switch (type) {
            case BooleanField.TYPE:
                return new BooleanField(name);
            case DateField.TYPE:
                return new DateField(name);
            case FloatField.TYPE:
                return new FloatField(name);
            case IntegerField.TYPE:
                return new IntegerField(name);
            case ReferenceField.TYPE:
                return new ReferenceField(name);
            default:
                return new StringField(name);
        }

    }
}

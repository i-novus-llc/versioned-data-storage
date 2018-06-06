package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import ru.i_novus.platform.datastorage.temporal.enums.FieldType;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.BooleanFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.IntegerFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.StringFieldValue;
import ru.i_novus.platform.datastorage.temporal.service.FieldValueFactory;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class FieldValueFactoryImpl implements FieldValueFactory {

    public FieldValue createFieldValue(String field, FieldType type, Object value){
        return null;
    }
}

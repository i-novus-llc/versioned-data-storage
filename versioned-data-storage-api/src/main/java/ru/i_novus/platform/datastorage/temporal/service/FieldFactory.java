package ru.i_novus.platform.datastorage.temporal.service;

import ru.i_novus.platform.datastorage.temporal.enums.FieldType;
import ru.i_novus.platform.datastorage.temporal.model.Field;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public interface FieldFactory {

    Field createField(String name, FieldType type);

    Field createUniqueField(String name, FieldType type);

    Field createSearchField(String name, FieldType type);

}

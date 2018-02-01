package ru.i_novus.platform.versioned_data_storage.api.model;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public interface FieldValue<T> {
    String getName();
    T getValue();
}

package ru.i_novus.platform.versioned_data_storage.api.model;

import java.util.List;

/**
 * @author lgalimova
 * @since 01.02.2018
 *
 */
public abstract class RowValue<T> {
    private T systemId;
    private List<FieldValue> fieldValues;

    public T getSystemId() {
        return systemId;
    }

    public void setSystemId(T systemId) {
        this.systemId = systemId;
    }

    public List<FieldValue> getFieldValues() {
        return fieldValues;
    }

    public void setFieldValues(List<FieldValue> fieldValues) {
        this.fieldValues = fieldValues;
    }
}

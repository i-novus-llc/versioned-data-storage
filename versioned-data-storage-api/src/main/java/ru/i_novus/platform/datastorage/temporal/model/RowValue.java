package ru.i_novus.platform.datastorage.temporal.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lgalimova
 * @since 01.02.2018
 *
 */
public abstract class RowValue<T> {
    private T systemId;
    private List<FieldValue> fieldValues = new ArrayList<>();

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

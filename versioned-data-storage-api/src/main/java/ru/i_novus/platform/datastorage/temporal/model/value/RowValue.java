package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author lgalimova
 * @since 01.02.2018
 *
 */
public abstract class RowValue<T> {
    private T systemId;
    private List<FieldValue> fieldValues = new ArrayList<>();

    public RowValue() {
    }

    public RowValue(T systemId, List<FieldValue> fieldValues) {
        this.systemId = systemId;
        this.fieldValues = fieldValues;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowValue<?> rowValue = (RowValue<?>) o;
        return Objects.equals(systemId, rowValue.systemId) &&
                Objects.equals(fieldValues, rowValue.fieldValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(systemId, fieldValues);
    }

    @Override
    public String toString() {
        return "RowValue{" + "systemId=" + systemId +
                ", fieldValues=" + fieldValues.stream().map(FieldValue::toString).collect(Collectors.joining(", ")) +
                '}';
    }
}

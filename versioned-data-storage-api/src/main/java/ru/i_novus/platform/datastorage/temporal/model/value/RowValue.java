package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static ru.i_novus.platform.datastorage.temporal.CollectionUtils.isNullOrEmpty;

/**
 * @author lgalimova
 * @since 01.02.2018
 *
 */
public abstract class RowValue<T> {

    private T systemId;
    private List<FieldValue> fieldValues = new ArrayList<>();
    private String hash;

    public RowValue() {
    }

    public RowValue(T systemId, List<FieldValue> fieldValues) {
        this.systemId = systemId;
        this.fieldValues = fieldValues;
    }

    public RowValue(T systemId, List<FieldValue> fieldValues, String hash) {
        this.systemId = systemId;
        this.fieldValues = fieldValues;
        this.hash = hash;
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

    public FieldValue getFieldValue(String fieldName) {

        if (isNullOrEmpty(fieldValues))
            return null;

        return fieldValues.stream()
                .filter(fieldValue -> fieldName.equals(fieldValue.getField()))
                .findFirst().orElse(null);
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RowValue<?> that = (RowValue<?>) o;
        return Objects.equals(systemId, that.systemId) &&
                Objects.equals(fieldValues, that.fieldValues) &&
                Objects.equals(hash, that.hash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(systemId, fieldValues, hash);
    }

    @Override
    public String toString() {
        return "RowValue{" +
                "systemId=" + systemId +
                ", fieldValues=" + fieldValues +
                ", hash='" + hash + '\'' +
                '}';
    }
}

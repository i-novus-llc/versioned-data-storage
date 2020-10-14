package ru.i_novus.platform.datastorage.temporal.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Значение поля хранилища.
 *
 * @author lgalimova
 * @since 01.02.2018
 */
public abstract class FieldValue<T extends Serializable> implements Serializable {

    /** Наименование поля. */
    private String field;

    /** Значение поля */
    private T value;

    public FieldValue() {
        // Nothing to do.
    }

    public FieldValue(String field, T value) {
        this.field = field;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldValue<?> that = (FieldValue<?>) o;
        return Objects.equals(field, that.field) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, value);
    }

    @Override
    public String toString() {
        return "FieldValue{" +
                "field='" + field + '\'' +
                ", value=" + value +
                '}';
    }
}

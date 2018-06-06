package ru.i_novus.platform.datastorage.temporal.model;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public abstract class FieldValue<T> {
    private String field;
    private T value;

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
}

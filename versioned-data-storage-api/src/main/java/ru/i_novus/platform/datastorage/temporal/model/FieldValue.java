package ru.i_novus.platform.datastorage.temporal.model;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class FieldValue<T> {
    private Field field;
    private T value;

    public FieldValue(Field field, T value) {
        this.field = field;
        this.value = value;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}

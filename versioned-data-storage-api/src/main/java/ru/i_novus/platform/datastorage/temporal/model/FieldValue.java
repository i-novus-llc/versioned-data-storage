package ru.i_novus.platform.datastorage.temporal.model;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class FieldValue<T> {
    private String name;
    private T value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}

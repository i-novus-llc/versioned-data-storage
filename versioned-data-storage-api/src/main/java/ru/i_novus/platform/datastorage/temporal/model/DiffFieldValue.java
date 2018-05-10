package ru.i_novus.platform.datastorage.temporal.model;

import java.io.Serializable;

/**
 * @author lgalimova
 * @since 10.05.2018
 */
public class DiffFieldValue<T> implements Serializable {
    private Field field;
    private T oldValue;
    private T newValue;
    private DiffStatusEnum status;

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public T getOldValue() {
        return oldValue;
    }

    public void setOldValue(T oldValue) {
        this.oldValue = oldValue;
    }

    public T getNewValue() {
        return newValue;
    }

    public void setNewValue(T newValue) {
        this.newValue = newValue;
    }

    public DiffStatusEnum getStatus() {
        return status;
    }

    public void setStatus(DiffStatusEnum status) {
        this.status = status;
    }
}

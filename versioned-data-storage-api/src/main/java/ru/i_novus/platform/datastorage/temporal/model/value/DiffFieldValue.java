package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author lgalimova
 * @since 10.05.2018
 */
public class DiffFieldValue<T> implements Serializable {

    private Field field;
    private T oldValue;
    private T newValue;
    private DiffStatusEnum status;

    public DiffFieldValue() {
    }

    public DiffFieldValue(Field field, T oldValue, T newValue, DiffStatusEnum status) {
        this.field = field;
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.status = status;
    }

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

    public T getValue(DiffStatusEnum status) {
        return DiffStatusEnum.DELETED.equals(status) ? getOldValue() : getNewValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiffFieldValue<?> that = (DiffFieldValue<?>) o;
        return Objects.equals(field, that.field) &&
                Objects.equals(oldValue, that.oldValue) &&
                Objects.equals(newValue, that.newValue) &&
                status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, oldValue, newValue, status);
    }
}

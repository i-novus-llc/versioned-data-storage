package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.io.Serializable;
import java.util.Objects;

/**
 * Разница между значениями поля в разных версиях хранилища.
 *
 * @author lgalimova
 * @since 10.05.2018
 */
@SuppressWarnings({"java:S3740", "java:S1948"})
public class DiffFieldValue<T extends Serializable> implements Serializable {

    private Field<T> field;

    private T oldValue;
    private T newValue;

    private DiffStatusEnum status;

    public DiffFieldValue() {
    }

    public DiffFieldValue(Field<T> field, T oldValue, T newValue, DiffStatusEnum status) {

        this.field = field;
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.status = status;
    }

    public Field<T> getField() {
        return field;
    }

    public void setField(Field<T> field) {
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

    @Override
    public String toString() {
        return "DiffFieldValue{" +
                "field=" + field +
                ", oldValue=" + oldValue +
                ", newValue=" + newValue +
                ", status=" + status +
                '}';
    }
}

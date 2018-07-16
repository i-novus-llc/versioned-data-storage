package ru.i_novus.platform.datastorage.temporal.model;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class Reference implements Serializable {
    //код хранилища данных, на которое осуществляется ссылка
    private String storageCode;
    //дата публикации версии
    private Date date;
    //поле, на которое осуществляется ссылка
    private String keyField;
    //поле, отображаемое в ссылке
    private String displayField;
    //значение ключа связи
    private String value;
    //значение отображаемого значения
    private String displayValue;

    public Reference(String storageCode, Date date, String keyField, String displayField) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayField = displayField;
    }

    public Reference(String storageCode, Date date, String keyField, String displayField, String value) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayField = displayField;
        this.value = value;
    }

    public Reference(String storageCode, Date date, String keyField, String displayField, String value, String displayValue) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayField = displayField;
        this.value = value;
        this.displayValue = displayValue;
    }

    public Reference(String value, String displayValue) {
        this.value = value;
        this.displayValue = displayValue;
    }

    public String getStorageCode() {
        return storageCode;
    }

    public Date getDate() {
        return date;
    }

    public String getKeyField() {
        return keyField;
    }

    public String getDisplayField() {
        return displayField;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Object getDisplayValue() {
        return displayValue;
    }

    public void setDisplayValue(String displayValue) {
        this.displayValue = displayValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Reference reference = (Reference) o;
        return Objects.equals(storageCode, reference.storageCode) &&
                Objects.equals(date, reference.date) &&
                Objects.equals(keyField, reference.keyField) &&
                Objects.equals(displayField, reference.displayField) &&
                Objects.equals(value, reference.value) &&
                Objects.equals(displayValue, reference.displayValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageCode, date, keyField, displayField, value, displayValue);
    }

    @Override
    public String toString() {
        return "Reference{" + "storageCode='" + storageCode + '\'' +
                ", date=" + date +
                ", keyField='" + keyField + '\'' +
                ", displayField='" + displayField + '\'' +
                ", value=" + value +
                ", displayValue=" + displayValue +
                '}';
    }
}

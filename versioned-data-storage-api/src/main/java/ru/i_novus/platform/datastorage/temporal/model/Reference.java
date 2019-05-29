package ru.i_novus.platform.datastorage.temporal.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class Reference implements Serializable {
    //код хранилища данных, на которое осуществляется ссылка
    private String storageCode;
    //дата публикации версии
    private LocalDateTime date;
    //поле, на которое осуществляется ссылка
    private String keyField;
    //поле, отображаемое в ссылке
    private String displayField;
    //формат отображения ссылки
    private DisplayExpression displayExpression;
    //значение ключа связи
    private String value;
    //значение отображаемого значения
    private String displayValue;

    public Reference() {
    }

    /**
     * @deprecated
     */
    @Deprecated
    public Reference(String storageCode, LocalDateTime date, String keyField, String displayField) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayField = displayField;
    }

    public Reference(String storageCode, LocalDateTime date, String keyField, DisplayExpression displayExpression) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayExpression = displayExpression;
    }

    /**
     * @deprecated
     */
    @Deprecated
    public Reference(String storageCode, LocalDateTime date, String keyField, String displayField, String value) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayField = displayField;
        this.value = value;
    }

    public Reference(String storageCode, LocalDateTime date, String keyField, DisplayExpression displayExpression, String value) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayExpression = displayExpression;
        this.value = value;
    }

    public Reference(String storageCode, LocalDateTime date, String keyField, String displayField, String value, String displayValue) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayField = displayField;
        this.value = value;
        this.displayValue = displayValue;
    }

    public Reference(String storageCode, LocalDateTime date, String keyField, DisplayExpression displayExpression, String value, String displayValue) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayExpression = displayExpression;
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

    public LocalDateTime getDate() {
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

    public String getDisplayValue() {
        return displayValue;
    }

    public void setDisplayValue(String displayValue) {
        this.displayValue = displayValue;
    }

    public DisplayExpression getDisplayExpression() {
        return displayExpression;
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
                Objects.equals(displayExpression, reference.displayExpression) &&
                Objects.equals(value, reference.value) &&
                Objects.equals(displayValue, reference.displayValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageCode, date, keyField, displayField, displayExpression, value, displayValue);
    }

    @Override
    public String toString() {
        return "Reference{" + "storageCode='" + storageCode + '\'' +
                ", date=" + date +
                ", keyField='" + keyField + '\'' +
                ", displayField='" + displayField + '\'' +
                ", displayExpression='" + displayExpression + '\'' +
                ", value=" + value +
                ", displayValue=" + displayValue +
                '}';
    }
}

package ru.i_novus.platform.datastorage.temporal.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class Reference implements Serializable {

    /** Код хранилища данных, на которое ведёт ссылка. */
    private String storageCode;

    /** Дата публикации версии. */
    private LocalDateTime date;

    /** Поле, на которое ведёт ссылка. */
    private String keyField;

    /** Поле, отображаемое в ссылке. */
    private String displayField;

    /** Формат отображения ссылки. */
    private DisplayExpression displayExpression;

    /** Значение ключа связи. */
    private String value;

    /** Значение отображаемого значения. */
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

        this(storageCode, date, keyField, displayField);
        this.value = value;
    }

    public Reference(String storageCode, LocalDateTime date, String keyField,
                     DisplayExpression displayExpression, String value) {

        this(storageCode, date, keyField, displayExpression);
        this.value = value;
    }

    /**
     * @deprecated
     */
    @Deprecated
    public Reference(String storageCode, LocalDateTime date, String keyField, String displayField,
                     String value, String displayValue) {

        this(storageCode, date, keyField, displayField);
        this.value = value;
        this.displayValue = displayValue;
    }

    public Reference(String storageCode, LocalDateTime date, String keyField,
                     DisplayExpression displayExpression, String value, String displayValue) {

        this(storageCode, date, keyField, displayExpression);
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

        Reference that = (Reference) o;
        return Objects.equals(storageCode, that.storageCode) &&
                Objects.equals(date, that.date) &&
                Objects.equals(keyField, that.keyField) &&
                Objects.equals(displayField, that.displayField) &&
                Objects.equals(displayExpression, that.displayExpression) &&
                Objects.equals(value, that.value) &&
                Objects.equals(displayValue, that.displayValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageCode, date, keyField,
                displayField, displayExpression, value, displayValue);
    }

    @Override
    public String toString() {
        return "Reference{" +
                "storageCode='" + storageCode + '\'' +
                ", date=" + date +
                ", keyField='" + keyField + '\'' +
                ", displayField='" + displayField + '\'' +
                ", displayExpression='" + displayExpression + '\'' +
                ", value=" + value +
                ", displayValue=" + displayValue +
                '}';
    }
}

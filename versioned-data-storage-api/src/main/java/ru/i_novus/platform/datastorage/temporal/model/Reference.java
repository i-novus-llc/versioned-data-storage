package ru.i_novus.platform.datastorage.temporal.model;

import java.io.Serializable;
import java.util.Date;

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
    private Object value;
    //значение отображаемого значения
    private Object displayValue;

    public Reference(String storageCode, Date date, String keyField, String displayField) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayField = displayField;
    }

    public Reference(String storageCode, Date date, String keyField, String displayField, Object value) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayField = displayField;
        this.value = value;
    }

    public Reference(String storageCode, Date date, String keyField, String displayField, Object value, Object displayValue) {
        this.storageCode = storageCode;
        this.date = date;
        this.keyField = keyField;
        this.displayField = displayField;
        this.value = value;
        this.displayValue = displayValue;
    }

    public Reference(Object value, Object displayValue) {
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

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getDisplayValue() {
        return displayValue;
    }

    public void setDisplayValue(Object displayValue) {
        this.displayValue = displayValue;
    }
}

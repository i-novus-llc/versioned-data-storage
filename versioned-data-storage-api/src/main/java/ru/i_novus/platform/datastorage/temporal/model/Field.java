package ru.i_novus.platform.datastorage.temporal.model;

import java.io.Serializable;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public abstract class Field<T> implements Serializable {
    private String name;
    private Integer maxLength;
    private Boolean searchEnabled = false;
    private Boolean required = false;
    private Boolean unique = false;

    public Field(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Database field type
     */
    public abstract String getType();

    public abstract FieldValue valueOf(T value);

    public Boolean getSearchEnabled() {
        return searchEnabled;
    }

    public void setSearchEnabled(Boolean searchEnabled) {
        this.searchEnabled = searchEnabled;
    }

    public Boolean getRequired() {
        return required;
    }

    public void setRequired(Boolean required) {
        this.required = required;
    }

    public Boolean getUnique() {
        return unique;
    }

    public void setUnique(Boolean unique) {
        this.unique = unique;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(Integer maxLength) {
        this.maxLength = maxLength;
    }
}

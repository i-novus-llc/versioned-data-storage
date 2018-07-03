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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Field<?> field = (Field<?>) o;

        if (name != null ? !name.equals(field.name) : field.name != null) return false;
        if (maxLength != null ? !maxLength.equals(field.maxLength) : field.maxLength != null) return false;
        if (searchEnabled != null ? !searchEnabled.equals(field.searchEnabled) : field.searchEnabled != null)
            return false;
        if (required != null ? !required.equals(field.required) : field.required != null) return false;
        return !(unique != null ? !unique.equals(field.unique) : field.unique != null);

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (maxLength != null ? maxLength.hashCode() : 0);
        result = 31 * result + (searchEnabled != null ? searchEnabled.hashCode() : 0);
        result = 31 * result + (required != null ? required.hashCode() : 0);
        result = 31 * result + (unique != null ? unique.hashCode() : 0);
        return result;
    }
}

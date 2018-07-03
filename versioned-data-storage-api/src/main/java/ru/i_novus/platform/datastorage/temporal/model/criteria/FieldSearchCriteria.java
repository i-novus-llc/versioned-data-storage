package ru.i_novus.platform.datastorage.temporal.model.criteria;

import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.io.Serializable;
import java.util.List;


/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class FieldSearchCriteria implements Serializable {
    private Field field;
    private List<?> values;
    private SearchTypeEnum type = SearchTypeEnum.EXACT;

    public FieldSearchCriteria(Field field) {
        this.field = field;
    }

    public FieldSearchCriteria(Field field, SearchTypeEnum type, List<?> values) {
        this.field = field;
        this.type = type;
        this.values = values;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public List<? extends Object> getValues() {
        return values;
    }

    public void setValues(List<? extends Object> values) {
        this.values = values;
    }

    public SearchTypeEnum getType() {
        return type;
    }

    public void setType(SearchTypeEnum type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldSearchCriteria that = (FieldSearchCriteria) o;

        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        if (values != null ? !values.equals(that.values) : that.values != null) return false;
        return type == that.type;

    }

    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (values != null ? values.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }
}

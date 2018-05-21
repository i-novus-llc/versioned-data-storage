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
}

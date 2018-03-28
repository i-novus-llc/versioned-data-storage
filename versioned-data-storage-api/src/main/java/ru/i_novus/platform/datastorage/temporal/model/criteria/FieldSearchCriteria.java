package ru.i_novus.platform.datastorage.temporal.model.criteria;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

import java.io.Serializable;
import java.util.List;


/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class FieldSearchCriteria implements Serializable {
    private List<FieldValue> values; //length 1 for LIKE!
    private SearchTypeEnum type = SearchTypeEnum.EXACT;


    public List<FieldValue> getValues() {
        return values;
    }

    public void setValues(List<FieldValue> values) {
        this.values = values;
    }

    public SearchTypeEnum getType() {
        return type;
    }

    public void setType(SearchTypeEnum type) {
        this.type = type;
    }
}

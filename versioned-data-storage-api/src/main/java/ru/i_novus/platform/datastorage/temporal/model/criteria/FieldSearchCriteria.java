package ru.i_novus.platform.datastorage.temporal.model.criteria;

import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Критерий поиска по полю.
 *
 * @author lgalimova
 * @since 01.02.2018
 */
public class FieldSearchCriteria implements Serializable {

    /** Поле. */
    private Field field;

    /** Значения. */
    private List<? extends Serializable> values;

    /** Тип. */
    private SearchTypeEnum type = SearchTypeEnum.EXACT;

    public FieldSearchCriteria(Field field) {

        this.field = field;
    }

    public FieldSearchCriteria(Field field, SearchTypeEnum type, List<? extends Serializable> values) {

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

    public List<? extends Serializable> getValues() {
        return values;
    }

    public void setValues(List<? extends Serializable> values) {
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
        return Objects.equals(field, that.field) &&
                Objects.equals(values, that.values) &&
                (type == that.type);

    }

    @Override
    public int hashCode() {
        return Objects.hash(field, values, type);
    }

    @Override
    public String toString() {
        return "FieldSearchCriteria{" +
                ", field=" + field +
                ", values=" + values +
                ", type=" + type +
                '}';
    }
}

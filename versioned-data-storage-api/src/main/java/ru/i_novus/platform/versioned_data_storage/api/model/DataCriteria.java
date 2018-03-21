package ru.i_novus.platform.versioned_data_storage.api.model;

import net.n2oapp.criteria.api.Criteria;
import ru.i_novus.platform.versioned_data_storage.api.criteria.FieldSearchCriteria;

import java.util.Date;
import java.util.List;

public class DataCriteria  extends Criteria {
    private final String tableName;
    private final Date date;
    private final List<Field> fields;
    private final List<FieldSearchCriteria> fieldFilter;
    private final String commonFilter;
    private final Criteria criteria;

    /**
     * @param tableName    наименование таблицы
     * @param date         дата версии
     * @param fields       список полей в ответе
     * @param fieldFilter  фильтр по отдельным полям
     * @param commonFilter фильтр по всем полям
     * @param criteria     содержит page, size, sorting
     */
    public DataCriteria(String tableName, Date date, List<Field> fields, List<FieldSearchCriteria> fieldFilter, String commonFilter, Criteria criteria) {
        this.tableName = tableName;
        this.date = date;
        this.fields = fields;
        this.fieldFilter = fieldFilter;
        this.commonFilter = commonFilter;
        this.criteria = criteria;
    }

    public String getTableName() {
        return tableName;
    }

    public Date getDate() {
        return date;
    }

    public List<Field> getFields() {
        return fields;
    }

    public List<FieldSearchCriteria> getFieldFilter() {
        return fieldFilter;
    }

    public String getCommonFilter() {
        return commonFilter;
    }

    public Criteria getCriteria() {
        return criteria;
    }
}

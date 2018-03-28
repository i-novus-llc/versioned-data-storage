package ru.i_novus.platform.datastorage.temporal.model.criteria;

import net.n2oapp.criteria.api.Criteria;
import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.util.Date;
import java.util.List;

public class DataCriteria extends Criteria {
    private final String tableName;
    private final Date bdate;
    private final Date edate;
    private final List<Field> fields;
    private final List<FieldSearchCriteria> fieldFilter;
    private final String commonFilter;

    /**
     * @param tableName    наименование таблицы
     * @param bdate         дата публикации версии
     * @param edate         дата создания версии
     * @param fields       список полей в ответе
     * @param fieldFilter  фильтр по отдельным полям
     * @param commonFilter фильтр по всем полям
     */
    public DataCriteria(String tableName, Date bdate, Date edate, List<Field> fields, List<FieldSearchCriteria> fieldFilter, String commonFilter) {
        this.tableName = tableName;
        this.bdate = bdate;
        this.edate = edate;
        this.fields = fields;
        this.fieldFilter = fieldFilter;
        this.commonFilter = commonFilter;
    }

    public String getTableName() {
        return tableName;
    }

    public Date getBdate() {
        return bdate;
    }

    public Date getEdate() {
        return edate;
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
}

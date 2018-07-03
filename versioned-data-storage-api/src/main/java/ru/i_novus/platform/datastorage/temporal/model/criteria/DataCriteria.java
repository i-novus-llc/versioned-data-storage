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
     * @param storageCode    наименование таблицы
     * @param bdate         дата публикации версии
     * @param edate         дата создания версии
     * @param fields       список полей в ответе
     * @param fieldFilter  фильтр по отдельным полям
     * @param commonFilter фильтр по всем полям
     */
    public DataCriteria(String storageCode, Date bdate, Date edate, List<Field> fields, List<FieldSearchCriteria> fieldFilter, String commonFilter) {
        this.tableName = storageCode;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataCriteria criteria = (DataCriteria) o;

        if (tableName != null ? !tableName.equals(criteria.tableName) : criteria.tableName != null) return false;
        if (bdate != null ? !bdate.equals(criteria.bdate) : criteria.bdate != null) return false;
        if (edate != null ? !edate.equals(criteria.edate) : criteria.edate != null) return false;
        if (fields != null ? !fields.equals(criteria.fields) : criteria.fields != null) return false;
        if (fieldFilter != null ? !fieldFilter.equals(criteria.fieldFilter) : criteria.fieldFilter != null)
            return false;
        return !(commonFilter != null ? !commonFilter.equals(criteria.commonFilter) : criteria.commonFilter != null);

    }

    @Override
    public int hashCode() {
        int result = tableName != null ? tableName.hashCode() : 0;
        result = 31 * result + (bdate != null ? bdate.hashCode() : 0);
        result = 31 * result + (edate != null ? edate.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        result = 31 * result + (fieldFilter != null ? fieldFilter.hashCode() : 0);
        result = 31 * result + (commonFilter != null ? commonFilter.hashCode() : 0);
        return result;
    }
}

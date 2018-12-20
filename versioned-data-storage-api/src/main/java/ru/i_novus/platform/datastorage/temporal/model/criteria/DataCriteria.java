package ru.i_novus.platform.datastorage.temporal.model.criteria;

import net.n2oapp.criteria.api.Criteria;
import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DataCriteria extends Criteria {
    private final String tableName;
    private final Date bdate;
    private final Date edate;
    private final List<Field> fields;
    private Set<List<FieldSearchCriteria>> fieldFilter;
    private String commonFilter;
    private List<String> hashList;

    /**
     * @param storageCode  наименование таблицы
     * @param bdate        дата публикации версии
     * @param edate        дата создания версии
     * @param fields       список полей в ответе
     * @param fieldFilter  фильтр по отдельным полям
     * @param commonFilter фильтр по всем полям
     */
    public DataCriteria(String storageCode, Date bdate, Date edate, List<Field> fields, List<FieldSearchCriteria> fieldFilter, String commonFilter) {
        this(storageCode, bdate, edate, fields,
                new HashSet<List<FieldSearchCriteria>>() {{
                    add(fieldFilter);
                }},
                commonFilter);
    }

    /**
     * @param storageCode   наименование таблицы
     * @param bdate         дата публикации версии
     * @param edate         дата создания версии
     * @param fields        список полей в ответе
     * @param fieldFilter множество фильтров по отдельным полям
     * @param commonFilter  фильтр по всем полям
     */
    public DataCriteria(String storageCode, Date bdate, Date edate, List<Field> fields, Set<List<FieldSearchCriteria>> fieldFilter, String commonFilter) {
        this.tableName = storageCode;
        this.bdate = bdate;
        this.edate = edate;
        this.fields = fields;
        this.fieldFilter = fieldFilter;
        this.commonFilter = commonFilter;
    }

    /**
     * @param storageCode   наименование таблицы
     * @param bdate         дата публикации версии
     * @param edate         дата создания версии
     * @param fields        список полей в ответе
     * @param hashList      хеши записей
     */
    public DataCriteria(String storageCode, Date bdate, Date edate, List<Field> fields, List<String> hashList) {
        this.tableName = storageCode;
        this.bdate = bdate;
        this.edate = edate;
        this.fields = fields;
        this.hashList = hashList;
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

    public Set<List<FieldSearchCriteria>> getFieldFilter() {
        return fieldFilter;
    }

    public String getCommonFilter() {
        return commonFilter;
    }

    public List<String> getHashList() {
        return hashList;
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
        if (fieldFilter != null ? !fieldFilter.equals(criteria.fieldFilter) : criteria.fieldFilter != null) return false;
        if (commonFilter != null ? !commonFilter.equals(criteria.commonFilter) : criteria.commonFilter != null) return false;
        return hashList != null ? hashList.equals(criteria.hashList) : criteria.hashList == null;
    }

    @Override
    public int hashCode() {
        int result = tableName != null ? tableName.hashCode() : 0;
        result = 31 * result + (bdate != null ? bdate.hashCode() : 0);
        result = 31 * result + (edate != null ? edate.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        result = 31 * result + (fieldFilter != null ? fieldFilter.hashCode() : 0);
        result = 31 * result + (commonFilter != null ? commonFilter.hashCode() : 0);
        result = 31 * result + (hashList != null ? hashList.hashCode() : 0);
        return result;
    }
}

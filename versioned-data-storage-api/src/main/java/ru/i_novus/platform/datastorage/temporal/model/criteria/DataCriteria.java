package ru.i_novus.platform.datastorage.temporal.model.criteria;

import net.n2oapp.criteria.api.Criteria;
import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

// add StorageDataCriteria with storageCode, fields, fieldFilter and statics
public class DataCriteria extends Criteria {

    public static final int MIN_PAGE = 1;
    public static final int MIN_SIZE = 1;
    public static final int NO_PAGINATION_PAGE = 0;
    public static final int NO_PAGINATION_SIZE = 0;

    private final String tableName;
    private final LocalDateTime bdate;
    private final LocalDateTime edate;
    private final List<Field> fields;

    private Set<List<FieldSearchCriteria>> fieldFilters;
    private List<Long> systemIds;
    private String commonFilter;
    private List<String> hashList;

    /**
     * @param storageCode   наименование таблицы
     * @param bdate         дата публикации версии
     * @param edate         дата создания версии
     * @param fields        список полей в ответе
     */
    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields) {
        this.tableName = storageCode;
        this.bdate = bdate;
        this.edate = edate;
        this.fields = fields;
    }

    /**
     * @param storageCode   наименование таблицы
     * @param bdate         дата публикации версии
     * @param edate         дата создания версии
     * @param fields        список полей в ответе
     * @param hashList      хеши записей
     */
    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                        List<String> hashList) {
        this(storageCode, bdate, edate, fields);

        this.hashList = hashList;
    }

    /**
     * @param storageCode   наименование таблицы
     * @param bdate         дата публикации версии
     * @param edate         дата создания версии
     * @param fields        список полей в ответе
     * @param fieldFilters   множество фильтров по отдельным полям
     * @param commonFilter  фильтр по всем полям
     */
    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                        Set<List<FieldSearchCriteria>> fieldFilters, String commonFilter) {
        this(storageCode, bdate, edate, fields);

        this.fieldFilters = fieldFilters;
        this.commonFilter = commonFilter;
    }

    /**
     * @param storageCode  наименование таблицы
     * @param bdate        дата публикации версии
     * @param edate        дата создания версии
     * @param fields       список полей в ответе
     * @param fieldFilters  фильтр по отдельным полям
     * @param commonFilter фильтр по всем полям
     */
    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                        List<FieldSearchCriteria> fieldFilters, String commonFilter) {
        this(storageCode, bdate, edate, fields);

        this.fieldFilters = new HashSet<>();
        this.fieldFilters.add(fieldFilters);
        this.commonFilter = commonFilter;
    }

    /**
     * @param storageCode   наименование таблицы
     * @param bdate         дата публикации версии
     * @param edate         дата создания версии
     * @param fields        список полей в ответе
     * @param fieldFilters  множество фильтров по отдельным полям
     * @param commonFilter  фильтр по всем полям
     * @param systemIds     фильтр по системным идентификаторам строк
     */
    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                        Set<List<FieldSearchCriteria>> fieldFilters, String commonFilter, List<Long> systemIds) {
        this(storageCode, bdate, edate, fields, fieldFilters, commonFilter);

        this.systemIds = systemIds;
    }

    public String getTableName() {
        return tableName;
    }

    public LocalDateTime getBdate() {
        return bdate;
    }

    public LocalDateTime getEdate() {
        return edate;
    }

    public List<Field> getFields() {
        return fields;
    }

    public Set<List<FieldSearchCriteria>> getFieldFilters() {
        return fieldFilters;
    }

    public List<Long> getSystemIds() {
        return systemIds;
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

        if (!Objects.equals(tableName, criteria.tableName)) return false;
        if (!Objects.equals(bdate, criteria.bdate)) return false;
        if (!Objects.equals(edate, criteria.edate)) return false;
        if (!Objects.equals(fields, criteria.fields)) return false;
        if (!Objects.equals(fieldFilters, criteria.fieldFilters)) return false;
        if (!Objects.equals(commonFilter, criteria.commonFilter)) return false;
        if (!Objects.equals(systemIds, criteria.systemIds)) return false;
        return Objects.equals(hashList, criteria.hashList);
    }

    @Override
    public int hashCode() {
        int result = tableName != null ? tableName.hashCode() : 0;
        result = 31 * result + (bdate != null ? bdate.hashCode() : 0);
        result = 31 * result + (edate != null ? edate.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        result = 31 * result + (fieldFilters != null ? fieldFilters.hashCode() : 0);
        result = 31 * result + (systemIds != null ? systemIds.hashCode() : 0);
        result = 31 * result + (commonFilter != null ? commonFilter.hashCode() : 0);
        result = 31 * result + (hashList != null ? hashList.hashCode() : 0);
        return result;
    }
}

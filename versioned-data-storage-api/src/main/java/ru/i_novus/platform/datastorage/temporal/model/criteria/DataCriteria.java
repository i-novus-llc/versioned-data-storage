package ru.i_novus.platform.datastorage.temporal.model.criteria;

import net.n2oapp.criteria.api.Criteria;
import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.time.LocalDateTime;
import java.util.*;

public class DataCriteria extends Criteria {

    public static final int MIN_PAGE = 1;
    public static final int MIN_SIZE = 1;
    public static final int NO_PAGINATION_PAGE = 0;
    public static final int NO_PAGINATION_SIZE = 0;

    /** Код хранилища. */
    private final String storageCode;

    /** Дата публикации записей. */
    private final LocalDateTime bdate;

    /** Дата прекращения действия записей. */
    private final LocalDateTime edate;

    /** Список требуемых полей в результате. */
    private final List<Field> fields;

    /** Множество фильтров по отдельным полям. */
    private Set<List<FieldSearchCriteria>> fieldFilters;

    /** Общее условие поиска (с использованием FTS). */
    private String commonFilter;

    /** Список хешей записей. */
    private List<String> hashList;
    /** Список системных идентификаторов записей. */
    private List<Long> systemIds;

    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields) {

        this.storageCode = storageCode;
        this.bdate = bdate;
        this.edate = edate;
        this.fields = fields;
    }

    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                        Set<List<FieldSearchCriteria>> fieldFilters, String commonFilter) {
        this(storageCode, bdate, edate, fields);

        this.fieldFilters = fieldFilters;
        this.commonFilter = commonFilter;
    }

    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                        List<FieldSearchCriteria> fieldFilters, String commonFilter) {
        this(storageCode, bdate, edate, fields);

        this.fieldFilters = new HashSet<>();
        this.fieldFilters.add(fieldFilters);
        this.commonFilter = commonFilter;
    }

    public DataCriteria(DataCriteria criteria) {

        super(criteria);

        this.storageCode = criteria.storageCode;
        this.bdate = criteria.bdate;
        this.edate = criteria.edate;
        this.fields = criteria.fields;

        this.fieldFilters = criteria.fieldFilters;
        this.commonFilter = criteria.commonFilter;

        this.hashList = criteria.hashList;
        this.systemIds = criteria.systemIds;
    }

    public String getStorageCode() {
        return storageCode;
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

    public void setFieldFilters(Set<List<FieldSearchCriteria>> fieldFilters) {
        this.fieldFilters = fieldFilters;
    }

    public String getCommonFilter() {
        return commonFilter;
    }

    public void setCommonFilter(String commonFilter) {
        this.commonFilter = commonFilter;
    }

    public List<String> getHashList() {
        return hashList;
    }

    public void setHashList(List<String> hashList) {
        this.hashList = hashList;
    }

    public List<Long> getSystemIds() {
        return systemIds;
    }

    public void setSystemIds(List<Long> systemIds) {
        this.systemIds = systemIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataCriteria that = (DataCriteria) o;
        return Objects.equals(storageCode, that.storageCode) &&
                Objects.equals(bdate, that.bdate) &&
                Objects.equals(edate, that.edate) &&
                Objects.equals(fields, that.fields) &&

                Objects.equals(fieldFilters, that.fieldFilters) &&
                Objects.equals(commonFilter, that.commonFilter) &&
                Objects.equals(hashList, that.hashList) &&
                Objects.equals(systemIds, that.systemIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageCode, bdate, edate, fields,
                fieldFilters, commonFilter, hashList, systemIds);
    }
}

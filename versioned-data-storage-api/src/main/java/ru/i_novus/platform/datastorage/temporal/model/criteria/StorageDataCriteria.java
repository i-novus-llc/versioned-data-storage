package ru.i_novus.platform.datastorage.temporal.model.criteria;

import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.time.LocalDateTime;
import java.util.*;

import static ru.i_novus.platform.datastorage.temporal.util.StorageUtils.*;

/** Критерий поиска данных в хранилище. */
public class StorageDataCriteria extends DataCriteria {

    /** Наименование схемы. */
    private final String schemaName;

    /** Наименование таблицы. */
    private final String tableName;

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

    public StorageDataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields) {

        this.schemaName = toSchemaName(storageCode);
        this.tableName = toTableName(storageCode);
        this.bdate = bdate;
        this.edate = edate;
        this.fields = fields;
    }

    public StorageDataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                               Set<List<FieldSearchCriteria>> fieldFilters, String commonFilter) {
        this(storageCode, bdate, edate, fields);

        this.fieldFilters = fieldFilters;
        this.commonFilter = commonFilter;
    }

    public StorageDataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                               List<FieldSearchCriteria> fieldFilters, String commonFilter) {
        this(storageCode, bdate, edate, fields);

        this.fieldFilters = new HashSet<>();
        this.fieldFilters.add(fieldFilters);
        this.commonFilter = commonFilter;
    }

    public StorageDataCriteria(StorageDataCriteria criteria) {

        super(criteria);

        this.schemaName = criteria.schemaName;
        this.tableName = criteria.tableName;
        this.bdate = criteria.bdate;
        this.edate = criteria.edate;
        this.fields = criteria.fields;

        this.fieldFilters = criteria.fieldFilters;
        this.commonFilter = criteria.commonFilter;
        this.hashList = criteria.hashList;
        this.systemIds = criteria.systemIds;
    }

    public String getSchemaName() {
        return schemaName;
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

    public String getStorageCode() {
        return toStorageCode(schemaName, tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StorageDataCriteria that = (StorageDataCriteria) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
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
        return Objects.hash(schemaName, tableName, bdate, edate, fields,
                fieldFilters, commonFilter, hashList, systemIds);
    }
}

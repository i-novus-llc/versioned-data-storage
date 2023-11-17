package ru.i_novus.platform.datastorage.temporal.model.criteria;

import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyList;

/** Критерий поиска данных в хранилище. */
@SuppressWarnings("java:S3740")
public class StorageDataCriteria extends DataCriteria {

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

    public StorageDataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields) {

        this.storageCode = storageCode;
        this.bdate = bdate;
        this.edate = edate;
        this.fields = fields != null ? fields : emptyList();
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
        if(!super.equals(o)) return false;

        StorageDataCriteria that = (StorageDataCriteria) o;
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
        return Objects.hash(super.hashCode(),
                storageCode, bdate, edate, fields,
                fieldFilters, commonFilter, hashList, systemIds);
    }

    @Override
    public String toString() {
        return "StorageDataCriteria{" +
                "storageCode='" + storageCode + '\'' +
                ", bdate=" + bdate +
                ", edate=" + edate +
                ", fields=" + fields +

                ", fieldFilters=" + fieldFilters +
                ", commonFilter='" + commonFilter + '\'' +
                ", hashList=" + hashList +
                ", systemIds=" + systemIds +
                "} " +
                super.toString();
    }
}

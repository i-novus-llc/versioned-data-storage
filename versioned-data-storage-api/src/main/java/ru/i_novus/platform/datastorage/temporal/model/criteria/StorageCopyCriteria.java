package ru.i_novus.platform.datastorage.temporal.model.criteria;

import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

import static ru.i_novus.platform.datastorage.temporal.util.StorageUtils.*;

/** Критерий копирования данных в хранилище. */
public class StorageCopyCriteria extends StorageDataCriteria {

    /** Наименование схемы назначения. */
    private final String purposeSchemaName;

    /** Наименование таблицы назначения. */
    private final String purposeTableName;

    /** Экранированные наименования полей для копирования. */
    private List<String> escapedFieldNames;

    public StorageCopyCriteria(String storageCode, String purposeCode,
                               LocalDateTime bdate, LocalDateTime edate, List<Field> fields) {

        super(storageCode, bdate, edate, fields);

        this.purposeSchemaName = toSchemaName(purposeCode);
        this.purposeTableName = toTableName(purposeCode);
    }

    public StorageCopyCriteria(StorageCopyCriteria criteria) {

        super(criteria);

        this.purposeSchemaName = criteria.purposeSchemaName;
        this.purposeTableName = criteria.purposeTableName;
    }

    public String getPurposeSchemaName() {
        return purposeSchemaName;
    }

    public String getPurposeTableName() {
        return purposeTableName;
    }

    public List<String> getEscapedFieldNames() {
        return escapedFieldNames;
    }

    public void setEscapedFieldNames(List<String> escapedFieldNames) {
        this.escapedFieldNames = escapedFieldNames;
    }

    public String getPurposeCode() {
        return toStorageCode(purposeSchemaName, purposeTableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        StorageCopyCriteria that = (StorageCopyCriteria) o;
        return Objects.equals(purposeSchemaName, that.purposeSchemaName) &&
                Objects.equals(purposeTableName, that.purposeTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), purposeSchemaName, purposeTableName);
    }
}

package ru.i_novus.platform.datastorage.temporal.model.criteria;

import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

/** Запрос на копирование данных в хранилище. */
public class StorageCopyRequest extends StorageDataCriteria {

    /** Код хранилища назначения. */
    private final String purposeCode;

    /** Экранированные наименования полей для копирования. */
    private List<String> escapedFieldNames;

    public StorageCopyRequest(String storageCode, String purposeCode,
                              LocalDateTime bdate, LocalDateTime edate, List<Field> fields) {

        super(storageCode, bdate, edate, fields);

        this.purposeCode = purposeCode;
    }

    public StorageCopyRequest(StorageCopyRequest criteria) {

        super(criteria);

        this.purposeCode = criteria.purposeCode;
        this.escapedFieldNames = criteria.escapedFieldNames;
    }

    public String getPurposeCode() {
        return purposeCode;
    }

    public List<String> getEscapedFieldNames() {
        return escapedFieldNames;
    }

    public void setEscapedFieldNames(List<String> escapedFieldNames) {
        this.escapedFieldNames = escapedFieldNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        StorageCopyRequest that = (StorageCopyRequest) o;
        return Objects.equals(purposeCode, that.purposeCode) &&
                Objects.equals(escapedFieldNames, that.escapedFieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), purposeCode, escapedFieldNames);
    }
}

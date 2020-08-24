package ru.i_novus.platform.datastorage.temporal.model.criteria;

import ru.i_novus.platform.datastorage.temporal.enums.DiffReturnTypeEnum;
import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

/** Критерий сравнения данных в хранилищах. */
public class CompareDataCriteria extends DataCriteria {

    private String storageCode;
    private String newStorageCode;

    private List<Field> fields;
    private List<String> primaryFields;
    private Set<List<FieldSearchCriteria>> primaryFieldsFilters;

    private LocalDateTime oldPublishDate;
    private LocalDateTime oldCloseDate;
    private LocalDateTime newPublishDate;
    private LocalDateTime newCloseDate;

    private DiffStatusEnum status;
    private Boolean countOnly;
    private DiffReturnTypeEnum returnType; //default ALL

    public CompareDataCriteria() {
        // Nothing to do.
    }

    public CompareDataCriteria(String storageCode, String newStorageCode,
                               List<Field> fields, List<String> primaryFields,
                               Set<List<FieldSearchCriteria>> primaryFieldsFilters) {
        this.storageCode = storageCode;
        this.newStorageCode = newStorageCode;

        this.fields = fields;
        this.primaryFields = primaryFields;
        this.primaryFieldsFilters = primaryFieldsFilters;
    }

    public String getStorageCode() {
        return storageCode;
    }

    public void setStorageCode(String storageCode) {
        this.storageCode = storageCode;
    }

    public String getNewStorageCode() {
        return newStorageCode;
    }

    public void setNewStorageCode(String newStorageCode) {
        this.newStorageCode = newStorageCode;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public List<String> getPrimaryFields() {
        return primaryFields;
    }

    public void setPrimaryFields(List<String> primaryFields) {
        this.primaryFields = primaryFields;
    }

    public Set<List<FieldSearchCriteria>> getPrimaryFieldsFilters() {
        return primaryFieldsFilters;
    }

    public void setPrimaryFieldsFilters(Set<List<FieldSearchCriteria>> primaryFieldsFilters) {
        this.primaryFieldsFilters = primaryFieldsFilters;
    }

    public LocalDateTime getOldPublishDate() {
        return oldPublishDate;
    }

    public void setOldPublishDate(LocalDateTime oldPublishDate) {
        this.oldPublishDate = oldPublishDate;
    }

    public LocalDateTime getOldCloseDate() {
        return oldCloseDate;
    }

    public void setOldCloseDate(LocalDateTime oldCloseDate) {
        this.oldCloseDate = oldCloseDate;
    }

    public LocalDateTime getNewPublishDate() {
        return newPublishDate;
    }

    public void setNewPublishDate(LocalDateTime newPublishDate) {
        this.newPublishDate = newPublishDate;
    }

    public LocalDateTime getNewCloseDate() {
        return newCloseDate;
    }

    public void setNewCloseDate(LocalDateTime newCloseDate) {
        this.newCloseDate = newCloseDate;
    }

    public DiffStatusEnum getStatus() {
        return status;
    }

    public void setStatus(DiffStatusEnum status) {
        this.status = status;
    }

    public Boolean getCountOnly() {
        return countOnly;
    }

    public void setCountOnly(Boolean countOnly) {
        this.countOnly = countOnly;
    }

    public DiffReturnTypeEnum getReturnType() {
        return returnType != null ? returnType : DiffReturnTypeEnum.ALL;
    }

    public void setReturnType(DiffReturnTypeEnum returnType) {
        this.returnType = returnType;
    }
}


package ru.i_novus.platform.datastorage.temporal.model.criteria;

import net.n2oapp.criteria.api.Criteria;
import ru.i_novus.platform.datastorage.temporal.enums.DiffReturnTypeEnum;
import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

public class CompareDataCriteria extends Criteria {
    private String storageCode;
    private String draftCode;
    private String newStorageCode;
    private LocalDateTime baseDataDate;
    private LocalDateTime targetDataDate;
    private LocalDateTime oldPublishDate;
    private LocalDateTime oldCloseDate;
    private LocalDateTime newPublishDate;
    private LocalDateTime newCloseDate;
    private List<Field> fields;
    private List<String> primaryFields;
    private DiffStatusEnum status;
    private Boolean countOnly;
    private DiffReturnTypeEnum returnType; //default ALL
    private Set<List<FieldSearchCriteria>> primaryFieldsFilters;

    public CompareDataCriteria() {
    }

    public CompareDataCriteria(String storageCode, String newStorageCode, LocalDateTime oldPublishDate, LocalDateTime oldCloseDate, LocalDateTime newPublishDate, LocalDateTime newCloseDate, List<Field> fields, List<String> primaryFields, Set<List<FieldSearchCriteria>> primaryFieldsFilters) {
        this.storageCode = storageCode;
        this.newStorageCode = newStorageCode;
        this.oldPublishDate = oldPublishDate;
        this.oldCloseDate = oldCloseDate;
        this.newPublishDate = newPublishDate;
        this.newCloseDate = newCloseDate;
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

    /*
     * use setNewStorageCode() method
     */
    @Deprecated
    public void setDraftCode(String draftCode) {
        this.draftCode = draftCode;
        this.newStorageCode = draftCode;
    }

    public String getNewStorageCode() {
        return newStorageCode;
    }

    public void setNewStorageCode(String newStorageCode) {
        this.newStorageCode = newStorageCode;
    }

    /*
     * use setOldPublishDate(), setOldCloseDate() methods
     */
    @Deprecated
    public void setBaseDataDate(LocalDateTime baseDataDate) {
        this.baseDataDate = baseDataDate;
        this.oldPublishDate = baseDataDate;
        this.oldCloseDate = baseDataDate;
    }

    /*
     * use setNewPublishDate(), setNewCloseDate() methods
     */
    @Deprecated
    public void setTargetDataDate(LocalDateTime targetDataDate) {
        this.targetDataDate = targetDataDate;
        this.newPublishDate = targetDataDate;
        this.newCloseDate = targetDataDate;
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

    public Set<List<FieldSearchCriteria>> getPrimaryFieldsFilters() {
        return primaryFieldsFilters;
    }

    public void setPrimaryFieldsFilters(Set<List<FieldSearchCriteria>> primaryFieldsFilters) {
        this.primaryFieldsFilters = primaryFieldsFilters;
    }
}


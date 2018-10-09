package ru.i_novus.platform.datastorage.temporal.model.criteria;

import net.n2oapp.criteria.api.Criteria;
import ru.i_novus.platform.datastorage.temporal.enums.DiffReturnTypeEnum;
import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.util.Date;
import java.util.List;
import java.util.Set;

public class CompareDataCriteria extends Criteria {
    private String storageCode;
    private String draftCode;
    private String newStorageCode;
    private Date baseDataDate;
    private Date targetDataDate;
    private Date oldPublishDate;
    private Date oldCloseDate;
    private Date newPublishDate;
    private Date newCloseDate;
    private List<Field> fields;
    private List<String> primaryFields;
    private DiffStatusEnum status;
    private Boolean countOnly;
    private DiffReturnTypeEnum returnType; //default ALL
    private Set<List<FieldSearchCriteria>> primaryFieldsFilters;

    public CompareDataCriteria() {
    }

    public CompareDataCriteria(String storageCode, String newStorageCode, Date oldPublishDate, Date oldCloseDate, Date newPublishDate, Date newCloseDate, List<Field> fields, List<String> primaryFields, Set<List<FieldSearchCriteria>> primaryFieldsFilters) {
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
    public void setBaseDataDate(Date baseDataDate) {
        this.baseDataDate = baseDataDate;
        this.oldPublishDate = baseDataDate;
        this.oldCloseDate = baseDataDate;
    }

    /*
     * use setNewPublishDate(), setNewCloseDate() methods
     */
    @Deprecated
    public void setTargetDataDate(Date targetDataDate) {
        this.targetDataDate = targetDataDate;
        this.newPublishDate = targetDataDate;
        this.newCloseDate = targetDataDate;
    }

    public Date getOldPublishDate() {
        return oldPublishDate;
    }

    public void setOldPublishDate(Date oldPublishDate) {
        this.oldPublishDate = oldPublishDate;
    }

    public Date getOldCloseDate() {
        return oldCloseDate;
    }

    public void setOldCloseDate(Date oldCloseDate) {
        this.oldCloseDate = oldCloseDate;
    }

    public Date getNewPublishDate() {
        return newPublishDate;
    }

    public void setNewPublishDate(Date newPublishDate) {
        this.newPublishDate = newPublishDate;
    }

    public Date getNewCloseDate() {
        return newCloseDate;
    }

    public void setNewCloseDate(Date newCloseDate) {
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


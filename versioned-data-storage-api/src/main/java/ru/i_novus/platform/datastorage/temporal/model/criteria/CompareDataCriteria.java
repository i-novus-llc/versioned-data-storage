package ru.i_novus.platform.datastorage.temporal.model.criteria;

import net.n2oapp.criteria.api.Criteria;
import ru.i_novus.platform.datastorage.temporal.enums.DiffReturnTypeEnum;
import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.util.Date;
import java.util.List;

public class CompareDataCriteria extends Criteria {
    private String storageCode;
    private String draftCode;
    private Date baseDataDate;
    private Date targetDataDate;
    private List<String> primaryFields;
    private List<Field> fields;
    private List<Field> targetFields;
    private DiffStatusEnum status;
    private Boolean countOnly;
    private DiffReturnTypeEnum returnType; //default ALL

    public String getStorageCode() {
        return storageCode;
    }

    public void setStorageCode(String storageCode) {
        this.storageCode = storageCode;
    }

    public Date getBaseDataDate() {
        return baseDataDate;
    }

    public void setBaseDataDate(Date baseDataDate) {
        this.baseDataDate = baseDataDate;
    }

    public Date getTargetDataDate() {
        return targetDataDate;
    }

    public void setTargetDataDate(Date targetDataDate) {
        this.targetDataDate = targetDataDate;
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

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public List<Field> getTargetFields() {
        return targetFields;
    }

    public void setTargetFields(List<Field> targetFields) {
        this.targetFields = targetFields;
    }

    public String getDraftCode() {
        return draftCode;
    }

    public void setDraftCode(String draftCode) {
        this.draftCode = draftCode;
    }

    public DiffReturnTypeEnum getReturnType() {
        return returnType != null ? returnType : DiffReturnTypeEnum.ALL;
    }

    public void setReturnType(DiffReturnTypeEnum returnType) {
        this.returnType = returnType;
    }
}


package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.platform.datastorage.temporal.exception.ListCodifiedException;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.BooleanField;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.TreeField;
import ru.kirkazan.common.exception.CodifiedException;

import javax.persistence.PersistenceException;
import javax.transaction.Transactional;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

import static ru.i_novus.platform.versioned_data_storage.pg_impl.ExceptionCodes.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.service.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.addDoubleQuotes;

/**
 * @author lgalimova
 * @since 22.03.2018
 */

public class DraftDataServiceImpl implements DraftDataService {

    private static final Logger logger = LoggerFactory.getLogger(DraftDataServiceImpl.class);
    private DataDao dataDao;

    public DraftDataServiceImpl(DataDao dataDao) {
        this.dataDao = dataDao;
    }

    @Override
    @Transactional
    public String createDraft(List<Field> fields) {
        String draftCode = UUID.randomUUID().toString();
        createTable(draftCode, fields, true);
        return draftCode;
    }

    @Override
    public String applyDraft(String sourceStorageCode, String draftCode, Date publishTime) {
        String newTable = createVersionTable(draftCode);
        if (sourceStorageCode != null && dataDao.tableStructureEquals(sourceStorageCode, draftCode)) {
            insertActualDataFromVersion(sourceStorageCode, draftCode, newTable);
            insertOldDataFromVersion(sourceStorageCode, newTable);
            insertClosedNowDataFromVersion(sourceStorageCode, draftCode, newTable, publishTime);
            insertNewDataFromDraft(sourceStorageCode, draftCode, newTable, publishTime);
        } else {
            List<String> draftFields = dataDao.getFieldNames(draftCode);
            BigInteger count = dataDao.countData(draftCode);
            draftFields.add(addDoubleQuotes("FTS"));
            for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
                dataDao.insertDataFromDraft(draftCode, i, newTable, TRANSACTION_SIZE, publishTime, draftFields);
            }
        }
        return newTable;
    }

    @Override
    public void addRows(String draftCode, List<RowValue> data) {
        List<CodifiedException> exceptions = new ArrayList<>();
        //validateRow

        if (!exceptions.isEmpty()) {
            throw new ListCodifiedException(exceptions);
        }
        dataDao.insertData(draftCode, data);
    }

    @Override
    public void deleteRows(String draftCode, List<Object> systemIds) {
        dataDao.deleteData(draftCode, systemIds);
    }

    @Override
    public void deleteAllRows(String draftCode) {
        dataDao.deleteData(draftCode);
    }

    @Override//
    public void updateRow(String draftCode, RowValue value) {
        List<CodifiedException> exceptions = new ArrayList<>();
//        validateRow(draftCode, value, exceptions);
        if (value.getSystemId() == null)
            exceptions.add(new CodifiedException(FIELD_IS_REQUIRED_EXCEPTION_CODE, DATA_PRIMARY_COLUMN));

        if (exceptions.size() != 0) {
            throw new ListCodifiedException(exceptions);
        }
        dataDao.updateData(draftCode, value);
    }

    @Override
    public void loadData(String draftCode, String sourceStorageCode, Date onDate) {
        List<String> draftFields = dataDao.getFieldNames(draftCode);
        Collections.sort(draftFields);
        List<String> sourceFields = dataDao.getFieldNames(draftCode);
        Collections.sort(sourceFields);
        if (!draftFields.equals(sourceFields)) {
            throw new CodifiedException(TABLES_NOT_EQUAL);
        }
        draftFields.add(addDoubleQuotes(DATA_PRIMARY_COLUMN));
        draftFields.add(addDoubleQuotes(FULL_TEXT_SEARCH));
        draftFields.add(addDoubleQuotes(SYS_HASH));
        dataDao.loadData(draftCode, sourceStorageCode, draftFields, onDate);
        dataDao.updateSequence(draftCode);
    }

    @Transactional
    @Override
    public void addField(String draftCode, Field field) {
        if (SYS_RECORDS.contains(field.getName()))
            throw new CodifiedException(SYS_FIELD_CONFLICT);
        if (dataDao.getFieldNames(draftCode).contains(field.getName()))
            throw new CodifiedException(COLUMN_ALREADY_EXISTS);
        dataDao.dropTrigger(draftCode);
        String defaultValue = (field instanceof BooleanField)? "false" : null;
        dataDao.addColumnToTable(draftCode, field.getName(), field.getType(), defaultValue);
        dataDao.createTrigger(draftCode);
    }

    @Transactional
    @Override
    public void deleteField(String draftCode, String fieldName) {
        if (!dataDao.getFieldNames(draftCode).contains(addDoubleQuotes(fieldName)))
            throw new CodifiedException(COLUMN_NOT_EXISTS);
        dataDao.dropTrigger(draftCode);
        dataDao.deleteColumnFromTable(draftCode, fieldName);
        dataDao.createTrigger(draftCode);
    }

    @Transactional
    @Override
    public void updateField(String draftCode, Field field) {
        String oldType = dataDao.getFieldType(draftCode, field.getName());
        String newType = field.getType();
        if (oldType.equals(newType))
            return;
        try {
            dataDao.dropTrigger(draftCode);
            dataDao.alterDataType(draftCode, field.getName(), oldType, newType);
            dataDao.createTrigger(draftCode);
        } catch (PersistenceException pe) {
            throw new CodifiedException(INCOMPATIBLE_NEW_DATA_TYPE_EXCEPTION_CODE, pe, field.getName());
        }
    }

    @Override
    public boolean isFieldUnique(String storageCode, String fieldName, Date publishTime) {
        return dataDao.isFieldUnique(storageCode, fieldName, publishTime);
    }

    private void createTable(String draftCode, List<Field> fields, boolean isDraft) {
        logger.debug("creating table with name: {}", draftCode);
        if (isDraft) {
            dataDao.createDraftTable(draftCode, fields);
        } else {
            dataDao.createVersionTable(draftCode, fields);
        }
        List<String> fieldNames = fields.stream().map(f -> addDoubleQuotes(f.getName())).filter(f -> !QueryConstants.SYS_RECORDS.contains(f)).collect(Collectors.toList());
        dataDao.createHashIndex(draftCode);
        if (!fields.isEmpty()) {
            dataDao.createTrigger(draftCode, fieldNames);
            for (Field field : fields) {
                if (field instanceof TreeField)
                    dataDao.createLtreeIndex(draftCode, field.getName());
                else if (BooleanUtils.toBoolean(field.getSearchEnabled())) {
                    dataDao.createIndex(draftCode, field.getName());
                }
            }
        }
        dataDao.createFullTextSearchIndex(draftCode);
    }

    private String createVersionTable(String draftCode) {
        String newTable = UUID.randomUUID().toString();
        dataDao.copyTable(newTable, draftCode);
        dataDao.addColumnToTable(newTable, "SYS_PUBLISHTIME", "timestamp with time zone", null);
        dataDao.addColumnToTable(newTable, "SYS_CLOSETIME", "timestamp with time zone", null);
        List<String> fieldNames = dataDao.getFieldNames(newTable);
        dataDao.createTrigger(newTable, fieldNames);
        return newTable;
    }

    private void insertActualDataFromVersion(String actualVersionTable, String draftTable, String newTable) {
        BigInteger count = dataDao.countActualDataFromVersion(actualVersionTable, draftTable);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertActualDataFromVersion(newTable, actualVersionTable, draftTable, i, TRANSACTION_SIZE);
        }
    }


    private void insertOldDataFromVersion(String actualVersionTable, String newTable) {
        BigInteger count = dataDao.countOldDataFromVersion(actualVersionTable);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertOldDataFromVersion(newTable, actualVersionTable, i, TRANSACTION_SIZE);
        }
    }

    private void insertClosedNowDataFromVersion(String actualVersionTable, String draftTable, String newTable, Date publishTime) {
        BigInteger count = dataDao.countClosedNowDataFromVersion(actualVersionTable, draftTable);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertClosedNowDataFromVersion(newTable, actualVersionTable, draftTable, i, TRANSACTION_SIZE, publishTime);
        }
    }

    private void insertNewDataFromDraft(String actualVersionTable, String draftTable, String newTable, Date publishTime) {
        BigInteger count = dataDao.countNewValFromDraft(draftTable, actualVersionTable);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertNewDataFromDraft(newTable, actualVersionTable, draftTable, i, TRANSACTION_SIZE, publishTime);
        }
    }
}

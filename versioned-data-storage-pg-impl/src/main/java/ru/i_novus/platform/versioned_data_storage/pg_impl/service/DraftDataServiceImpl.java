package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.platform.datastorage.temporal.exception.ListCodifiedException;
import ru.i_novus.platform.datastorage.temporal.model.*;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.model.value.TreeFieldValue;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;
import ru.kirkazan.common.exception.CodifiedException;

import javax.persistence.PersistenceException;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

import static ru.i_novus.platform.versioned_data_storage.pg_impl.ExceptionCodes.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.service.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.addEscapeCharacters;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.isCompatibleTypes;

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
    public String createDraft(List<Field> fields) {
        String draftCode = UUID.randomUUID().toString();
        createTable(draftCode, fields, true);
        return draftCode;
    }

    @Override
    public String applyDraft(String sourceStorageCode, String draftCode, Date publishTime) {
        List<String> draftFields = dataDao.getFieldNames(draftCode);
        List<String> sourceFields = dataDao.getFieldNames(sourceStorageCode);
        String newTable = createVersionTable(draftCode);
        if (sourceStorageCode != null && draftFields.equals(sourceFields)) {
            insertActualDataFromVersion(sourceStorageCode, draftCode, newTable);
            insertOldDataFromVersion(sourceStorageCode, newTable);
            insertClosedNowDataFromVersion(sourceStorageCode, draftCode, newTable, publishTime);
            insertNewDataFromDraft(sourceStorageCode, draftCode, newTable, publishTime);
        } else {
            BigInteger count = dataDao.countData(draftCode);
            draftFields.add(addEscapeCharacters("FTS"));
            for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
                dataDao.insertDataFromDraft(draftCode, i, newTable, TRANSACTION_SIZE, publishTime, draftFields);
            }
        }
        return newTable;
    }

    @Override
    public void addRows(String draftCode, List<RowValue> data) {
        List<CodifiedException> exceptions = new ArrayList<>();
        List<String> fieldNames = dataDao.getFieldNames(draftCode);
        String keys = fieldNames.stream().collect(Collectors.joining(","));
        List<String> values = new ArrayList<>();
        for (RowValue rowValue : data) {
//            validateRow(draftCode, rowValue, exceptions);
            List<String> rowValues = new ArrayList<>();
            for (Object fieldValueObj : rowValue.getFieldValues()) {
                FieldValue fieldValue = (FieldValue) fieldValueObj;
                if (fieldValue.getValue() == null) {
                    rowValues.add("null");
                } else if (fieldValue instanceof ReferenceFieldValue) {
                    rowValues.add("?\\:\\:jsonb");
                } else if (fieldValue instanceof TreeFieldValue) {
//                    rowValues.add("'" + fieldValue.getValue().toString() + "'");
                    rowValues.add("?\\:\\:ltree");
                } else {
                    rowValues.add("?");
                }
            }
            values.add(String.join(",", rowValues));
        }

        if (!exceptions.isEmpty()) {
            throw new ListCodifiedException(exceptions);
        }
        dataDao.insertData(draftCode, keys, values, data);
    }

    @Override
    public void deleteRows(String draftCode, List<Object> systemIds) {
        dataDao.deleteData(draftCode, systemIds);
    }

    @Override
    public void deleteAllRows(String draftCode) {
        dataDao.deleteData(draftCode);
    }

    @Override
    public void updateRow(String draftCode, RowValue value) {
        List<CodifiedException> exceptions = new ArrayList<>();
//        validateRow(draftCode, value, exceptions);
        List<String> keyList = new ArrayList<>();
        for (Object objectValue : value.getFieldValues()) {
            FieldValue fieldValue = (FieldValue) objectValue;
            String fieldName = fieldValue.getField();
            if (fieldValue.getValue() == null || fieldValue.getValue().equals("null")) {
                keyList.add(addEscapeCharacters(fieldName) + " = NULL");
            } else if (fieldValue instanceof ReferenceFieldValue) {
                keyList.add(addEscapeCharacters(fieldName) + " = ?\\:\\:jsonb");
            } else {
                keyList.add(addEscapeCharacters(fieldName) + " = ?");
            }
        }
        String keys = String.join(",", keyList);
        if (exceptions.size() != 0) {
            throw new ListCodifiedException(exceptions);
        }
        dataDao.updateData(draftCode, keys, value);
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
        draftFields.add(addEscapeCharacters(DATA_PRIMARY_COLUMN));
        draftFields.add(addEscapeCharacters(FULL_TEXT_SEARCH));
        draftFields.add(addEscapeCharacters(SYS_HASH));
        dataDao.loadData(draftCode, sourceStorageCode, draftFields, onDate);
        dataDao.updateSequence(draftCode);
    }

    @Override
    public void addField(String draftCode, Field field) {
        if (SYS_RECORDS.contains(field.getName()))
            throw new CodifiedException(SYS_FIELD_CONFLICT);
        if (dataDao.getFieldNames(draftCode).contains(field.getName()))
            throw new CodifiedException(COLUMN_ALREADY_EXISTS);
        dataDao.dropTrigger(draftCode);
        dataDao.addColumnToTable(draftCode, field.getName(), field.getType());
        dataDao.createTrigger(draftCode);
    }

    @Override
    public void deleteField(String draftCode, String fieldName) {
        if (!dataDao.getFieldNames(draftCode).contains(addEscapeCharacters(fieldName)))
            throw new CodifiedException(COLUMN_NOT_EXISTS);
        dataDao.dropTrigger(draftCode);
        dataDao.deleteColumnFromTable(draftCode, fieldName);
        dataDao.createTrigger(draftCode);
    }

    @Override
    public void updateField(String draftCode, Field field) {
        String oldType = dataDao.getFieldType(draftCode, field.getName());
        String newType = field.getType();
        boolean ifFieldIsNotEmpty = dataDao.ifFieldIsNotEmpty(draftCode, field.getName());
        if (ifFieldIsNotEmpty) {
            boolean isCompatible = isCompatibleTypes(oldType, newType);
            if (!isCompatible) {
                throw new CodifiedException(INCOMPATIBLE_NEW_DATA_TYPE_EXCEPTION_CODE, field.getName());
            }
        }
        try {
            dataDao.dropTrigger(draftCode);
            dataDao.alterDataType(draftCode, field.getName(), oldType, newType);
            dataDao.createTrigger(draftCode);
        } catch (PersistenceException pe) {
            throw new CodifiedException(INCOMPATIBLE_NEW_DATA_TYPE_EXCEPTION_CODE, field.getName());
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
        List<String> fieldNames = fields.stream().map(f -> addEscapeCharacters(f.getName())).filter(f -> !QueryConstants.SYS_RECORDS.contains(f)).collect(Collectors.toList());
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
        dataDao.addColumnToTable(newTable, "SYS_PUBLISHTIME", "timestamp with time zone");
        dataDao.addColumnToTable(newTable, "SYS_CLOSETIME", "timestamp with time zone");
        List<String> fieldNames = dataDao.getFieldNames(newTable);
        dataDao.createTrigger(newTable, fieldNames);
        return newTable;
    }
/*
    private void validateRow(String draftCode, RowValue row, List<CodifiedException> exceptions) {
        List<FieldValue> dataCopy = new ArrayList<>(row.getFieldValues());
        dataCopy.removeIf(v -> v.getValue() == null);
        if (dataCopy.size() == 0)
            throw new CodifiedException(EMPTY_RECORD_EXCEPTION_CODE);

        Date dateBegin = null;
        Date dateEnd = null;
        for (Object objectValue : row.getFieldValues()) {
            FieldValue fieldValue = (FieldValue) objectValue;
            String field = fieldValue.getField();
            if (DATE_BEGIN.equals(field))
                dateBegin = (Date) fieldValue.getValue();
            if (DATE_END.equals(field))
                dateEnd = (Date) fieldValue.getValue();
            if (BooleanUtils.toBoolean(field.getRequired()) && Util.isEmpty(fieldValue.getValue())) {
                exceptions.add(new CodifiedException(FIELD_IS_REQUIRED_EXCEPTION_CODE, field.getName()));
            } else {
                if (!(field instanceof DateField) && !(field instanceof BooleanField)) {
                    if (field instanceof ReferenceField) {
                        ObjectMapper mapper = new ObjectMapper();
                        try {
                            mapper.readValue(fieldValue.getValue().toString(), new TypeReference<Map<String, String>>() {
                            });
                        } catch (IOException e) {
                            exceptions.add(new CodifiedException(e.getMessage()));
                        }
                    }
                    if (fieldValue.getValue() != null && field.getMaxLength() != null && fieldValue.getValue().toString().length() > field.getMaxLength()) {
                        //todo выводить значение в текст опасно, оно может быть очень длинным. Плюс надо добавить в сообщение максимальную длину поля.
                        exceptions.add(new CodifiedException(INCORRECT_FIELD_LENGTH_EXCEPTION_CODE, field.getName(), fieldValue.getValue()));
                    }
                }
            }

            if (BooleanUtils.toBoolean(field.getUnique())) {
                if (Util.isEmpty(fieldValue.getValue())) {
                    exceptions.add(new CodifiedException(FIELD_IS_REQUIRED_EXCEPTION_CODE, field.getName()));
                } else {
                    List result = dataDao.getRowsByField(draftCode, field.getName(), fieldValue.getValue(),
                            dateBegin != null, dateBegin, dateEnd, row.getSystemId());
                    if (result.size() > 0) {
                        exceptions.add(new CodifiedException(DUPLICATE_UNIQUE_VALUE_EXCEPTION_CODE, fieldValue.getValue()));
                    }
                }
            }

        }
        if (dateBegin != null && dateEnd != null && dateBegin.after(dateEnd)) {
            exceptions.add(new CodifiedException(BEGIN_END_DATE_EXCEPTION_CODE));
        }
    } */


    protected void insertActualDataFromVersion(String actualVersionTable, String draftTable, String newTable) {
        BigInteger count = dataDao.countActualDataFromVersion(actualVersionTable, draftTable);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertActualDataFromVersion(newTable, actualVersionTable, draftTable, i, TRANSACTION_SIZE);
        }
    }


    protected void insertOldDataFromVersion(String actualVersionTable, String newTable) {
        BigInteger count = dataDao.countOldDataFromVersion(actualVersionTable);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertOldDataFromVersion(newTable, actualVersionTable, i, TRANSACTION_SIZE);
        }
    }

    protected void insertClosedNowDataFromVersion(String actualVersionTable, String draftTable, String newTable, Date publishTime) {
        BigInteger count = dataDao.countClosedNowDataFromVersion(actualVersionTable, draftTable);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertClosedNowDataFromVersion(newTable, actualVersionTable, draftTable, i, TRANSACTION_SIZE, publishTime);
        }
    }

    protected void insertNewDataFromDraft(String actualVersionTable, String draftTable, String newTable, Date publishTime) {
        BigInteger count = dataDao.countNewValFromDraft(draftTable, actualVersionTable);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertNewDataFromDraft(newTable, actualVersionTable, draftTable, i, TRANSACTION_SIZE, publishTime);
        }
    }

    public void setDataDao(DataDao dataDao) {
        this.dataDao = dataDao;
    }
}

package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.atria.common.lang.Util;
import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.platform.datastorage.temporal.exception.ListCodifiedException;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.BooleanField;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.DateField;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.ReferenceField;
import ru.kirkazan.common.exception.CodifiedException;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

import static ru.i_novus.platform.versioned_data_storage.pg_impl.ExceptionCodes.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.service.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.addEscapeCharacters;

/**
 * @author lgalimova
 * @since 22.03.2018
 */
public class DraftDataServiceImpl implements DraftDataService {

    private static final Logger logger = LoggerFactory.getLogger(DraftDataServiceImpl.class);
    private DataDao dataDao;

    @Override
    public String createDraft(List<Field> fields, List<RowValue> data) {
        String draftCode = createDraft(fields);
        List<String> valueList = new ArrayList<>();
        String keys = null;
        for (RowValue rowValue : data) {
            List<FieldValue> fieldValues = new ArrayList<>();
            for (Object value : rowValue.getFieldValues()) {
                fieldValues.add((FieldValue) value);
            }
            keys = fieldValues.stream().map(v -> addEscapeCharacters(v.getField().getName())).collect(Collectors.joining(",")) + ",\"FTS\"";
            String values = fieldValues.stream().map(v -> "?").collect(Collectors.joining(", ")) + "," + getFts(fieldValues);
            valueList.add(values);
        }
        dataDao.insertData(draftCode, keys, valueList, data);

        return draftCode;
    }

    @Override
    public String createDraft(List<Field> fields) {
        String draftCode = UUID.randomUUID().toString();
        createTable(draftCode, fields, true);
        return draftCode;
    }

    @Override
    public String applyDraft(String sourceStorageCode, String draftCode, Date publishTime) {
//        String newTable = UUID.randomUUID().toString();
//        List<String> draftFields = dataDao.getFieldNames(draftCode);
//        createTable(newTable, draftFields, false);
//        if (sourceStorageCode != null && draftStructure.equals(actualVersionStructure)) {
//            insertActualDataFromVersion(sourceStorageCode, draftCode, newTable);
//            insertOldDataFromVersion(sourceStorageCode, newTable);
//            insertClosedNowDataFromVersion(sourceStorageCode, draftCode, newTable, publishTime);
//            insertNewDataFromDraft(sourceStorageCode, draftCode, newTable, publishTime);
//        } else {
//            BigInteger count = dataDao.countData(draftCode);
//            draftFields.add("FTS");
//            for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
//                dataDao.insertDataFromDraft(draftCode, i, newTable, TRANSACTION_SIZE, publishTime, draftFields);
//            }
//        }
//        return newTable;
        return null;
    }

    @Override
    public List<String> addRows(String draftCode, List<RowValue> data) {
        List<CodifiedException> exceptions = new ArrayList<>();

        List<FieldValue> fieldValues = new ArrayList<>();
        for (Object value : data.get(0).getFieldValues()) {
            fieldValues.add((FieldValue) value);
        }
        String keys = fieldValues.stream().map(field -> addEscapeCharacters(field.getField().getName())).collect(Collectors.joining(","));
        List<String> values = new ArrayList<>();
        for (RowValue rowValue : data) {
            validateRow(draftCode, rowValue.getFieldValues(), null, exceptions);
            List<String> rowValues = new ArrayList<>();
            for (Object fieldValueObj : rowValue.getFieldValues()) {
                FieldValue fieldValue = (FieldValue) fieldValueObj;
                if (fieldValue.getValue() == null) {
                    rowValues.add("null");
                } else if (fieldValue.getField() instanceof ReferenceField) {
                    rowValues.add("?\\:\\:jsonb");
                } else
                    rowValues.add("?");
            }
            values.add(String.join(",", rowValues));
        }

        if (!exceptions.isEmpty()) {
            throw new ListCodifiedException(exceptions);
        }
        dataDao.insertData(draftCode, keys, values, data);
        return null;
    }

    @Override
    public void deleteRows(String draftCode, List<String> systemIds) {
        dataDao.deleteData(draftCode, systemIds);
    }

    @Override
    public void deleteAllRows(String draftCode) {
        dataDao.deleteData(draftCode);
    }

    @Override
    public void updateRow(String draftCode, String systemId, List<FieldValue> data) {
        List<CodifiedException> exceptions = new ArrayList<>();
        Map<String, String> types = new HashMap<>();
        validateRow(draftCode, data, systemId, exceptions);
        List<String> keyList = new ArrayList<>();
        for (FieldValue fieldValue : data) {
            String fieldName = fieldValue.getField().getName();
            if (fieldValue.getValue() == null || fieldValue.getValue().equals("null")) {
                keyList.add(addEscapeCharacters(fieldName) + " = NULL");
            } else if (fieldValue.getField() instanceof ReferenceField) {
                keyList.add(addEscapeCharacters(fieldName) + " = ?\\:\\:jsonb");
            } else {
                keyList.add(addEscapeCharacters(fieldName) + " = ?");
            }
        }
        String keys = String.join(",", keyList);
        if (exceptions.size() != 0) {
            throw new ListCodifiedException(exceptions);
        }
        dataDao.updateData(draftCode, systemId, keys, data, types);
    }

    @Override
    public void addField(String draftCode, Field field) {
        dataDao.dropTrigger(draftCode);
        dataDao.addColumnToTable(draftCode, field);
        dataDao.createTrigger(draftCode);
    }

    @Override
    public void deleteField(String draftCode, String fieldName) {
        dataDao.dropTrigger(draftCode);
        dataDao.deleteColumnFromTable(draftCode, fieldName);
        dataDao.createTrigger(draftCode);
    }

    private void createTable(String draftCode, List<Field> fields, boolean isDraft) {
        List<String> fieldNames = fields.stream().map(Field::getName).collect(Collectors.toList());
        logger.debug("creating table with name: {}", draftCode);
        if (isDraft) {
            dataDao.createDraftTable(draftCode, fields);
        } else {
            dataDao.createVersionTable(draftCode, fields);
        }
        dataDao.createHashIndex(draftCode);
        if (!fields.isEmpty()) {
            dataDao.createTrigger(draftCode, fieldNames);
            for (Field field : fields) {
                if (BooleanUtils.toBoolean(field.getSearchEnabled())) {
                    dataDao.createIndex(draftCode, field.getName());
                }
            }
        }
        dataDao.createFullTextSearchIndex(draftCode);
    }

    private void validateRow(String draftCode, List<FieldValue> data, String systemId, List<CodifiedException> exceptions) {
        List<FieldValue> dataCopy = new ArrayList<>(data);
        dataCopy.removeIf(v -> v.getValue() == null);
        if (dataCopy.size() == 0)
            throw new CodifiedException(EMPTY_RECORD_EXCEPTION_CODE);

        Date dateBegin = null;
        Date dateEnd = null;
        for (FieldValue fieldValue : data) {
            Field field = fieldValue.getField();
            if (DATE_BEGIN.equals(field.getName()))
                dateBegin = (Date) fieldValue.getValue();
            if (DATE_END.equals(field.getName()))
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
                            dateBegin != null, dateBegin, dateEnd, systemId);
                    if (result.size() > 0) {
                        exceptions.add(new CodifiedException(DUPLICATE_UNIQUE_VALUE_EXCEPTION_CODE, fieldValue.getValue()));
                    }
                }
            }

        }
        if (dateBegin != null && dateEnd != null && dateBegin.after(dateEnd)) {
            exceptions.add(new CodifiedException(BEGIN_END_DATE_EXCEPTION_CODE));
        }
    }

    private String getFts(List<FieldValue> fields) {
        StringBuilder fullTextSearch = new StringBuilder();
        List<String> values = new ArrayList<>();
        for (FieldValue field : fields) {
            Object value = field.getValue();
            //todo check value type
            if (value != null)
                values.add(value.toString());
        }
        for (String value : values) {
            fullTextSearch.append(" coalesce( to_tsvector('ru', '").append(value).append("'),'')");
            if (!value.equals(values.get(values.size() - 1)))
                fullTextSearch.append(" || ' ' ||");
        }
        return fullTextSearch.toString();
    }


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

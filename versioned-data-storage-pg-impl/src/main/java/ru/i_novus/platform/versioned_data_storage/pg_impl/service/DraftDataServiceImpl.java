package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;

import java.util.*;
import java.util.stream.Collectors;

import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.addEscapeCharacters;

/**
 * @author lgalimova
 * @since 22.03.2018
 */
public class DraftDataServiceImpl implements DraftDataService {

    private static final Logger logger = LoggerFactory.getLogger(DataDao.class);
    private DataDao dataDao;

    @Override
    public String createDraft(List<Field> fields, List<RowValue> data) {
        String draftCode = createDraft(fields);
        for (RowValue rowValue : data) {
            List<FieldValue> fieldValues = new ArrayList<>();
            for (Object value : rowValue.getFieldValues()) {
                fieldValues.add((FieldValue) value);
            }
            final String keys = fieldValues.stream().map(v -> addEscapeCharacters(v.getName())).collect(Collectors.joining(",")) + ",\"FTS\"";
            final String values = fieldValues.stream().map(v -> "?").collect(Collectors.joining(", ")) + "," + getFts(fieldValues);
            dataDao.insertData(draftCode, keys, values, fieldValues);
        }
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
        return null;
    }

    @Override
    public List<String> addRows(String draftCode, List<RowValue> data) {
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

    private void createTable(String tableName, List<Field> fields, boolean isDraft) {
        List<String> fieldNames = fields.stream().map(Field::getName).collect(Collectors.toList());
        logger.debug("creating table with name: {}", tableName);
        if (isDraft) {
            dataDao.createDraftTable(tableName, fields);
        } else {
            dataDao.createVersionTable(tableName, fields);
        }
        dataDao.createHashIndex(tableName);
        if (!fields.isEmpty()) {
            dataDao.createTrigger(tableName, fieldNames);
            for (Field field : fields) {
                if (BooleanUtils.toBoolean(field.getSearchEnabled())) {
                    dataDao.createIndex(tableName, field.getName());
                }
            }
        }
        dataDao.createFullTextSearchIndex(tableName);
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


    public void setDataDao(DataDao dataDao) {
        this.dataDao = dataDao;
    }
}

package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.components.common.exception.CodifiedException;
import ru.i_novus.platform.datastorage.temporal.exception.ListCodifiedException;
import ru.i_novus.platform.datastorage.temporal.exception.NotUniqueException;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;
import ru.i_novus.platform.datastorage.temporal.service.StorageCodeService;
import ru.i_novus.platform.datastorage.temporal.util.CollectionUtils;
import ru.i_novus.platform.datastorage.temporal.util.StorageUtils;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.BooleanField;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.TreeField;

import javax.persistence.PersistenceException;
import javax.transaction.Transactional;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Optional.of;
import static ru.i_novus.platform.datastorage.temporal.model.StorageConstants.*;
import static ru.i_novus.platform.datastorage.temporal.util.StorageUtils.*;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.addDoubleQuotes;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.ExceptionCodes.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.*;

/**
 * @author lgalimova
 * @since 22.03.2018
 */
public class DraftDataServiceImpl implements DraftDataService {

    private static final Logger logger = LoggerFactory.getLogger(DraftDataServiceImpl.class);

    private DataDao dataDao;

    private StorageCodeService storageCodeService;

    public DraftDataServiceImpl(DataDao dataDao, StorageCodeService storageCodeService) {

        this.dataDao = dataDao;
        this.storageCodeService = storageCodeService;
    }

    @Override
    public void createSchema(String schemaName) {
        dataDao.createSchema(schemaName);
    }

    @Override
    @Transactional
    public String createDraft(List<Field> fields) {
        return createDraft(null, fields);
    }

    @Override
    @Transactional
    public String createDraft(String schemaName, List<Field> fields) {

        String draftCode = storageCodeService.generateStorageName();
        createDraftTable(StorageUtils.toStorageCode(schemaName, draftCode), fields);
        return draftCode;
    }

    @Override
    @Transactional(Transactional.TxType.NOT_SUPPORTED)
    public String applyDraft(String baseStorageCode, String draftCode, LocalDateTime publishTime) {

        return applyDraft(baseStorageCode, draftCode, publishTime, null);
    }

    @Override
    @Transactional(Transactional.TxType.NOT_SUPPORTED)
    public String applyDraft(String baseStorageCode, String draftCode,
                             LocalDateTime publishTime, LocalDateTime closeTime) {

        if (!storageExists(draftCode))
            throw new IllegalArgumentException("draft.table.does.not.exist");

        String targetCode = createVersionTable(draftCode);
        List<String> fieldNames = dataDao.getEscapedFieldNames(draftCode);

        if (baseStorageCode != null && dataDao.tableStructureEquals(baseStorageCode, draftCode)) {
            insertActualDataFromVersion(baseStorageCode, draftCode, targetCode, fieldNames, publishTime, closeTime);
            insertOldDataFromVersion(baseStorageCode, draftCode, targetCode, fieldNames, publishTime, closeTime);
            insertClosedNowDataFromVersion(baseStorageCode, draftCode, targetCode, fieldNames, publishTime, closeTime);
            insertNewDataFromDraft(baseStorageCode, draftCode, targetCode, fieldNames, publishTime, closeTime);
            dataDao.deletePointRows(targetCode);

        } else {
            insertAllDataFromDraft(draftCode, targetCode, fieldNames, publishTime, closeTime);
        }

        return targetCode;
    }

    @Override
    public boolean schemaExists(String schemaName) {
        return dataDao.schemaExists(schemaName);
    }

    @Override
    public boolean storageExists(String storageCode) {
        return dataDao.storageExists(storageCode);
    }

    @Override
    public void addRows(String draftCode, List<RowValue> rowValues) {
        try {
            dataDao.insertData(draftCode, rowValues);

        } catch (PersistenceException pe) {
            processNotUniqueRowException(pe);
        }
    }

    @Override
    public void updateRows(String draftCode, List<RowValue> rowValues) {

        List<CodifiedException> exceptions = new ArrayList<>();
        // NB: Валидация validateRow закомментирована
        if (rowValues.stream().anyMatch(rowValue -> rowValue.getSystemId() == null))
            exceptions.add(new CodifiedException(FIELD_IS_REQUIRED_EXCEPTION_CODE, SYS_PRIMARY_COLUMN));

        if (!exceptions.isEmpty()) {
            throw new ListCodifiedException(exceptions);
        }

        rowValues.forEach(rowValue -> dataDao.updateData(draftCode, rowValue));
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
    public void loadData(String draftCode, String baseStorageCode, LocalDateTime onDate) {
        loadData(draftCode, baseStorageCode, onDate, null);
    }

    @Override
    public void loadData(String draftCode, String baseStorageCode, LocalDateTime fromDate, LocalDateTime toDate) {

        List<String> draftFieldNames = dataDao.getEscapedFieldNames(draftCode);
        List<String> baseFieldNames = dataDao.getEscapedFieldNames(baseStorageCode);
        if (!draftFieldNames.equals(baseFieldNames)) {
            throw new CodifiedException(TABLES_NOT_EQUAL);
        }

        draftFieldNames.add(addDoubleQuotes(SYS_PRIMARY_COLUMN));
        draftFieldNames.add(addDoubleQuotes(SYS_FTS));
        draftFieldNames.add(addDoubleQuotes(SYS_HASH));

        dataDao.loadData(draftCode, baseStorageCode, draftFieldNames, fromDate, toDate);
        dataDao.updateSequence(draftCode);
    }

    @Override
    public void copyAllData(String sourceCode, String targetCode) {

        BigInteger count = dataDao.countData(sourceCode);
        for (int offset = 0; offset < count.intValue(); offset += TRANSACTION_ROW_LIMIT) {
            dataDao.copyTableData(sourceCode, targetCode, offset, TRANSACTION_ROW_LIMIT);
        }
    }

    @Override
    public void updateReferenceInRows(String storageCode, ReferenceFieldValue fieldValue, List<Object> systemIds) {
        dataDao.updateReferenceInRows(storageCode, fieldValue, systemIds);
    }

    @Override
    public void updateReferenceInRefRows(String storageCode, ReferenceFieldValue fieldValue,
                                         LocalDateTime publishTime, LocalDateTime closeTime) {
        BigInteger count = dataDao.countReferenceInRefRows(storageCode, fieldValue);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_ROW_LIMIT) {
            dataDao.updateReferenceInRefRows(storageCode, fieldValue, i, TRANSACTION_ROW_LIMIT);
        }
    }

    @Override
    @Transactional
    public void addField(String draftCode, Field field) {

        if (systemFieldList().contains(field.getName()))
            throw new CodifiedException(SYS_FIELD_CONFLICT);

        List<String> fieldNames = dataDao.getEscapedFieldNames(draftCode);
        if (fieldNames.contains(addDoubleQuotes(field.getName())))
            throw new CodifiedException(COLUMN_ALREADY_EXISTS);

        dataDao.dropTriggers(draftCode);
        String defaultValue = (field instanceof BooleanField) ? "false" : null;
        dataDao.addColumn(draftCode, field.getName(), field.getType(), defaultValue);

        fieldNames = dataDao.getHashUsedFieldNames(draftCode);
        dataDao.createTriggers(draftCode, fieldNames);
        dataDao.updateHashRows(draftCode, fieldNames);
    }

    @Override
    @Transactional
    public void updateField(String draftCode, Field field) {

        String oldType = dataDao.getFieldType(draftCode, field.getName());
        String newType = field.getType();
        if (oldType.equals(newType))
            return;

        try {
            dataDao.dropTriggers(draftCode);
            dataDao.alterDataType(draftCode, field.getName(), oldType, newType);

            List<String> fieldNames = dataDao.getHashUsedFieldNames(draftCode);
            dataDao.createTriggers(draftCode, fieldNames);

        } catch (PersistenceException pe) {
            throw new CodifiedException(INCOMPATIBLE_NEW_DATA_TYPE_EXCEPTION_CODE, pe, field.getName());
        }
    }

    @Override
    @Transactional
    public void deleteField(String draftCode, String fieldName) {

        List<String> fieldNames = dataDao.getEscapedFieldNames(draftCode);
        if (!fieldNames.contains(addDoubleQuotes(fieldName)))
            throw new CodifiedException(COLUMN_NOT_EXISTS);

        dataDao.dropTriggers(draftCode);
        dataDao.deleteColumn(draftCode, fieldName);
        dataDao.deleteEmptyRows(draftCode);

        fieldNames = dataDao.getHashUsedFieldNames(draftCode);
        if (CollectionUtils.isNullOrEmpty(fieldNames))
            return;

        dataDao.createTriggers(draftCode, fieldNames);
        try {
            dataDao.updateHashRows(draftCode, fieldNames);

        } catch (PersistenceException pe) {
            processNotUniqueRowException(pe);
        }
        dataDao.updateFtsRows(draftCode, fieldNames);
    }

    @Override
    public boolean isFieldNotEmpty(String storageCode, String fieldName) {
        return dataDao.isFieldNotEmpty(storageCode, fieldName);
    }

    @Override
    public boolean isFieldContainEmptyValues(String storageCode, String fieldName) {
        return dataDao.isFieldContainEmptyValues(storageCode, fieldName);
    }

    @Override
    public boolean isFieldUnique(String storageCode, String fieldName, LocalDateTime publishTime) {
        return dataDao.isUnique(storageCode, singletonList(fieldName), publishTime);
    }

    @Override
    public boolean isUnique(String storageCode, List<String> fieldNames) {
        return dataDao.isUnique(storageCode, fieldNames, null);
    }

    private void createDraftTable(String draftCode, List<Field> fields) {

        //todo никак не учитывается Field.unique - уникальность в рамках черновика
        logger.debug("creating table with name: {}", draftCode);
        dataDao.createDraftTable(draftCode, fields);

        List<String> fieldNames = fields.stream()
                .map(this::getHashUsedFieldName)
                .filter(f -> !systemFieldList().contains(f))
                .collect(Collectors.toList());
        Collections.sort(fieldNames);

        if (!fields.isEmpty()) {
            dataDao.createTriggers(draftCode, fieldNames);

            for (Field field : fields) {
                String fieldName = field.getName();

                if (field instanceof TreeField)
                    dataDao.createLtreeIndex(draftCode, fieldName);

                else if (Boolean.TRUE.equals(field.getSearchEnabled())) {
                    String indexName = escapeTableIndexName(toTableName(draftCode), fieldName.toLowerCase());
                    dataDao.createIndex(draftCode, indexName, singletonList(fieldName));
                }
            }
        }
        dataDao.createFullTextSearchIndex(draftCode);
    }

    private String getHashUsedFieldName(Field field) {
        String name = addDoubleQuotes(field.getName());
        if (REFERENCE_FIELD_SQL_TYPE.equals(field.getType()))
            name += "->>'value'";
        return name;
    }

    private String createVersionTable(String draftCode) {

        //todo никак не учитывается Field.unique - уникальность в рамках даты
        String versionName = storageCodeService.generateStorageName();
        String versionCode = StorageUtils.toStorageCode(toSchemaName(draftCode), versionName);
        dataDao.copyTable(draftCode, versionCode);

        dataDao.addColumn(versionName, SYS_PUBLISHTIME, "timestamp without time zone", MIN_TIMESTAMP_VALUE);
        dataDao.addColumn(versionName, SYS_CLOSETIME, "timestamp without time zone", MAX_TIMESTAMP_VALUE);

        dataDao.createIndex(versionCode,
                escapeTableIndexName(versionName, TABLE_INDEX_SYSDATE_NAME),
                Arrays.asList(SYS_PUBLISHTIME, SYS_CLOSETIME));

        List<String> fieldNames = dataDao.getHashUsedFieldNames(versionName);
        dataDao.createTriggers(versionName, fieldNames);

        return versionCode;
    }

    private void insertAllDataFromDraft(String draftCode, String targetCode, List<String> fieldNames,
                                        LocalDateTime publishTime, LocalDateTime closeTime) {

        fieldNames.add(addDoubleQuotes(SYS_FTS));

        BigInteger count = dataDao.countData(draftCode);
        for (int offset = 0; offset < count.intValue(); offset += TRANSACTION_ROW_LIMIT) {
            dataDao.insertAllDataFromDraft(draftCode, targetCode, fieldNames,
                    offset, TRANSACTION_ROW_LIMIT, publishTime, closeTime);
        }
    }

    /*
     * есть пересечения по дате
     * есть SYS_HASH (draftCode join versionCode по SYS_HASH)
     */
    private void insertActualDataFromVersion(String versionCode, String draftCode,
                                             String targetCode, List<String> fieldNames,
                                             LocalDateTime publishTime, LocalDateTime closeTime) {

        Map<String, String> dataTypes = dataDao.getColumnDataTypes(versionCode);
        Map<String, String> typedNames = new LinkedHashMap<>();
        fieldNames.forEach(column -> typedNames.put(column, dataTypes.get(column.replaceAll("\"", ""))));

        BigInteger count = dataDao.countActualDataFromVersion(versionCode, draftCode, publishTime, closeTime);
        for (int offset = 0; offset < count.intValue(); offset += TRANSACTION_ROW_LIMIT) {
            dataDao.insertActualDataFromVersion(targetCode, versionCode, draftCode, typedNames,
                    offset, TRANSACTION_ROW_LIMIT, publishTime, closeTime);
        }
    }

    /*
     * нет пересечений по дате
     * нет SYS_HASH (из versionCode те, которых нет в draftCode)
     */
    private void insertOldDataFromVersion(String versionCode, String draftCode,
                                          String targetCode, List<String> fieldNames,
                                          LocalDateTime publishTime, LocalDateTime closeTime) {

        BigInteger count = dataDao.countOldDataFromVersion(versionCode, draftCode, publishTime, closeTime);
        for (int offset = 0; offset < count.intValue(); offset += TRANSACTION_ROW_LIMIT) {
            dataDao.insertOldDataFromVersion(targetCode, versionCode, draftCode, fieldNames,
                    offset, TRANSACTION_ROW_LIMIT, publishTime, closeTime);
        }
    }

    /*
     * есть пересечения по дате
     * нет SYS_HASH (из versionCode те, которых нет в draftCode
     */
    private void insertClosedNowDataFromVersion(String versionCode, String draftCode,
                                                String targetCode, List<String> fieldNames,
                                                LocalDateTime publishTime, LocalDateTime closeTime) {

        Map<String, String> dataTypes = dataDao.getColumnDataTypes(versionCode);
        Map<String, String> typedNames = new LinkedHashMap<>();
        fieldNames.forEach(column -> typedNames.put(column, dataTypes.get(column.replaceAll("\"", ""))));

        BigInteger count = dataDao.countClosedNowDataFromVersion(versionCode, draftCode, publishTime, closeTime);
        for (int offset = 0; offset < count.intValue(); offset += TRANSACTION_ROW_LIMIT) {
            dataDao.insertClosedNowDataFromVersion(targetCode, versionCode, draftCode, typedNames,
                    offset, TRANSACTION_ROW_LIMIT, publishTime, closeTime);
        }
    }

    /*
     * добавить запись
     *
     * нет пересечений по дате
     * есть SYS_HASH (draftCode join versionCode по SYS_HASH)
     * или
     * нет SYS_HASH (из draftCode те, которых нет в versionCode)
     */
    private void insertNewDataFromDraft(String versionCode, String draftCode,
                                        String targetCode, List<String> fieldNames,
                                        LocalDateTime publishTime, LocalDateTime closeTime) {

        BigInteger count = dataDao.countNewValFromDraft(draftCode, versionCode, publishTime, closeTime);
        for (int offset = 0; offset < count.intValue(); offset += TRANSACTION_ROW_LIMIT) {
            dataDao.insertNewDataFromDraft(targetCode, versionCode, draftCode, fieldNames,
                    offset, TRANSACTION_ROW_LIMIT, publishTime, closeTime);
        }
    }

    private void processNotUniqueRowException(PersistenceException pe) {

        //Обработка кода ошибки о нарушении уникальности в postgres
        SQLException sqlException = (SQLException) of(pe).map(Throwable::getCause).map(Throwable::getCause)
                .filter(e -> e instanceof SQLException).orElse(null);
        if (sqlException != null && "23505".equals(sqlException.getSQLState())) {
            throw new NotUniqueException(NOT_UNIQUE_ROW);
        }
        throw pe;
    }
}

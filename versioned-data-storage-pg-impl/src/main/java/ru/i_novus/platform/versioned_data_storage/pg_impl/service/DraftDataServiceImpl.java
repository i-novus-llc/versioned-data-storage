package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import ru.i_novus.components.common.exception.CodifiedException;
import ru.i_novus.platform.datastorage.temporal.exception.ListCodifiedException;
import ru.i_novus.platform.datastorage.temporal.exception.NotUniqueException;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;
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

import static java.util.Optional.of;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.ExceptionCodes.*;
import static ru.i_novus.platform.datastorage.temporal.model.DataConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.*;
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
        createDraftTable(draftCode, fields);
        return draftCode;
    }

    @Override
    @Transactional(Transactional.TxType.NOT_SUPPORTED)
    public String applyDraft(String sourceStorageCode, String draftCode, LocalDateTime publishTime) {
        return applyDraft(sourceStorageCode, draftCode, publishTime, null);
    }

    @Override
    @Transactional(Transactional.TxType.NOT_SUPPORTED)
    public String applyDraft(String baseStorageCode, String draftCode,
                             LocalDateTime publishTime, LocalDateTime closeTime) {

        if (!draftExists(draftCode))
            throw new IllegalArgumentException("draft.table.does.not.exist");

        String newTable = createVersionTable(draftCode);
        List<String> draftFields = dataDao.getFieldNames(draftCode);

        if (baseStorageCode != null && dataDao.tableStructureEquals(baseStorageCode, draftCode)) {
            insertActualDataFromVersion(baseStorageCode, draftCode, newTable, draftFields, publishTime, closeTime);
            insertOldDataFromVersion(baseStorageCode, draftCode, newTable, draftFields, publishTime, closeTime);
            insertClosedNowDataFromVersion(baseStorageCode, draftCode, newTable, draftFields, publishTime, closeTime);
            insertNewDataFromDraft(baseStorageCode, draftCode, newTable, draftFields, publishTime, closeTime);
            dataDao.deletePointRows(newTable);

        } else {
            BigInteger count = dataDao.countData(draftCode);
            draftFields.add(addDoubleQuotes("FTS"));
            for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
                dataDao.insertDataFromDraft(draftCode, newTable, draftFields, i, TRANSACTION_SIZE, publishTime, closeTime);
            }
        }
        return newTable;
    }

    @Override
    public boolean draftExists(String draftCode) {
        return dataDao.tableExists(draftCode);
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
    public void deleteRows(String draftCode, List<Object> systemIds) {
        dataDao.deleteData(draftCode, systemIds);
    }

    @Override
    public void deleteAllRows(String draftCode) {
        dataDao.deleteData(draftCode);
    }

    @Override
    public void updateRow(String draftCode, RowValue rowValue) {

        List<CodifiedException> exceptions = new ArrayList<>();
        // NB: Валидация validateRow закомментирована
        if (rowValue.getSystemId() == null)
            exceptions.add(new CodifiedException(FIELD_IS_REQUIRED_EXCEPTION_CODE, SYS_PRIMARY_COLUMN));

        if (!exceptions.isEmpty()) {
            throw new ListCodifiedException(exceptions);
        }

        dataDao.updateData(draftCode, rowValue);
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
    public void updateReferenceInRows(String storageCode, ReferenceFieldValue fieldValue, List<Object> systemIds) {
        dataDao.updateReferenceInRows(storageCode, fieldValue, systemIds);
    }

    @Override
    public void updateReferenceInRefRows(String storageCode, ReferenceFieldValue fieldValue,
                                         LocalDateTime publishTime, LocalDateTime closeTime) {
        BigInteger count = dataDao.countReferenceInRefRows(storageCode, fieldValue);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.updateReferenceInRefRows(storageCode, fieldValue, i, TRANSACTION_SIZE);
        }
    }

    @Override
    public void loadData(String draftCode, String sourceStorageCode, LocalDateTime onDate) {
        loadData(draftCode, sourceStorageCode, onDate, null);
    }

    @Override
    public void loadData(String draftCode, String sourceStorageCode, LocalDateTime fromDate, LocalDateTime toDate) {
        List<String> draftFields = dataDao.getFieldNames(draftCode);
        List<String> sourceFields = dataDao.getFieldNames(draftCode);
        if (!draftFields.equals(sourceFields)) {
            throw new CodifiedException(TABLES_NOT_EQUAL);
        }

        draftFields.add(addDoubleQuotes(SYS_PRIMARY_COLUMN));
        draftFields.add(addDoubleQuotes(SYS_FULL_TEXT_SEARCH));
        draftFields.add(addDoubleQuotes(SYS_HASH));

        dataDao.loadData(draftCode, sourceStorageCode, draftFields, fromDate, toDate);
        dataDao.updateSequence(draftCode);
    }

    @Transactional
    @Override
    public void addField(String draftCode, Field field) {
        if (systemFieldList().contains(field.getName()))
            throw new CodifiedException(SYS_FIELD_CONFLICT);
        if (dataDao.getFieldNames(draftCode).contains(addDoubleQuotes(field.getName())))
            throw new CodifiedException(COLUMN_ALREADY_EXISTS);

        dataDao.dropTrigger(draftCode);
        String defaultValue = (field instanceof BooleanField) ? "false" : null;
        dataDao.addColumnToTable(draftCode, field.getName(), field.getType(), defaultValue);
        dataDao.createTrigger(draftCode);
        dataDao.updateHashRows(draftCode);
    }

    @Transactional
    @Override
    public void deleteField(String draftCode, String fieldName) {
        if (!dataDao.getFieldNames(draftCode).contains(addDoubleQuotes(fieldName)))
            throw new CodifiedException(COLUMN_NOT_EXISTS);

        dataDao.dropTrigger(draftCode);
        dataDao.deleteColumnFromTable(draftCode, fieldName);
        dataDao.deleteEmptyRows(draftCode);

        if (!CollectionUtils.isEmpty(dataDao.getFieldNames(draftCode))) {
            dataDao.createTrigger(draftCode);
            try {
                dataDao.updateHashRows(draftCode);

            } catch (PersistenceException pe) {
                processNotUniqueRowException(pe);
            }
            dataDao.updateFtsRows(draftCode);
        }
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
    public boolean isFieldNotEmpty(String storageCode, String fieldName) {
        return dataDao.isFieldNotEmpty(storageCode, fieldName);
    }

    @Override
    public boolean isFieldContainEmptyValues(String storageCode, String fieldName) {
        return dataDao.isFieldContainEmptyValues(storageCode, fieldName);
    }

    @Override
    public boolean isFieldUnique(String storageCode, String fieldName, LocalDateTime publishTime) {
        return dataDao.isUnique(storageCode, Collections.singletonList(fieldName), publishTime);
    }

    @Override
    public boolean isUnique(String storageCode, List<String> fieldNames) {
        return dataDao.isUnique(storageCode, fieldNames, null);
    }

    private String getHashUsedFieldName(Field field) {
        String name = addDoubleQuotes(field.getName());
        if (REFERENCE_FIELD_SQL_TYPE.equals(field.getType()))
            name += "->>'value'";
        return name;
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
            dataDao.createTrigger(draftCode, fieldNames);
            for (Field field : fields) {
                if (field instanceof TreeField)
                    dataDao.createLtreeIndex(draftCode, field.getName());
                else if (BooleanUtils.toBoolean(field.getSearchEnabled())) {
                    dataDao.createIndex(draftCode, addDoubleQuotes(draftCode + "_" + field.getName().toLowerCase() + "_idx"),
                            Collections.singletonList(field.getName()));
                }
            }
        }
        dataDao.createFullTextSearchIndex(draftCode);
    }

    private String createVersionTable(String draftCode) {
        //todo никак не учитывается Field.unique - уникальность в рамках даты
        String newTable = UUID.randomUUID().toString();
        dataDao.copyTable(newTable, draftCode);
        dataDao.addColumnToTable(newTable, SYS_PUBLISHTIME, "timestamp without time zone", MIN_DATETIME_VALUE);
        dataDao.addColumnToTable(newTable, SYS_CLOSETIME, "timestamp without time zone", MAX_DATETIME_VALUE);
        dataDao.createIndex(newTable, addDoubleQuotes(newTable + "_SYSDATE_idx"), Arrays.asList(SYS_PUBLISHTIME, SYS_CLOSETIME));
        List<String> fieldNames = dataDao.getHashUsedFieldNames(newTable);
        dataDao.createTrigger(newTable, fieldNames);
        return newTable;
    }

    /*
     * есть пересечения по дате
     * есть SYS_HASH (draftTable join actualVersionTable по SYS_HASH)
     */
    private void insertActualDataFromVersion(String actualVersionTable, String draftTable, String newTable, List<String> columns, LocalDateTime publishTime, LocalDateTime closeTime) {
        BigInteger count = dataDao.countActualDataFromVersion(actualVersionTable, draftTable, publishTime, closeTime);
        Map<String, String> columnsWithType = new LinkedHashMap<>();
        columns.forEach(column -> columnsWithType.put(column, dataDao.getFieldType(actualVersionTable, column.replaceAll("\"", ""))));
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertActualDataFromVersion(newTable, actualVersionTable, draftTable, columnsWithType, i, TRANSACTION_SIZE, publishTime, closeTime);
        }
    }

    /*
     * нет пересечений по дате
     * нет SYS_HASH (из actualVersionTable те, которых нет в draftTable
     */
    private void insertOldDataFromVersion(String actualVersionTable, String draftTable, String newTable, List<String> columns, LocalDateTime publishTime, LocalDateTime closeTime) {
        BigInteger count = dataDao.countOldDataFromVersion(actualVersionTable, draftTable, publishTime, closeTime);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertOldDataFromVersion(newTable, actualVersionTable, draftTable, columns, i, TRANSACTION_SIZE, publishTime, closeTime);
        }
    }

    /*
     * есть пересечения по дате
     * нет SYS_HASH (из actualVersionTable те, которых нет в draftTable
     */
    private void insertClosedNowDataFromVersion(String actualVersionTable, String draftTable, String newTable,
                                                List<String> columns, LocalDateTime publishTime, LocalDateTime closeTime) {
        BigInteger count = dataDao.countClosedNowDataFromVersion(actualVersionTable, draftTable, publishTime, closeTime);
        logger.info("closed now count {}", count);
        Map<String, String> columnsWithType = new LinkedHashMap<>();
        columns.forEach(column -> columnsWithType.put(column, dataDao.getFieldType(actualVersionTable, column.replaceAll("\"", ""))));
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertClosedNowDataFromVersion(newTable, actualVersionTable, draftTable, columnsWithType, i, TRANSACTION_SIZE, publishTime, closeTime);
        }
    }

    /*
     * добавить запись
     *
     * нет пересечений по дате
     * есть SYS_HASH (draftTable join actualVersionTable по SYS_HASH)
     * или
     * нет SYS_HASH (из draftTable те, которых нет в actualVersionTable)
     */
    private void insertNewDataFromDraft(String actualVersionTable, String draftTable, String newTable,
                                        List<String> columns, LocalDateTime publishTime, LocalDateTime closeTime) {
        BigInteger count = dataDao.countNewValFromDraft(draftTable, actualVersionTable, publishTime, closeTime);
        for (int i = 0; i < count.intValue(); i += TRANSACTION_SIZE) {
            dataDao.insertNewDataFromDraft(newTable, actualVersionTable, draftTable, columns, i, TRANSACTION_SIZE, publishTime, closeTime);
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

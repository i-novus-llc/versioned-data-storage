package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.components.common.exception.CodifiedException;
import ru.i_novus.platform.datastorage.temporal.exception.ListCodifiedException;
import ru.i_novus.platform.datastorage.temporal.exception.NotUniqueException;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.criteria.BaseDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageCopyRequest;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.StorageConstants;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.BooleanField;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.TreeField;
import ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil;
import ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils;

import javax.persistence.PersistenceException;
import javax.transaction.Transactional;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;

import static java.util.Collections.singletonList;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static ru.i_novus.platform.datastorage.temporal.util.CollectionUtils.isNullOrEmpty;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.ExceptionCodes.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.TRANSACTION_ROW_LIMIT;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.StorageConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StorageUtils.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.addDoubleQuotes;

/**
 * @author lgalimova
 * @since 22.03.2018
 */
@SuppressWarnings({"rawtypes", "java:S3740"})
public class DraftDataServiceImpl implements DraftDataService {

    private static final Logger logger = LoggerFactory.getLogger(DraftDataServiceImpl.class);

    private final DataDao dataDao;

    public DraftDataServiceImpl(DataDao dataDao) {

        this.dataDao = dataDao;
    }

    @Override
    @Transactional
    public String createDraft(List<Field> fields) {

        return createDraft(null, fields);
    }

    @Override
    @Transactional
    public String createDraft(String schemaName, List<Field> fields) {

        String draftCode = generateStorageName();
        createDraftTable(toStorageCode(schemaName, draftCode), fields);
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

        if (!StringUtils.isNullOrEmpty(baseStorageCode) &&
                dataDao.storageStructureEquals(baseStorageCode, draftCode)) {
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
    public boolean storageExists(String storageCode) {

        return dataDao.storageExists(storageCode);
    }

    @Override
    public void addRows(String draftCode, List<RowValue> rowValues) {

        insertData(draftCode, rowValues);
    }

    @SuppressWarnings("UnusedReturnValue")
    protected List<String> insertData(String draftCode, List<RowValue> rowValues) {

        try {
            return dataDao.insertData(draftCode, rowValues);

        } catch (PersistenceException pe) {
            throw transformException(pe);
        }
    }

    @Override
    public void updateRows(String draftCode, List<RowValue> rowValues) {

        updateData(draftCode, rowValues);
    }

    @SuppressWarnings("UnusedReturnValue")
    protected List<String> updateData(String draftCode, List<RowValue> rowValues) {

        validateUpdateData(rowValues);

        List<String> hashes = new ArrayList<>(rowValues.size());

        rowValues.forEach(rowValue -> {
            String hash = dataDao.updateData(draftCode, rowValue);
            hashes.add(hash);
        });

        return hashes;
    }

    private void validateUpdateData(List<RowValue> rowValues) {

        List<CodifiedException> exceptions = new ArrayList<>();
        // NB: Валидация validateRow закомментирована
        if (rowValues.stream().anyMatch(rowValue -> rowValue.getSystemId() == null))
            exceptions.add(new CodifiedException(FIELD_IS_REQUIRED_EXCEPTION_CODE, SYS_PRIMARY_COLUMN));

        if (!exceptions.isEmpty())
            throw new ListCodifiedException(exceptions);
    }

    @Override
    public void deleteRows(String draftCode, List<Object> systemIds) {

        deleteData(draftCode, systemIds);
    }

    @SuppressWarnings("UnusedReturnValue")
    protected List<String> deleteData(String draftCode, List<Object> systemIds) {

        return dataDao.deleteData(draftCode, systemIds);
    }

    @Override
    public void deleteAllRows(String draftCode) {

        dataDao.deleteData(draftCode);
    }

    @Override
    public void loadData(String draftCode, String sourceCode, LocalDateTime onDate) {

        loadData(draftCode, sourceCode, onDate, null);
    }

    @Override
    public void loadData(String draftCode, String sourceCode, LocalDateTime fromDate, LocalDateTime toDate) {

        List<String> draftFieldNames = dataDao.getAllEscapedFieldNames(draftCode);
        List<String> sourceFieldNames = dataDao.getAllEscapedFieldNames(sourceCode);
        List<String> versionedFieldNames = StorageConstants.escapedVersionedFieldNames();
        sourceFieldNames.removeIf(versionedFieldNames::contains);

        if (!draftFieldNames.equals(sourceFieldNames)) {
            throw new CodifiedException(TABLES_NOT_EQUAL);
        }

        copyTableData(sourceCode, draftCode, draftFieldNames, fromDate, toDate);
        dataDao.updateTableSequence(draftCode);
    }

    @Override
    public void copyAllData(String sourceCode, String targetCode) {

        List<String> fieldNames = dataDao.getAllCommonFieldNames(sourceCode, targetCode);
        copyTableData(sourceCode, targetCode, fieldNames, null, null);
    }

    private void copyTableData(String sourceCode, String targetCode, List<String> fieldNames,
                               LocalDateTime fromDate, LocalDateTime toDate) {

        BigInteger count = dataDao.countData(sourceCode);
        if (BigInteger.ZERO.equals(count))
            return;

        if (dataDao.hasData(targetCode))
            throw new CodifiedException("target.table.is.not.empty");

        boolean isTriggersRedundant = isNullOrEmpty(fieldNames) ||
                fieldNames.containsAll(escapedTriggeredFieldNames());

        if (isTriggersRedundant) {
            dataDao.disableTriggers(targetCode);
        }
        try {
            StorageCopyRequest request = new StorageCopyRequest(sourceCode, targetCode, fromDate, toDate, null);
            request.setEscapedFieldNames(fieldNames);

            request.setCount(count.intValue());
            request.setSize(TRANSACTION_ROW_LIMIT);

            int pageCount = request.getPageCount();
            for (int page = 0; page < pageCount; page++) {
                request.setPage(page + BaseDataCriteria.PAGE_SHIFT);
                dataDao.copyTableData(request);
            }

        } finally {
            if (isTriggersRedundant) {
                dataDao.enableTriggers(targetCode);
            }
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

        if (dataDao.getSystemFieldNames().contains(field.getName()))
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
        if (isNullOrEmpty(fieldNames))
            return;

        dataDao.createTriggers(draftCode, fieldNames);
        try {
            dataDao.updateHashRows(draftCode, fieldNames);

        } catch (PersistenceException pe) {
            throw transformException(pe);
        }
        dataDao.updateFtsRows(draftCode, fieldNames);
    }

    @Override
    public boolean isFieldNotEmpty(String storageCode, String fieldName) {
        return dataDao.isFieldNotNull(storageCode, fieldName);
    }

    @Override
    public boolean isFieldContainEmptyValues(String storageCode, String fieldName) {
        return dataDao.isFieldContainNullValues(storageCode, fieldName);
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

        // todo: Для Field.unique создавать индексы с уникальностью в рамках черновика.
        logger.debug("creating table with name: {}", draftCode);
        dataDao.createDraftTable(draftCode, fields);

        List<String> fieldNames = fields.stream()
                .map(QueryUtil::getHashUsedFieldName)
                .filter(f -> !dataDao.getSystemFieldNames().contains(f))
                .collect(toList());
        Collections.sort(fieldNames);

        if (!fields.isEmpty()) {
            dataDao.createTriggers(draftCode, fieldNames);

            for (Field field : fields) {
                String fieldName = field.getName();

                if (field instanceof TreeField)
                    dataDao.createLtreeIndex(draftCode, fieldName);

                else if (Boolean.TRUE.equals(field.getSearchEnabled())) {
                    dataDao.createFieldIndex(draftCode, fieldName);
                }
            }
        }
        dataDao.createFtsIndex(draftCode);
    }

    private String createVersionTable(String draftCode) {

        // todo: Для Field.unique заменять уникальные индексы на индексы
        //  с уникальностью в рамках дат публикации и прекращения действия записи.
        String versionName = generateStorageName();
        String versionCode = toStorageCode(toSchemaName(draftCode), versionName);
        dataDao.copyTable(draftCode, versionCode);
        dataDao.addVersionedInformation(versionCode);

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
        fieldNames.forEach(column -> typedNames.put(column, dataTypes.get(column.replace("\"", ""))));

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
    @SuppressWarnings("I-novus:MethodNameWordCountRule")
    private void insertClosedNowDataFromVersion(String versionCode, String draftCode,
                                                String targetCode, List<String> fieldNames,
                                                LocalDateTime publishTime, LocalDateTime closeTime) {

        Map<String, String> dataTypes = dataDao.getColumnDataTypes(versionCode);
        Map<String, String> typedNames = new LinkedHashMap<>();
        fieldNames.forEach(column -> typedNames.put(column, dataTypes.get(column.replace("\"", ""))));

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

    private RuntimeException transformException(PersistenceException exception) {

        //Обработка кода ошибки о нарушении уникальности в postgres
        SQLException sqlException = (SQLException) of(exception)
                .map(Throwable::getCause).map(Throwable::getCause)
                .filter(e -> e instanceof SQLException).orElse(null);

        final String uniqueViolationErrorCode = "23505";
        if (sqlException != null &&
                uniqueViolationErrorCode.equals(sqlException.getSQLState())) {
            return new NotUniqueException(NOT_UNIQUE_ROW);
        }

        return exception;
    }
}

package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import ru.i_novus.platform.datastorage.temporal.model.DataDifference;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.criteria.CompareDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface DataDao {

    List<RowValue> getData(StorageDataCriteria criteria);

    BigInteger getDataCount(StorageDataCriteria criteria);

    RowValue getRowData(String storageCode, List<String> fieldNames, Object systemId);

    List<RowValue> getRowData(String storageCode, List<String> fieldNames, List<Object> systemIds);

    List<String> findNonExistentHashes(String tableName, LocalDateTime bdate, LocalDateTime edate,
                                       List<String> hashList);

    boolean tableStructureEquals(String tableName1, String tableName2);

    Map<String, String> getColumnDataTypes(String storageCode);

    BigInteger countData(String tableName);

    /** Создание схемы. */
    void createSchema(String schemaName);

    /** Создание черновика с заданными полями. */
    void createDraftTable(String storageCode, List<Field> fields);

    /** Удаление таблицы. */
    void dropTable(String storageCode);

    /** Проверка схемы на существование. */
    boolean schemaExists(String schemaName);

    /** Поиск существующих схем. */
    List<String> findExistentSchemas(List<String> schemaNames);

    /** Поиск существующих схем с существующей таблицей. */
    List<String> findExistentTableSchemas(List<String> schemaNames, String tableName);

    /** Проверка таблицы на существование. */
    boolean storageExists(String storageCode);

    /**
     * Копирование таблицы.
     * <p>
     * Копируется только структура без данных.
     * Копируются все индексы (кроме индекса для SYS_HASH).
     * Создаётся неуникальный индекс для SYS_HASH.
     * Преобразуется SYS_PRIMARY_COLUMN в первичный ключ.
     */
    void copyTable(String sourceCode, String targetCode);

    void addColumn(String storageCode, String name, String type, String defaultValue);

    void deleteColumn(String storageCode, String name);

    void insertData(String storageCode, List<RowValue> data);

    void loadData(String draftTable, String sourceTable, List<String> fields,
                  LocalDateTime fromDate, LocalDateTime toDate);

    void updateData(String storageCode, RowValue rowValue);

    void deleteData(String storageCode);

    void deleteData(String storageCode, List<Object> systemIds);

    void updateReferenceInRows(String storageCode, ReferenceFieldValue fieldValue, List<Object> systemIds);

    BigInteger countReferenceInRefRows(String storageCode, ReferenceFieldValue fieldValue);

    void updateReferenceInRefRows(String storageCode, ReferenceFieldValue fieldValue, int offset, int limit);

    void deleteEmptyRows(String draftCode);

    boolean isUnique(String storageCode, List<String> fieldNames, LocalDateTime publishTime);

    void updateSequence(String tableName);

    void createTriggers(String storageCode, List<String> fieldNames);

    void updateHashRows(String tableName, List<String> fieldNames);

    void updateFtsRows(String tableName, List<String> fieldNames);

    void dropTriggers(String tableName);

    void createIndex(String storageCode, String name, List<String> fields);

    void createFullTextSearchIndex(String storageCode);

    void createLtreeIndex(String storageCode, String field);

    void createHashIndex(String storageCode);

    List<String> getFieldNames(String storageCode, String sqlSelect);

    List<String> getEscapedFieldNames(String storageCode);

    List<String> getAllEscapedFieldNames(String storageCode);

    List<String> getHashUsedFieldNames(String storageCode);

    String getFieldType(String storageCode, String fieldName);

    void alterDataType(String tableName, String field, String oldType, String newType);

    boolean isFieldNotEmpty(String tableName, String fieldName);

    boolean isFieldContainEmptyValues(String tableName, String fieldName);

    void copyTableData(String sourceCode, String targetCode, int offset, int limit);

    void insertAllDataFromDraft(String draftCode, String targetCode, List<String> fieldNames,
                                int offset, int limit,
                                LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countActualDataFromVersion(String versionTable, String draftTable,
                                          LocalDateTime publishTime, LocalDateTime closeTime);

    void insertActualDataFromVersion(String targetTable, String versionTable,
                                     String draftTable, Map<String, String> fieldNames,
                                     int offset, int limit,
                                     LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countOldDataFromVersion(String versionTable, String draftTable,
                                       LocalDateTime publishTime, LocalDateTime closeTime);

    void insertOldDataFromVersion(String targetTable, String versionTable,
                                  String draftTable, List<String> fieldNames,
                                  int offset, int limit,
                                  LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countClosedNowDataFromVersion(String versionTable, String draftTable,
                                             LocalDateTime publishTime, LocalDateTime closeTime);

    void insertClosedNowDataFromVersion(String targetTable, String versionTable,
                                        String draftTable, Map<String, String> fieldNames,
                                        int offset, int limit,
                                        LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countNewValFromDraft(String draftTable, String versionTable,
                                    LocalDateTime publishTime, LocalDateTime closeTime);

    void insertNewDataFromDraft(String targetTable, String versionTable,
                                String draftTable,  List<String> fieldNames,
                                int offset, int limit,
                                LocalDateTime publishTime, LocalDateTime closeTime);

    /**
     * Удаление точечных записей -
     * записей с совпадающими датами публикации и прекращения действия.
     */
    void deletePointRows(String targetCode);

    DataDifference getDataDifference(CompareDataCriteria criteria);
}

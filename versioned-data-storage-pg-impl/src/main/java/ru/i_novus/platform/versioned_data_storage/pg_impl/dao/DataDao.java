package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import ru.i_novus.platform.datastorage.temporal.model.DataDifference;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.criteria.CompareDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageCopyCriteria;
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

    List<String> findExistentHashes(String tableName, LocalDateTime bdate, LocalDateTime edate,
                                    List<String> hashList);

    boolean tableStructureEquals(String tableName1, String tableName2);

    /**
     * Получение наименования и типа полей хранилища
     *
     * @param storageCode код хранилища
     * @return Набор наименований полей с типами
     */
    Map<String, String> getColumnDataTypes(String storageCode);

    BigInteger countData(String tableName);

    /** Создание схемы. */
    void createSchema(String schemaName);

    /** Создание черновика с заданными полями. */
    void createDraftTable(String storageCode, List<Field> fields);

    /** Удаление таблицы. */
    void dropTable(String storageCode);

    /** Обновление последовательности для таблицы. */
    void updateTableSequence(String storageCode);

    /** Проверка схемы на существование. */
    boolean schemaExists(String schemaName);

    /** Поиск существующих схем. */
    List<String> findExistentSchemas(List<String> schemaNames);

    /** Поиск существующих схем с существующей таблицей. */
    List<String> findExistentTableSchemas(List<String> schemaNames, String tableName);

    /** Проверка таблицы на существование. */
    boolean storageExists(String storageCode);

    /** Проверка поля хранилища на существование. */
    boolean storageFieldExists(String storageCode, String columnName);

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

    /** Изменение типа данных поля. */
    void alterDataType(String storageCode, String fieldName, String oldType, String newType);

    void deleteColumn(String storageCode, String name);

    List<String> insertData(String storageCode, List<RowValue> data);

    String updateData(String storageCode, RowValue rowValue);

    void deleteData(String storageCode);

    List<String> deleteData(String storageCode, List<Object> systemIds);

    void loadData(String draftCode, String sourceCode, List<String> fields,
                  LocalDateTime fromDate, LocalDateTime toDate);

    void updateReferenceInRows(String storageCode, ReferenceFieldValue fieldValue, List<Object> systemIds);

    void deleteEmptyRows(String draftCode);

    BigInteger countReferenceInRefRows(String storageCode, ReferenceFieldValue fieldValue);

    void updateReferenceInRefRows(String storageCode, ReferenceFieldValue fieldValue, int offset, int limit);

    void createTriggers(String storageCode, List<String> fieldNames);

    void dropTriggers(String storageCode);

    void updateHashRows(String storageCode, List<String> fieldNames);

    void updateFtsRows(String storageCode, List<String> fieldNames);

    void createIndex(String storageCode, String name, List<String> fieldNames);

    void createFullTextSearchIndex(String storageCode);

    void createLtreeIndex(String storageCode, String fieldName);

    void createHashIndex(String storageCode);

    List<String> getFieldNames(String storageCode, String sqlSelect);

    /**
     * Получение закавыченных наименований полей хранилища (исключая системные поля).
     *
     * @param storageCode код хранилища
     * @return Список наименований полей
     */
    List<String> getEscapedFieldNames(String storageCode);

    /**
     * Получение всех закавыченных наименований полей хранилища.
     *
     * @param storageCode код хранилища
     * @return Список наименований полей
     */
    List<String> getAllEscapedFieldNames(String storageCode);

    /**
     * Получение всех закавыченных наименований полей хранилища для вычисления hash и fts.
     *
     * @param storageCode код хранилища
     * @return Список наименований полей
     */
    List<String> getHashUsedFieldNames(String storageCode);

    /**
     * Получение типа поля хранилища.
     *
     * @param storageCode код хранилища
     * @param fieldName   наименование поля
     * @return Тип колонки
     */
    String getFieldType(String storageCode, String fieldName);

    /**
     * Проверка хранилища на наличие записей с не-null значением поля.
     *
     * @param storageCode код хранилища
     * @param fieldName   наименование поля
     * @return Результат проверки
     */
    boolean isFieldNotNull(String storageCode, String fieldName);

    /**
     * Проверка хранилища на наличие записей с null значением поля.
     *
     * @param storageCode код хранилища
     * @param fieldName   наименование поля
     * @return Результат проверки
     */
    boolean isFieldContainNullValues(String storageCode, String fieldName);

    /**
     * Проверка хранилища на уникальность записей по полям.
     *
     * @param storageCode код хранилища
     * @param fieldNames  наименования полей
     * @param publishTime дата публикации записи
     * @return Результат проверки
     */
    boolean isUnique(String storageCode, List<String> fieldNames, LocalDateTime publishTime);

    /**
     * Копирование данных хранилища по критерию.
     *
     * @param criteria критерий копирования
     */
    void copyTableData(StorageCopyCriteria criteria);

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

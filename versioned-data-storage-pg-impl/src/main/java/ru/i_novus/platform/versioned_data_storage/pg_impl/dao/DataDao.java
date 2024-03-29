package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import ru.i_novus.platform.datastorage.temporal.model.DataDifference;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.criteria.CompareDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageCopyRequest;
import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"rawtypes", "java:S3740"})
public interface DataDao {

    /**
     * Получение записей по критерию.
     *
     * @param criteria критерий поиска
     * @return Список записей
     */
    List<RowValue> getData(StorageDataCriteria criteria);

    /**
     * Подсчёт количества записей по критерию.
     *
     * @param criteria критерий поиска
     * @return Список записей
     */
    BigInteger getDataCount(StorageDataCriteria criteria);

    /**
     * Проверка на наличие данных.
     *
     * @param storageCode код хранилища данных
     * @return Признак наличия
     */
    boolean hasData(String storageCode);

    /**
     * Получение записи хранилища по системному идентификатору.
     *
     * @param storageCode код хранилища
     * @param fieldNames  список полей
     * @param systemId    системный идентификатор
     * @return Запись
     */
    RowValue getRowData(String storageCode, List<String> fieldNames, Object systemId);

    /**
     * Получение списка записей хранилища по системным идентификаторам.
     *
     * @param storageCode код хранилища
     * @param fieldNames  список полей
     * @param systemIds   список системных идентификаторов
     * @return Список записей
     */
    List<RowValue> getRowData(String storageCode, List<String> fieldNames, List<Object> systemIds);

    /**
     * Получение хешей, соответствующих записям в хранилище.
     *
     * @param storageCode код хранилища данных
     * @param bdate       дата публикации версии
     * @param edate       дата создания версии
     * @param hashList    список хешей записей
     * @return Список существующих хешей
     */
    List<String> findExistentHashes(String storageCode, LocalDateTime bdate, LocalDateTime edate,
                                    List<String> hashList);

    /**
     * Проверка эквивалентности наименований и типов полей хранилищ.
     *
     * @param storageCode1 код первого хранилища данных
     * @param storageCode2 код второго хранилища данных
     * @return Результат проверки
     */
    boolean storageStructureEquals(String storageCode1, String storageCode2);

    /**
     * Получение наименования и типа полей хранилища
     *
     * @param storageCode код хранилища
     * @return Набор наименований полей с типами
     */
    Map<String, String> getColumnDataTypes(String storageCode);

    /**
     * Подсчёт количества записей хранилища.
     *
     * @param storageCode код хранилища
     * @return Количество записей
     */
    BigInteger countData(String storageCode);

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

    /** Создание схемы. */
    void createSchema(String schemaName);

    /** Создание черновика с заданными полями. */
    void createDraftTable(String storageCode, List<Field> fields);

    /** Удаление таблицы. */
    void dropTable(String storageCode);

    /** Обновление последовательности для таблицы. */
    void updateTableSequence(String storageCode);

    /** Создание необходимых триггеров с функциями для таблицы. */
    void createTriggers(String storageCode, List<String> fieldNames);

    /** Создание триггеров для таблицы. */
    void dropTriggers(String storageCode);

    /** Включение триггеров для таблицы. */
    void enableTriggers(String storageCode);

    /** Отключение триггеров для таблицы. */
    void disableTriggers(String storageCode);

    /** Создание функций для таблицы. */
    void dropTableFunctions(String storageCode);

    void updateHashRows(String storageCode, List<String> fieldNames);

    void updateFtsRows(String storageCode, List<String> fieldNames);

    /** Создание обычного индекса по одному полю. */
    void createFieldIndex(String storageCode, String fieldName);

    /** Создание индекса по списку полей. */
    void createFieldsIndex(String storageCode, String indexName, List<String> fieldNames);

    /** Создание индекса по полю SYS_HASH. */
    void createHashIndex(String storageCode);

    /** Создание индекса по полю SYS_FTS. */
    void createFtsIndex(String storageCode);

    void createLtreeIndex(String storageCode, String fieldName);

    /**
     * Копирование таблицы.
     * <p>
     * Копируется только структура без данных.
     * Копируются все индексы (кроме индекса для SYS_HASH).
     * Создаётся неуникальный индекс для SYS_HASH.
     * Преобразуется SYS_PRIMARY_COLUMN в первичный ключ.
     */
    void copyTable(String sourceCode, String targetCode);

    /** Добавление информации для версионирования. */
    void addVersionedInformation(String storageCode);

    void addColumn(String storageCode, String fieldName, String fieldType, String defaultValue);

    /** Изменение типа данных поля. */
    void alterDataType(String storageCode, String fieldName, String oldType, String newType);

    void deleteColumn(String storageCode, String fieldName);

    List<String> insertData(String storageCode, List<RowValue> data);

    String updateData(String storageCode, RowValue rowValue);

    void deleteData(String storageCode);

    List<String> deleteData(String storageCode, List<Object> systemIds);

    void updateReferenceInRows(String storageCode, ReferenceFieldValue fieldValue, List<Object> systemIds);

    void deleteEmptyRows(String draftCode);

    BigInteger countReferenceInRefRows(String storageCode, ReferenceFieldValue fieldValue);

    void updateReferenceInRefRows(String storageCode, ReferenceFieldValue fieldValue, int offset, int limit);

    /**
     * Получение наименований системных полей.
     *
     * @return Список наименований полей
     */
    List<String> getSystemFieldNames();

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
     * Получение закавыченных наименований полей, общих для обоих хранилищ.
     *
     * @param storageCode1 код хранилища 1
     * @param storageCode2 код хранилища 2
     * @return Список наименований полей
     */
    List<String> getAllCommonFieldNames(String storageCode1, String storageCode2);

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
     * Копирование данных таблицы по параметрам.
     *
     * @param request параметры копирования
     */
    void copyTableData(StorageCopyRequest request);

    void insertAllDataFromDraft(String draftCode, String targetCode, List<String> fieldNames,
                                int offset, int limit,
                                LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countActualDataFromVersion(String versionCode, String draftCode,
                                          LocalDateTime publishTime, LocalDateTime closeTime);

    void insertActualDataFromVersion(String targetCode, String versionCode,
                                     String draftCode, Map<String, String> typedNames,
                                     int offset, int limit,
                                     LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countOldDataFromVersion(String versionCode, String draftCode,
                                       LocalDateTime publishTime, LocalDateTime closeTime);

    void insertOldDataFromVersion(String targetCode, String versionCode,
                                  String draftCode, List<String> fieldNames,
                                  int offset, int limit,
                                  LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countClosedNowDataFromVersion(String versionCode, String draftCode,
                                             LocalDateTime publishTime, LocalDateTime closeTime);

    void insertClosedNowDataFromVersion(String targetCode, String versionCode,
                                        String draftCode, Map<String, String> typedNames,
                                        int offset, int limit,
                                        LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countNewValFromDraft(String draftCode, String versionCode,
                                    LocalDateTime publishTime, LocalDateTime closeTime);

    void insertNewDataFromDraft(String targetCode, String versionCode,
                                String draftCode,  List<String> fieldNames,
                                int offset, int limit,
                                LocalDateTime publishTime, LocalDateTime closeTime);

    /**
     * Удаление точечных записей -
     * записей с совпадающими датами публикации и прекращения действия.
     */
    void deletePointRows(String targetCode);

    DataDifference getDataDifference(CompareDataCriteria criteria);
}

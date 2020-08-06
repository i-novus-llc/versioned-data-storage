package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import ru.i_novus.platform.datastorage.temporal.model.DataDifference;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.criteria.CompareDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface DataDao {

    List<RowValue> getData(DataCriteria criteria);

    BigInteger getDataCount(DataCriteria criteria);

    RowValue getRowData(String storageCode, List<String> fieldNames, Object systemId);

    List<RowValue> getRowData(String storageCode, List<String> fieldNames, List<Object> systemIds);

    List<String> getNotExists(String tableName, LocalDateTime bdate, LocalDateTime edate, List<String> hashList);

    boolean tableStructureEquals(String tableName1, String tableName2);

    Map<String, String> getColumnDataTypes(String storageCode);

    BigInteger countData(String tableName);

    void createSchema(String schemaName);

    void createDraftTable(String storageCode, List<Field> fields);

    void copyTable(String sourceCode, String targetCode);

    void dropTable(String storageCode);

    boolean schemaExists(String schemaName);

    boolean storageExists(String storageCode);

    void addColumnToTable(String tableName, String name, String type, String defaultValue);

    void deleteColumnFromTable(String tableName, String field);

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

    List<String> getHashUsedFieldNames(String storageCode);

    String getFieldType(String storageCode, String fieldName);

    void alterDataType(String tableName, String field, String oldType, String newType);

    boolean isFieldNotEmpty(String tableName, String fieldName);

    boolean isFieldContainEmptyValues(String tableName, String fieldName);

    void insertAllDataFromDraft(String draftCode, String targetCode, List<String> columns,
                                int offset, int limit,
                                LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countActualDataFromVersion(String versionTable, String draftTable,
                                          LocalDateTime publishTime, LocalDateTime closeTime);

    void insertActualDataFromVersion(String targetTable, String versionTable,
                                     String draftTable, Map<String, String> columns,
                                     int offset, int limit,
                                     LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countOldDataFromVersion(String versionTable, String draftTable,
                                       LocalDateTime publishTime, LocalDateTime closeTime);

    void insertOldDataFromVersion(String targetTable, String versionTable,
                                  String draftTable, List<String> columns,
                                  int offset, int limit,
                                  LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countClosedNowDataFromVersion(String versionTable, String draftTable,
                                             LocalDateTime publishTime, LocalDateTime closeTime);

    void insertClosedNowDataFromVersion(String targetTable, String versionTable,
                                        String draftTable, Map<String, String> columns,
                                        int offset, int limit,
                                        LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countNewValFromDraft(String draftTable, String versionTable,
                                    LocalDateTime publishTime, LocalDateTime closeTime);

    void insertNewDataFromDraft(String targetTable, String versionTable, String draftTable,
                                List<String> columns, int offset, int limit,
                                LocalDateTime publishTime, LocalDateTime closeTime);

    void deletePointRows(String targetCode);

    DataDifference getDataDifference(CompareDataCriteria criteria);
}

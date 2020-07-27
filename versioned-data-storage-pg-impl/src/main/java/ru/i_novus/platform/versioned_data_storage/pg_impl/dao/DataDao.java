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

    Map<String, String> getColumnDataTypes(String tableName);

    BigInteger countData(String tableName);

    void createDraftTable(String storageCode, List<Field> fields);

    void copyTable(String newTableName, String sourceTableName);

    void dropTable(String tableName);

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

    void updateReferenceInRows(String tableName, ReferenceFieldValue fieldValue, List<Object> systemIds);

    BigInteger countReferenceInRefRows(String tableName, ReferenceFieldValue fieldValue);

    void updateReferenceInRefRows(String tableName, ReferenceFieldValue fieldValue, int offset, int limit);

    void deleteEmptyRows(String draftCode);

    boolean isUnique(String storageCode, List<String> fieldNames, LocalDateTime publishTime);

    void updateSequence(String tableName);

    void createTriggers(String schemaName, String tableName);

    void createTriggers(String schemaName, String tableName, List<String> fieldNames);

    void updateHashRows(String tableName);

    void updateFtsRows(String tableName);

    void dropTriggers(String tableName);

    void createIndex(String schemaName, String tableName, String name, List<String> fields);

    void createFullTextSearchIndex(String schemaName, String tableName);

    void createLtreeIndex(String schemaName, String tableName, String field);

    void createHashIndex(String schemaName, String tableName);

    List<String> getFieldNames(String tableName, String sqlFieldNames);

    List<String> getFieldNames(String tableName);

    List<String> getHashUsedFieldNames(String tableName);

    String getFieldType(String tableName, String field);

    void alterDataType(String tableName, String field, String oldType, String newType);

    boolean isFieldNotEmpty(String tableName, String fieldName);

    boolean isFieldContainEmptyValues(String tableName, String fieldName);

    BigInteger countActualDataFromVersion(String versionTable, String draftTable,
                                          LocalDateTime publishTime, LocalDateTime closeTime);

    void insertActualDataFromVersion(String tableToInsert, String versionTable,
                                     String draftTable, Map<String, String> columns,
                                     int offset, int transactionSize,
                                     LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countOldDataFromVersion(String versionTable, String draftTable,
                                       LocalDateTime publishTime, LocalDateTime closeTime);

    void insertOldDataFromVersion(String tableToInsert, String versionTable,
                                  String draftTable, List<String> columns,
                                  int offset, int transactionSize,
                                  LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countClosedNowDataFromVersion(String versionTable, String draftTable,
                                             LocalDateTime publishTime, LocalDateTime closeTime);

    void insertClosedNowDataFromVersion(String tableToInsert, String versionTable,
                                        String draftTable, Map<String, String> columns,
                                        int offset, int transactionSize,
                                        LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countNewValFromDraft(String draftTable, String versionTable,
                                    LocalDateTime publishTime, LocalDateTime closeTime);

    void insertNewDataFromDraft(String tableToInsert, String versionTable, String draftTable,
                                List<String> columns, int offset, int transactionSize,
                                LocalDateTime publishTime, LocalDateTime closeTime);

    void insertDataFromDraft(String draftTable, String tableToInsert, List<String> columns,
                             int offset, int transactionSize,
                             LocalDateTime publishTime, LocalDateTime closeTime);

    void deletePointRows(String targetTable);

    DataDifference getDataDifference(CompareDataCriteria criteria);
}

package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import ru.i_novus.platform.datastorage.temporal.model.DataDifference;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.criteria.CompareDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;

import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.transaction.Transactional;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface DataDao {

    List<RowValue> getData(DataCriteria criteria);

    BigInteger getDataCount(DataCriteria criteria);

    RowValue getRowData(String tableName, List<String> fieldNames, Object systemId);

    List<RowValue> getRowData(String tableName, List<String> fieldNames, List<Object> systemIds);

    List<String> getNotExists(String tableName, LocalDateTime bdate, LocalDateTime edate, List<String> hashList);

    boolean tableStructureEquals(String tableName1, String tableName2);

    Map<String, String> getColumnDataTypes(String tableName);

    BigInteger countData(String tableName);

    @Transactional
    void createDraftTable(String tableName, List<Field> fields);

    @Transactional
    void copyTable(String newTableName, String sourceTableName);

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void dropTable(String tableName);

    boolean tableExists(String schemaName, String tableName);

    @Transactional
    void addColumnToTable(String tableName, String name, String type, String defaultValue);

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void deleteColumnFromTable(String tableName, String field);

    @Transactional
    void insertData(String tableName, List<RowValue> data);

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void loadData(String draftCode, String sourceStorageCode, List<String> fields,
                  LocalDateTime fromDate, LocalDateTime toDate);

    @Transactional
    void updateData(String tableName, RowValue rowValue);

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void deleteData(String tableName);

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void deleteData(String tableName, List<Object> systemIds);

    @Transactional
    void updateReferenceInRows(String tableName, ReferenceFieldValue fieldValue, List<Object> systemIds);

    BigInteger countReferenceInRefRows(String tableName, ReferenceFieldValue fieldValue);

    @Transactional
    void updateReferenceInRefRows(String tableName, ReferenceFieldValue fieldValue, int offset, int limit);

    @Transactional
    void deleteEmptyRows(String draftCode);

    boolean isUnique(String storageCode, List<String> fieldNames, LocalDateTime publishTime);

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void updateSequence(String tableName);

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void createTriggers(String tableName);

    @Transactional
    void createTriggers(String tableName, List<String> fields);

    @Transactional
    void updateHashRows(String tableName);

    @Transactional
    void updateFtsRows(String tableName);

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void dropTrigger(String tableName);

    @Transactional
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void createIndex(String tableName, String name, List<String> fields);

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void createFullTextSearchIndex(String tableName);

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void createLtreeIndex(String tableName, String field);

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    void createHashIndex(String tableName);

    List<String> getFieldNames(String tableName, String sqlFieldNames);

    List<String> getFieldNames(String tableName);

    List<String> getHashUsedFieldNames(String tableName);

    String getFieldType(String tableName, String field);

    void alterDataType(String tableName, String field, String oldType, String newType);

    boolean isFieldNotEmpty(String tableName, String fieldName);

    boolean isFieldContainEmptyValues(String tableName, String fieldName);

    BigInteger countActualDataFromVersion(String versionTable, String draftTable,
                                          LocalDateTime publishTime, LocalDateTime closeTime);

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    void insertActualDataFromVersion(String tableToInsert, String versionTable, String draftTable,
                                     Map<String, String> columns, int offset, int transactionSize,
                                     LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countOldDataFromVersion(String versionTable, String draftTable,
                                       LocalDateTime publishTime, LocalDateTime closeTime);

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    void insertOldDataFromVersion(String tableToInsert, String tableFromInsert,
                                  String draftTable, List<String> columns,
                                  int offset, int transactionSize,
                                  LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countClosedNowDataFromVersion(String versionTable, String draftTable,
                                             LocalDateTime publishTime, LocalDateTime closeTime);

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    void insertClosedNowDataFromVersion(String tableToInsert, String versionTable, String draftTable,
                                        Map<String, String> columns, int offset, int transactionSize,
                                        LocalDateTime publishTime, LocalDateTime closeTime);

    BigInteger countNewValFromDraft(String draftTable, String versionTable,
                                    LocalDateTime publishTime, LocalDateTime closeTime);

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    void insertNewDataFromDraft(String tableToInsert, String versionTable, String draftTable,
                                List<String> columns, int offset, int transactionSize,
                                LocalDateTime publishTime, LocalDateTime closeTime);

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    void insertDataFromDraft(String draftTable, String targetTable, List<String> columns,
                             int offset, int transactionSize,
                             LocalDateTime publishTime, LocalDateTime closeTime);

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    void deletePointRows(String targetTable);

    DataDifference getDataDifference(CompareDataCriteria criteria);
}

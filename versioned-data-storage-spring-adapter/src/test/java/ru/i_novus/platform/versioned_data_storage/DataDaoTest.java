package ru.i_novus.platform.versioned_data_storage;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;
import ru.i_novus.platform.datastorage.temporal.enums.FieldType;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.LongRowValue;
import ru.i_novus.platform.datastorage.temporal.model.criteria.BaseDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageCopyRequest;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.FieldFactory;
import ru.i_novus.platform.versioned_data_storage.config.VersionedDataStorageConfig;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;
import static ru.i_novus.platform.versioned_data_storage.DataTestUtils.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.HASH_EXPRESSION;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.StorageConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StorageUtils.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.addDoubleQuotes;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.addSingleQuotes;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JpaTestConfig.class, VersionedDataStorageConfig.class})
public class DataDaoTest {

    private static final Logger logger = LoggerFactory.getLogger(DataDaoTest.class);

    private static final String NEW_GOOD_SCHEMA_NAME = "data_good";
    private static final String NEW_BAD_SCHEMA_NAME = "data\"bad";

    private static final String INSERT_RECORD = "INSERT INTO %1$s.%2$s (%3$s)\n";
    private static final String INSERT_VALUES = "VALUES(%s)\n";

    private Field hashField;

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private DataDao dataDao;

    @Autowired
    private FieldFactory fieldFactory;

    @Before
    public void setUp() {
        hashField = fieldFactory.createField(SYS_HASH, FieldType.STRING);
    }

    @Test
    public void testSchemaExists() {

        assertTrue(dataDao.schemaExists(DATA_SCHEMA_NAME));
        assertTrue(dataDao.schemaExists(TEST_SCHEMA_NAME));
        assertFalse(dataDao.schemaExists(NONEXISTENT_SCHEMA_NAME));
    }

    @Test
    public void testFindExistentSchemas() {

        List<String> schemaNames = asList(DATA_SCHEMA_NAME, TEST_SCHEMA_NAME, NONEXISTENT_SCHEMA_NAME);

        List<String> expected = asList(DATA_SCHEMA_NAME, TEST_SCHEMA_NAME);
        List<String> actual = dataDao.findExistentSchemas(schemaNames);
        assertEquals(expected, actual);
    }

    @Test
    public void testFindExistentTableSchemas() {

        List<String> schemaNames = asList(DATA_SCHEMA_NAME, TEST_SCHEMA_NAME, NONEXISTENT_SCHEMA_NAME);
        List<String> expected = singletonList(DATA_SCHEMA_NAME);

        final String tableName = "findExistentTableSchemas";
        List<Field> fields = newTestFields();

        dataDao.createDraftTable(tableName, fields);
        List<String> actual = dataDao.findExistentTableSchemas(schemaNames, tableName);
        assertEquals(expected, actual);
    }

    @Test
    @Transactional
    public void testStorageExists() {

        String tableName = "test_storage_exists";
        testStorageExists(null, tableName);
        testStorageExists(TEST_SCHEMA_NAME, tableName);
    }

    private void testStorageExists(String schemaName, String tableName) {

        String storageCode = toStorageCode(schemaName, tableName);
        assertFalse(dataDao.storageExists(storageCode));

        String ddlFormat = "CREATE TABLE %1$s.%2$s (\n" +
                "  " + addDoubleQuotes(SYS_PRIMARY_COLUMN) + " bigserial NOT NULL,\n" +
                "  " + addDoubleQuotes(FIELD_ID_CODE) + " integer,\n" +
                "  " + addDoubleQuotes(FIELD_NAME_CODE) + " varchar(32),\n" +
                "  " + addDoubleQuotes(SYS_HASH) + " char(32)\n" +
                ");";

        String ddl = String.format(ddlFormat, getSchemaNameOrDefault(schemaName), addDoubleQuotes(tableName));
        entityManager.createNativeQuery(ddl).executeUpdate();
        assertTrue(dataDao.storageExists(storageCode));
        assertTrue(dataDao.storageFieldExists(storageCode, FIELD_ID_CODE));
        assertTrue(dataDao.storageFieldExists(storageCode, FIELD_NAME_CODE));
        assertTrue(dataDao.storageFieldExists(storageCode, SYS_HASH));
    }

    @Test
    public void testCreateSchema() {

        try {
            dataDao.createSchema(NEW_GOOD_SCHEMA_NAME);

        } catch (Exception e) {
            fail("Error creating schema '" + NEW_GOOD_SCHEMA_NAME + "':\n" + e.getMessage());
        }

        try {
            dataDao.createSchema(NEW_GOOD_SCHEMA_NAME);

        } catch (Exception e) {
            fail("Error creating same schema '" + NEW_GOOD_SCHEMA_NAME + "':\n" + e.getMessage());
        }

        try {
            dataDao.createSchema(NEW_BAD_SCHEMA_NAME);
            fail("Error creating bad schema '" + NEW_BAD_SCHEMA_NAME + "'");

        } catch (Exception e) {
            // Nothing to do.
        }
    }

    @Test
    @Transactional
    public void testProcessTable() {

        String tableName = newTestTableName();
        List<Field> fields = newTestFields();

        testCreateDraftTable(null, tableName, fields);
        testCreateDraftTable(TEST_SCHEMA_NAME, tableName, fields);

        testDropTable(null, tableName);
        testDropTable(TEST_SCHEMA_NAME, tableName);
    }

    private void testCreateDraftTable(String schemaName, String tableName, List<Field> fields) {

        String storageCode = toStorageCode(schemaName, tableName);

        dataDao.createDraftTable(storageCode, fields);
        assertTrue(dataDao.storageExists(storageCode));
    }

    private void testDropTable(String schemaName, String tableName) {

        String storageCode = toStorageCode(schemaName, tableName);

        dataDao.dropTable(storageCode);
        assertFalse(dataDao.storageExists(storageCode));
    }

    @Test
    @Transactional
    public void testGetData() {

        String tableName = newTestTableName();
        List<Field> fields = newTestFields();

        testGetData(null, tableName, fields, dataNames);
        testGetData(TEST_SCHEMA_NAME, tableName, fields, testNames);
    }

    private void testGetData(String schemaName, String tableName,
                             List<Field> fields, List<String> nameValues) {

        String storageCode = toStorageCode(schemaName, tableName);
        dataDao.createDraftTable(storageCode, fields);

        String escapedTableName = addDoubleQuotes(tableName);
        String columns = fields.stream()
                .map(field -> addDoubleQuotes(field.getName()))
                .reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        columns += ", " + addDoubleQuotes(SYS_HASH);

        String sqlValuesFormat = "%1$s" + ", " + String.format(HASH_EXPRESSION, "%1$s");
        String sqlInsert = String.format(INSERT_RECORD, getSchemaNameOrDefault(schemaName), escapedTableName, columns) +
                String.format(INSERT_VALUES, sqlValuesFormat);
        insertValues(sqlInsert, nameValues);
        List<RowValue> dataValues = dataDao.getData(toCriteria(storageCode, fields));
        assertValues(dataValues, nameValues);
    }

    @Test
    @SuppressWarnings("java:S5778")
    public void testGetDataForNonExistentSchema() {

        String tableName = newTestTableName();
        List<Field> fields = newTestFields();
        try {
            dataDao.getData(toCriteria(NONEXISTENT_SCHEMA_NAME, tableName, fields));
            fail();

        } catch (PersistenceException e) {
            assertNotNull(e.getMessage());

        } catch (Exception e) {
            fail();
        }
    }

    private void insertValues(String sqlInsert, List<String> nameValues) {

        IntStream.range(0, nameValues.size()).forEach(index -> {
            String sql = String.format(sqlInsert, indexToId(index) + ", " + addSingleQuotes(nameValues.get(index)));
            entityManager.createNativeQuery(sql).executeUpdate();
        });
    }

    @Test
    @Transactional
    public void testProcessData() {

        String tableName = newTestTableName();
        List<Field> fields = newTestFields();

        testProcessData(null, tableName, fields, dataNames);
        testProcessData(TEST_SCHEMA_NAME, tableName, fields, testNames);
    }

    private void testProcessData(String schemaName, String tableName,
                                 List<Field> fields, List<String> nameValues) {

        logger.info("Starting: Process data for {}", getSchemaNameOrDefault(schemaName));

        String storageCode = toStorageCode(schemaName, tableName);
        dataDao.createDraftTable(storageCode, fields);

        logger.info("Stage 1: Process data: insertData");
        dataDao.insertData(storageCode, nameValuesToRowValues(fields, nameValues));
        assertData(storageCode, fields, nameValues);

        logger.info("Stage 2: Process data: changeData");
        int allCount = nameValues.size();
        int changedCount = 2;
        List<BigInteger> changedIds = IntStream.range(allCount - changedCount, allCount)
                .mapToObj(DataTestUtils::indexToId).collect(toList());

        List<RowValue> dataValues = dataDao.getData(toCriteria(storageCode, fields));

        logger.info("Stage 3: Process data: updateData");
        final String updateNamePrefix = "updated: ";
        dataValues.stream()
                .filter(rowValue -> changedIds.contains(getRowIdFieldValue(rowValue)))
                .map(rowValue -> {
                    FieldValue idFieldValue = rowValue.getFieldValue(FIELD_ID_CODE);
                    FieldValue nameFieldValue = rowValue.getFieldValue(FIELD_NAME_CODE);
                    nameFieldValue.setValue(updateNamePrefix + nameFieldValue.getValue());

                    return new LongRowValue((Long) rowValue.getSystemId(), asList(idFieldValue, nameFieldValue));
                })
                .forEachOrdered(rowValue -> dataDao.updateData(storageCode, rowValue));

        List<String> dataNamesAfterUpdate = IntStream.range(0, allCount)
                .mapToObj(index -> ((index < allCount - changedCount) ? "" : updateNamePrefix) + nameValues.get(index))
                .collect(toList());
        assertData(storageCode, fields, dataNamesAfterUpdate);

        logger.info("Stage 4: Process data: deleteData");
        List<Object> deletedSystemIds = dataValues.stream()
                .filter(rowValue -> changedIds.contains(getRowIdFieldValue(rowValue)))
                .map(RowValue::getSystemId)
                .collect(toList());

        dataDao.deleteData(storageCode, deletedSystemIds);
        List<String> dataNamesAfterDelete = new ArrayList<>(nameValues.subList(0, allCount - changedCount));
        assertData(storageCode, fields, dataNamesAfterDelete);

        logger.info("Stage 5: Process data: deleteAllData");
        dataDao.deleteData(storageCode);
        assertData(storageCode, fields, emptyList());

        logger.info("Finishing: Process data for {}", getSchemaNameOrDefault(schemaName));
    }

    @SuppressWarnings("SameParameterValue")
    private void assertData(String storageCode, List<Field> fields, List<String> nameValues) {

        List<RowValue> dataValues = dataDao.getData(toCriteria(storageCode, fields));
        assertValues(dataValues, nameValues);
    }

    @Test
    public void testCopyTableData() {

        String sourceName = newTestTableName();
        String sourceCode = toStorageCode(null, sourceName);
        List<Field> fields = newTestFields();

        dataDao.createDraftTable(sourceCode, fields);
        dataDao.insertData(sourceCode, nameValuesToRowValues(fields, dataNames));

        List<RowValue> dataValues = dataDao.getData(toCriteria(sourceCode, fields));
        assertValues(dataValues, dataNames);

        String targetName = newTestTableName();
        String targetCode = toStorageCode(TEST_SCHEMA_NAME, targetName);

        dataDao.copyTable(sourceCode, targetCode);
        List<RowValue> emptyDataValues = dataDao.getData(toCriteria(targetCode, fields));
        assertEquals(0, emptyDataValues == null ? 0 : emptyDataValues.size());

        StorageCopyRequest request = new StorageCopyRequest(sourceCode, targetCode, null, null, null);
        request.setPage(BaseDataCriteria.MIN_PAGE);
        request.setSize(dataNames.size());

        dataDao.copyTableData(request);
        dataValues = dataDao.getData(toCriteria(targetCode, fields));
        assertValues(dataValues, dataNames);
    }

    private String newTestTableName() {

        return "data_test_" + generateStorageName();
    }

    private List<Field> newTestFields() {

        return newIdNameFields();
    }

    private List<RowValue> nameValuesToRowValues(List<Field> fields, List<String> nameValues) {

        return toRowValues(nameValues.size(), index -> toRowValue(index, nameValues.get(index), fields));
    }

    private RowValue toRowValue(int index, String name, List<Field> fields) {

        RowValue result = toIdNameRowValue(index, name, fields);

        FieldValue hashValue = hashField.valueOf(String.valueOf(index));
        result.getFieldValues().add(hashValue);

        return result;
    }
}
package ru.i_novus.platform.versioned_data_storage;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;
import ru.i_novus.platform.datastorage.temporal.enums.FieldType;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.LongRowValue;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.value.IntegerFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.model.value.StringFieldValue;
import ru.i_novus.platform.datastorage.temporal.service.FieldFactory;
import ru.i_novus.platform.versioned_data_storage.config.VersionedDataStorageConfig;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceException;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.model.StorageConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StorageUtils.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.addDoubleQuotes;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.addSingleQuotes;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.HASH_EXPRESSION;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JpaTestConfig.class, VersionedDataStorageConfig.class})
public class DataDaoTest {

    private static final String TEST_SCHEMA_NAME = "data_test";
    private static final String NULL_SCHEMA_NAME = "data_null";
    private static final String NEW_GOOD_SCHEMA_NAME = "data_good";
    private static final String NEW_BAD_SCHEMA_NAME = "data\"bad";

    private static final String INSERT_RECORD = "INSERT INTO %1$s.%2$s (%3$s)\n";
    private static final String INSERT_VALUES = "VALUES(%s)\n";

    private static final String FIELD_ID_CODE = "id";
    private static final String FIELD_NAME_CODE = "name";

    private final List<String> dataNames = Arrays.asList("первый", "второй");
    private final List<String> testNames = Arrays.asList("first", "second");

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
    public void testSchemaExists() {

        assertTrue(dataDao.schemaExists(DATA_SCHEMA_NAME));
        assertTrue(dataDao.schemaExists(TEST_SCHEMA_NAME));
        assertFalse(dataDao.schemaExists(NULL_SCHEMA_NAME));
    }

    @Test
    @Transactional
    public void testStorageExists() {

        String tableName = "test_table_exists";
        testStorageExists(null, tableName);
        testStorageExists(TEST_SCHEMA_NAME, tableName);
    }

    private void testStorageExists(String schemaName, String tableName) {

        String storageCode = toStorageCode(schemaName, tableName);
        assertFalse(dataDao.storageExists(storageCode));

        String ddlFormat = "CREATE TABLE %1$s.%2$s (\n" +
                "  " + addDoubleQuotes(SYS_PRIMARY_COLUMN) + " bigserial NOT NULL,\n" +
                "  " + FIELD_ID_CODE + " integer,\n" +
                "  " + FIELD_NAME_CODE + " varchar(32),\n" +
                "  " + addDoubleQuotes(SYS_HASH) + " char(32)\n" +
                ");";

        String ddl = String.format(ddlFormat, getSchemaNameOrDefault(schemaName), addDoubleQuotes(tableName));
        entityManager.createNativeQuery(ddl).executeUpdate();
        assertTrue(dataDao.storageExists(storageCode));
    }

    @Test
    public void testCreateDraftTable() {

        String tableName = newTestTableName();
        List<Field> fields = newTestFields();

        testCreateDraftTable(null, tableName, fields);
        testCreateDraftTable(TEST_SCHEMA_NAME, tableName, fields);
    }

    private void testCreateDraftTable(String schemaName, String tableName, List<Field> fields) {

        String storageCode = toStorageCode(schemaName, tableName);

        dataDao.createDraftTable(storageCode, fields);
        assertTrue(dataDao.storageExists(storageCode));
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

        dataDao.createDraftTable(toStorageCode(schemaName, tableName), fields);

        String escapedTableName = addDoubleQuotes(tableName);
        String columns = fields.stream()
                .map(field -> addDoubleQuotes(field.getName()))
                .reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        columns += ", " + addDoubleQuotes(SYS_HASH);

        String sqlValuesFormat = "%1$s" + ", " + String.format(HASH_EXPRESSION, "%1$s");
        String sqlInsert = String.format(INSERT_RECORD, getSchemaNameOrDefault(schemaName), escapedTableName, columns) +
                String.format(INSERT_VALUES, sqlValuesFormat);
        insertValues(sqlInsert, nameValues);
        List<RowValue> dataValues = getData(schemaName, tableName, fields);
        assertValues(dataValues, nameValues);
    }

    @Test
    public void testNullGetData() {

        String tableName = newTestTableName();
        List<Field> fields = newTestFields();
        try {
            getData(NULL_SCHEMA_NAME, tableName, fields);
            fail();

        } catch (PersistenceException e) {
            assertNotNull(e.getMessage());

        } catch (Exception e) {
            fail();
        }
    }

    private void insertValues(String sqlInsert, List<String> nameValues) {

        IntStream.range(0, nameValues.size()).forEach(index -> {
            String sql = String.format(sqlInsert, index + ", " + addSingleQuotes(nameValues.get(index)));
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

        dataDao.createDraftTable(toStorageCode(schemaName, tableName), fields);

        // Вставка записей.
        dataDao.insertData(toStorageCode(schemaName, tableName), toRowValues(fields, nameValues));
        List<RowValue> dataValues = getData(schemaName, tableName, fields);
        assertValues(dataValues, nameValues);

        //dataDao.updateData();
    }

    @Test
    public void testCopyTableData() {

        String sourceName = newTestTableName();
        String sourceCode = toStorageCode(null, sourceName);

        List<Field> fields = newTestFields();

        dataDao.createDraftTable(sourceCode, fields);
        dataDao.insertData(sourceCode, toRowValues(fields, dataNames));
        List<RowValue> dataValues = getData(null, sourceName, fields);
        assertValues(dataValues, dataNames);

        String targetName = newTestTableName();
        String targetCode = toStorageCode(TEST_SCHEMA_NAME, targetName);

        dataDao.copyTable(sourceCode, targetCode);
        List<RowValue> emptyDataValues = getData(TEST_SCHEMA_NAME, targetName, fields);
        assertEquals(0, emptyDataValues == null ? 0 : emptyDataValues.size());

        dataDao.copyTableData(sourceCode, targetCode, 0, dataNames.size());
        dataValues = getData(TEST_SCHEMA_NAME, targetName, fields);
        assertValues(dataValues, dataNames);
    }

    private String newTestTableName() {

        return "data_test_" + generateStorageName();
    }

    private List<Field> newTestFields() {

        List<Field> fields = new ArrayList<>();
        Field idField = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field nameField = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        fields.add(idField);
        fields.add(nameField);

        return fields;
    }

    private Field findFieldOrThrow(String name, List<Field> fields) {
        return fields.stream()
                .filter(field -> name.equals(field.getName()))
                .findFirst().orElseThrow(() ->
                        new IllegalArgumentException("field '" + name + "' is not found"));
    }

    private List<RowValue> getData(String schemaName, String tableName, List<Field> fields) {

        DataCriteria criteria = new DataCriteria(toStorageCode(schemaName, tableName), null, null, fields);
        return dataDao.getData(criteria);
    }

    private List<RowValue> toRowValues(List<Field> fields, List<String> nameValues) {

        return IntStream.range(0, nameValues.size()).boxed()
                .map(index -> toRowValue(index, nameValues.get(index), fields))
                .collect(toList());
    }

    private RowValue toRowValue(int id, String name, List<Field> fields) {

        FieldValue idValue = findFieldOrThrow(FIELD_ID_CODE, fields).valueOf(BigInteger.valueOf(id));
        FieldValue nameValue = findFieldOrThrow(FIELD_NAME_CODE, fields).valueOf(name);
        FieldValue hashValue = hashField.valueOf(String.valueOf(id));
        return new LongRowValue(idValue, nameValue, hashValue);
    }

    private void assertValues(List<RowValue> dataValues, List<String> nameValues) {

        assertNotNull(dataValues);
        assertEquals(nameValues.size(), dataValues.size());

        IntStream.range(0, nameValues.size()).forEach(index -> {
            StringFieldValue nameValue = dataValues.stream()
                    .filter(rowValue -> {
                        IntegerFieldValue idValue = (IntegerFieldValue) rowValue.getFieldValue(FIELD_ID_CODE);
                        BigInteger value = idValue != null ? idValue.getValue() : null;
                        return value != null && value.equals(BigInteger.valueOf(index));
                    })
                    .map(rowValue -> (StringFieldValue) rowValue.getFieldValue(FIELD_NAME_CODE))
                    .findFirst().orElse(null);
            assertNotNull(nameValue);
            assertEquals(nameValues.get(index), nameValue.getValue());
        });
    }
}
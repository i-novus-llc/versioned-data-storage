package ru.i_novus.platform.versioned_data_storage;

import org.junit.Assert;
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
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.FieldFactory;
import ru.i_novus.platform.versioned_data_storage.config.VersionedDataStorageConfig;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.*;
import java.util.stream.IntStream;

import static ru.i_novus.platform.datastorage.temporal.model.DataConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.DataUtil.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.escapeTableName;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.getSchemaName;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JpaTestConfig.class, VersionedDataStorageConfig.class})
public class DataDaoTest {

    private static final Logger logger = LoggerFactory.getLogger(DataDaoTest.class);

    private static final String TEST_SCHEMA_NAME = "data_test";

    private static final String FIELD_ID_CODE = "id";
    private static final String FIELD_NAME_CODE = "name";

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private DataDao dataDao;

    @Autowired
    private FieldFactory fieldFactory;

    @Test
    public void testSchemaExists() {

        Assert.assertTrue(dataDao.schemaExists(DATA_SCHEMA_NAME));
        Assert.assertTrue(dataDao.schemaExists(TEST_SCHEMA_NAME));
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
        Assert.assertFalse(dataDao.storageExists(storageCode));

        String ddlFormat = "CREATE TABLE %1$s.%2$s (\n" +
                "  " + addDoubleQuotes(SYS_PRIMARY_COLUMN) + " bigserial NOT NULL,\n" +
                "  " + FIELD_ID_CODE + " integer,\n" +
                "  " + FIELD_NAME_CODE + " varchar(32),\n" +
                "  " + addDoubleQuotes(SYS_HASH) + " char(32)\n" +
                ");";

        String ddl = String.format(ddlFormat, getSchemaName(schemaName), addDoubleQuotes(tableName));
        entityManager.createNativeQuery(ddl).executeUpdate();
        Assert.assertTrue(dataDao.storageExists(storageCode));
    }

    @Test
    public void testCreateDraftTable() {

        String tableName = "test_" + UUID.randomUUID().toString();
        List<Field> fields = getTestFields();

        testCreateDraftTable(null, tableName, fields);
        testCreateDraftTable(TEST_SCHEMA_NAME, tableName, fields);
    }

    private void testCreateDraftTable(String schemaName, String tableName, List<Field> fields) {

        String storageCode = toStorageCode(schemaName, tableName);

        dataDao.createDraftTable(storageCode, fields);
        Assert.assertTrue(dataDao.storageExists(storageCode));
    }

    @Test
    @Transactional
    public void testGetData() {

        String tableName = "test_" + UUID.randomUUID().toString();
        String escapedTableName = escapeTableName(tableName);

        List<Field> fields = getTestFields();
        String columnsStr = fields.stream()
                .map(field -> "" + field.getName() + "")
                .reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        columnsStr += ", " + addDoubleQuotes(SYS_HASH);

        dataDao.createDraftTable(tableName, fields);

        final String dataName1 = "первый";
        final String dataName2 = "второй";
        final List<String> dataNames = Arrays.asList(dataName1, dataName2);

        String sqlValuesFormat = "%1$s" + ", " + String.format(HASH_EXPRESSION, "%1$s");

        String sqlInsert = String.format(INSERT_QUERY_TEMPLATE,
                DATA_SCHEMA_NAME, escapedTableName, columnsStr, sqlValuesFormat);
        insertValues(sqlInsert, dataNames);
        testGetIdName(null, tableName, fields, dataNames);
        testGetIdName(TEST_SCHEMA_NAME, tableName, fields, dataNames);

        final String testName1 = "first";
        final String testName2 = "second";
        final List<String> testNames = Arrays.asList(testName1, testName2);

        dataDao.createDraftTable(TEST_SCHEMA_NAME + NAME_SEPARATOR + tableName, fields);

        sqlInsert = String.format(INSERT_QUERY_TEMPLATE, TEST_SCHEMA_NAME,
                escapedTableName, columnsStr, sqlValuesFormat);
        insertValues(sqlInsert, testNames);
        testGetIdName(TEST_SCHEMA_NAME, tableName, fields, testNames);
    }

    private void insertValues(String sqlInsert, List<String> nameValues) {

        IntStream.range(0, nameValues.size()).forEach(index -> {
            String sql = String.format(sqlInsert, index + ", " + addSingleQuotes(nameValues.get(index)));
            entityManager.createNativeQuery(sql).executeUpdate();
        });
    }

    private void testGetIdName(String schemaName, String tableName,
                               List<Field> fields, List<String> nameValues) {

        DataCriteria criteria = new DataCriteria(tableName, null, null, fields);
        criteria.setSchemaName(schemaName);
        List<RowValue> dataValues = dataDao.getData(criteria);
        Assert.assertNotNull(dataValues);
        Assert.assertEquals(2, dataValues.size());

        IntStream.range(0, nameValues.size()).forEach(index -> {
            FieldValue nameValue = dataValues.get(index).getFieldValue(FIELD_NAME_CODE);
            Assert.assertNotNull(nameValue);
            Assert.assertTrue(nameValues.contains((String) nameValue.getValue()));
        });
    }

    private List<Field> getTestFields() {

        List<Field> fields = new ArrayList<>();
        Field fieldId = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field fieldName = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        fields.add(fieldId);
        fields.add(fieldName);

        return fields;
    }

    private String toStorageCode(String schemaName, String tableName) {
        return isNullOrEmpty(schemaName) ? tableName : schemaName + NAME_SEPARATOR + tableName;
    }
}
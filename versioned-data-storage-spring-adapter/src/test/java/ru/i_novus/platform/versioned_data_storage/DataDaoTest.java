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
import ru.i_novus.platform.datastorage.temporal.service.FieldFactory;
import ru.i_novus.platform.versioned_data_storage.config.VersionedDataStorageConfig;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static ru.i_novus.platform.datastorage.temporal.model.DataConstants.DATA_SCHEMA_NAME;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.DataUtil.addDoubleQuotes;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JpaTestConfig.class, VersionedDataStorageConfig.class})
public class DataDaoTest {

    private static final Logger logger = LoggerFactory.getLogger(DataDaoTest.class);

    private static final String TEST_SCHEMA_NAME = "data_test";

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
    public void testTableExists() {

        String tableName = "test_table_exists";

        Assert.assertFalse(dataDao.tableExists(DATA_SCHEMA_NAME, tableName));

        String ddlFormat = "CREATE TABLE %1$s.%2$s (\n" +
                "  id integer,\n" +
                "  name varchar(32)\n" +
                ");";

        String ddl = String.format(ddlFormat, DATA_SCHEMA_NAME, addDoubleQuotes(tableName));
        entityManager.createNativeQuery(ddl).executeUpdate();

        Assert.assertTrue(dataDao.tableExists(DATA_SCHEMA_NAME, tableName));
    }

    @Test
    public void testCreateDraftTable() {

        String tableName = "test_" + UUID.randomUUID().toString();
        List<Field> fields = getTestFields();

        dataDao.createDraftTable(DATA_SCHEMA_NAME, tableName, fields);
        Assert.assertTrue(dataDao.tableExists(DATA_SCHEMA_NAME, tableName));

        dataDao.createDraftTable(TEST_SCHEMA_NAME, tableName, fields);
        Assert.assertTrue(dataDao.tableExists(TEST_SCHEMA_NAME, tableName));
    }

    private List<Field> getTestFields() {

        List<Field> fields = new ArrayList<>();
        Field fieldId = fieldFactory.createField("ID", FieldType.INTEGER);
        Field fieldName = fieldFactory.createField("NAME", FieldType.STRING);
        fields.add(fieldId);
        fields.add(fieldName);

        return fields;
    }
}
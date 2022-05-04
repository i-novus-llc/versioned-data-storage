package ru.i_novus.platform.versioned_data_storage.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.StorageConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StorageUtils.*;

public class StorageUtilsTest {

    private static final String EXPECTED_NAME = "Abc_123-";
    private static final List<String> TEST_NAMES = List.of(
            "Abc_123-",
            "йцуAbc_123-",
            "Abc_№#123-",
            "Abc_123-$%^",
            "йцуAbc_№#123-$%^"
    );

    private static final String VERY_LONG_NAME = RandomStringUtils.randomAlphanumeric(100);

    private static final String EXPECTED_SCHEMA_NAME = "abc_1230";
    private static final List<String> TEST_SCHEMA_NAMES = List.of(
            "Abc_1230",
            "Abc_123-",
            "Abc_123й",
            "Abc_123#",
            "Abc_123№"
    );

    private static final String WRONG_SCHEMA_NAME = "йцу№#$%^";
    private static final String WRONG_SCHEMA_REPLACE_NAME = SCHEMA_NAME_WRONG_CHAR_REPLACE.repeat(WRONG_SCHEMA_NAME.length());

    private static final String TEST_SCHEMA_NAME = "test_schema";
    private static final String TEST_TABLE_NAME = "test-table";
    private static final String DATA_STORAGE_CODE = DATA_SCHEMA_NAME + CODE_SEPARATOR + TEST_TABLE_NAME;
    private static final String TEST_STORAGE_CODE = TEST_SCHEMA_NAME + CODE_SEPARATOR + TEST_TABLE_NAME;
    private static final String DATA_SCHEMA_TABLE_NAME = DATA_SCHEMA_NAME + NAME_SEPARATOR + '"' + TEST_TABLE_NAME + '"';
    private static final String TEST_SCHEMA_TABLE_NAME = TEST_SCHEMA_NAME + NAME_SEPARATOR + '"' + TEST_TABLE_NAME + '"';

    private static final String TEST_TABLE_ALIAS = "t";
    private static final String TEST_FIELD_NAME = "name";
    private static final String TEST_FIELD_TYPE = "varchar";

    private static final String TEST_SYSTEM_FIELD_NAME = "system_name";
    private static final String ESCAPED_SYSTEM_FIELD_NAME = '"' + TEST_SYSTEM_FIELD_NAME + '"';

    private static final String TEST_CUSTOM_FIELD_NAME = "йцуAbc_№#123-$%^field";
    private static final String EXPECTED_CUSTOM_FIELD_NAME = "Abc_123-field";
    private static final String ESCAPED_CUSTOM_FIELD_NAME = '"' + EXPECTED_CUSTOM_FIELD_NAME + '"';

    @Test
    public void testSqlName() {

        assertEquals("", sqlName(null));
        assertEquals("", sqlName(""));
        assertEquals("", sqlName(" "));

        TEST_NAMES.forEach(name ->
            assertEquals(EXPECTED_NAME, sqlName(name))
        );

        assertTrue(sqlName(VERY_LONG_NAME).length() <= SQL_NAME_MAX_LENGTH);
    }

    @Test
    public void testLimitSqlName() {

        assertTrue(limitSqlName(VERY_LONG_NAME).length() <= SQL_NAME_MAX_LENGTH);
    }

    @Test
    public void testSqlSchemaName() {

        assertEquals("", sqlSchemaName(null));
        assertEquals("", sqlSchemaName(""));

        TEST_SCHEMA_NAMES.forEach(name ->
                assertEquals(EXPECTED_SCHEMA_NAME, sqlSchemaName(name))
        );

        assertEquals(WRONG_SCHEMA_REPLACE_NAME, sqlSchemaName(WRONG_SCHEMA_NAME));

        assertTrue(sqlSchemaName(VERY_LONG_NAME).length() <= SQL_NAME_MAX_LENGTH);
    }

    @Test
    public void testEscapeSystemName() {

        assertEquals("\"null\"", escapeSystemName(null));
        assertEquals("\"\"", escapeSystemName(""));

        assertEquals(ESCAPED_SYSTEM_FIELD_NAME, escapeSystemName(TEST_SYSTEM_FIELD_NAME));
    }

    @Test
    public void testEscapeCustomName() {

        assertEquals("", escapeCustomName(null));
        assertEquals("", escapeCustomName(""));

        String expectedName = '"' + EXPECTED_NAME + '"';
        TEST_NAMES.forEach(name ->
                assertEquals(expectedName, escapeCustomName(name))
        );
    }

    @Test
    public void testToSchemaName() {

        assertEquals(DATA_SCHEMA_NAME, toSchemaName(null));
        assertEquals(DATA_SCHEMA_NAME, toSchemaName(""));
        assertEquals(DATA_SCHEMA_NAME, toSchemaName(" "));

        assertEquals(DATA_SCHEMA_NAME, toSchemaName(TEST_TABLE_NAME));
        assertEquals(DATA_SCHEMA_NAME, toSchemaName(DATA_STORAGE_CODE));
        assertEquals(DATA_SCHEMA_NAME, toSchemaName(CODE_SEPARATOR));
        assertEquals(DATA_SCHEMA_NAME, toSchemaName(CODE_SEPARATOR + TEST_TABLE_NAME));
        assertEquals(TEST_SCHEMA_NAME, toSchemaName(TEST_STORAGE_CODE));

        assertEquals(SCHEMA_NAME_WRONG_CHAR_REPLACE, toSchemaName(" " + CODE_SEPARATOR + TEST_TABLE_NAME));

        TEST_SCHEMA_NAMES.forEach(name ->
                assertEquals(EXPECTED_SCHEMA_NAME, toSchemaName(name + CODE_SEPARATOR + TEST_TABLE_NAME))
        );
    }

    @Test
    public void testToTableName() {

        assertEquals("", toTableName(null));
        assertEquals("", toTableName(""));
        assertEquals("", toTableName(CODE_SEPARATOR));

        assertEquals(TEST_TABLE_NAME, toTableName(TEST_TABLE_NAME));
        assertEquals(TEST_TABLE_NAME, toTableName(DATA_STORAGE_CODE));
        assertEquals(TEST_TABLE_NAME, toTableName(CODE_SEPARATOR + TEST_TABLE_NAME));
        assertEquals(TEST_TABLE_NAME, toTableName(TEST_STORAGE_CODE));

        TEST_NAMES.forEach(name ->
                assertEquals(EXPECTED_NAME, toTableName(TEST_SCHEMA_NAME + CODE_SEPARATOR + name))
        );
    }

    @Test
    public void testToStorageCode() {

        assertEquals(TEST_TABLE_NAME, toStorageCode(null, TEST_TABLE_NAME));
        assertEquals(TEST_TABLE_NAME, toStorageCode("", TEST_TABLE_NAME));

        assertEquals(TEST_TABLE_NAME, toStorageCode(DATA_SCHEMA_NAME, TEST_TABLE_NAME));
        assertEquals(TEST_STORAGE_CODE, toStorageCode(TEST_SCHEMA_NAME, TEST_TABLE_NAME));
    }

    @Test
    public void testIsDefaultSchema() {

        assertTrue(isDefaultSchema(null));
        assertTrue(isDefaultSchema(""));

        assertTrue(isDefaultSchema(DATA_SCHEMA_NAME));
        assertFalse(isDefaultSchema(TEST_SCHEMA_NAME));
    }

    @Test
    public void testIsValidSchemaName() {

        assertFalse(isValidSchemaName(null));
        assertFalse(isValidSchemaName(""));

        assertTrue(isValidSchemaName(TEST_SCHEMA_NAME));
        assertFalse(isValidSchemaName("0" + TEST_SCHEMA_NAME));

        TEST_SCHEMA_NAMES.forEach(name ->
                assertFalse(isValidSchemaName(name))
        );
    }

    @Test
    public void testEscapeSchemaName() {

        assertEquals(DATA_SCHEMA_NAME, escapeSchemaName(null));
        assertEquals(DATA_SCHEMA_NAME, escapeSchemaName(""));

        assertEquals(DATA_SCHEMA_NAME, escapeSchemaName(DATA_SCHEMA_NAME));
        assertEquals(WRONG_SCHEMA_REPLACE_NAME, escapeSchemaName(WRONG_SCHEMA_NAME));

        TEST_SCHEMA_NAMES.forEach(name ->
                assertEquals(EXPECTED_SCHEMA_NAME, escapeSchemaName(name))
        );

        assertTrue(escapeSchemaName(VERY_LONG_NAME).length() <= SQL_NAME_MAX_LENGTH);
    }

    @Test
    public void testEscapeTableName() {

        assertEquals(DATA_SCHEMA_TABLE_NAME, escapeTableName(null, TEST_TABLE_NAME));
        assertEquals(DATA_SCHEMA_TABLE_NAME, escapeTableName("", TEST_TABLE_NAME));

        assertEquals(DATA_SCHEMA_TABLE_NAME, escapeTableName(DATA_SCHEMA_NAME, TEST_TABLE_NAME));
        assertEquals(TEST_SCHEMA_TABLE_NAME, escapeTableName(TEST_SCHEMA_NAME, TEST_TABLE_NAME));
    }

    @Test
    public void testEscapeStorageTableName() {

        assertEquals(DATA_SCHEMA_TABLE_NAME, escapeStorageTableName(TEST_TABLE_NAME));
        assertEquals(DATA_SCHEMA_TABLE_NAME, escapeStorageTableName(TEST_TABLE_NAME));

        assertEquals(DATA_SCHEMA_TABLE_NAME, escapeStorageTableName(DATA_STORAGE_CODE));
        assertEquals(TEST_SCHEMA_TABLE_NAME, escapeStorageTableName(TEST_STORAGE_CODE));
    }

    @Test
    public void testAliasColumnName() {

        assertEquals(TEST_TABLE_ALIAS + NAME_SEPARATOR + TEST_FIELD_NAME,
                aliasColumnName(TEST_TABLE_ALIAS, TEST_FIELD_NAME));
    }

    @Test
    public void testTypedColumnName() {

        assertEquals(TEST_FIELD_NAME + " " + TEST_FIELD_TYPE,
                typedColumnName(TEST_FIELD_NAME, TEST_FIELD_TYPE));
    }

    @Test
    public void testEscapeSystemFieldName() {

        assertEquals(ESCAPED_SYSTEM_FIELD_NAME, escapeSystemName(TEST_SYSTEM_FIELD_NAME));
    }

    @Test
    public void testAliasSystemFieldName() {

        assertEquals(ESCAPED_SYSTEM_FIELD_NAME, aliasSystemFieldName(null, TEST_SYSTEM_FIELD_NAME));
        assertEquals(TEST_TABLE_ALIAS + NAME_SEPARATOR + ESCAPED_SYSTEM_FIELD_NAME,
                aliasSystemFieldName(TEST_TABLE_ALIAS, TEST_SYSTEM_FIELD_NAME));
    }

    @Test
    public void testEscapeFieldName() {

        assertEquals(ESCAPED_CUSTOM_FIELD_NAME, escapeFieldName(TEST_CUSTOM_FIELD_NAME));
    }

    @Test
    public void testAliasFieldName() {

        assertEquals(ESCAPED_CUSTOM_FIELD_NAME, aliasFieldName(null, TEST_CUSTOM_FIELD_NAME));
        assertEquals(TEST_TABLE_ALIAS + NAME_SEPARATOR + ESCAPED_CUSTOM_FIELD_NAME,
                aliasFieldName(TEST_TABLE_ALIAS, TEST_CUSTOM_FIELD_NAME));
    }
}

package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import java.util.UUID;

import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.StorageConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.addDoubleQuotes;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.isNullOrEmpty;

public class StorageUtils {

    private StorageUtils() {
        throw new UnsupportedOperationException();
    }

    public static String toSchemaName(String storageCode) {

        if (isNullOrEmpty(storageCode))
            return DATA_SCHEMA_NAME;

        int separatorIndex = storageCode.indexOf(CODE_SEPARATOR);
        if (separatorIndex > 0) {
            return storageCode.substring(0, separatorIndex);
        }

        return DATA_SCHEMA_NAME;
    }

    public static String toTableName(String storageCode) {

        if (isNullOrEmpty(storageCode))
            return null;

        int separatorIndex = storageCode.indexOf(CODE_SEPARATOR);
        if (separatorIndex >= 0) {
            return storageCode.substring(separatorIndex + 1);
        }

        return storageCode;
    }

    public static String toStorageCode(String schemaName, String tableName) {

        return isNullOrEmpty(schemaName) || DATA_SCHEMA_NAME.equals(schemaName)
                ? tableName
                : schemaName + CODE_SEPARATOR + tableName;
    }

    /**
     * Проверка схемы на соответствие схеме по умолчанию.
     *
     * @param schemaName наименование схемы
     * @return Результат проверки
     */
    public static boolean isDefaultSchema(String schemaName) {

        return isNullOrEmpty(schemaName) || DATA_SCHEMA_NAME.equals(schemaName);
    }

    /**
     * Проверка наименования схемы на корректность.
     *
     * @param schemaName наименование схемы
     * @return Результат проверки
     */
    public static boolean isValidSchemaName(String schemaName) {

        return SCHEMA_NAME_PATTERN.matcher(schemaName).matches();
    }

    public static String getSchemaNameOrDefault(String schemaName) {

        return isNullOrEmpty(schemaName) ? DATA_SCHEMA_NAME : schemaName;
    }

    public static String escapeTableName(String schemaName, String tableName) {

        return getSchemaNameOrDefault(schemaName) + NAME_SEPARATOR + addDoubleQuotes(tableName);
    }

    public static String escapeStorageTableName(String storageCode) {

        return escapeTableName(toSchemaName(storageCode), toTableName(storageCode));
    }

    public static String escapeFieldName(String tableAlias, String fieldName) {

        String escapedFieldName = addDoubleQuotes(fieldName);
        return isNullOrEmpty(tableAlias) ? escapedFieldName : tableAlias + NAME_SEPARATOR + escapedFieldName;
    }

    public static String escapeSequenceName(String tableName) {
        return addDoubleQuotes(tableName + NAME_CONNECTOR + SYS_PRIMARY_COLUMN + TABLE_SEQUENCE_SUFFIX);
    }

    public static String escapeSchemaSequenceName(String schemaName, String tableName) {

        return getSchemaNameOrDefault(schemaName) + NAME_SEPARATOR + escapeSequenceName(tableName);
    }

    public static String escapeStorageSequenceName(String storageCode) {

        return escapeSchemaSequenceName(toSchemaName(storageCode), toTableName(storageCode));
    }

    public static String escapeTableIndexName(String tableName, String indexName) {

        return addDoubleQuotes(tableName + NAME_CONNECTOR + indexName + TABLE_INDEX_SUFFIX);
    }

    /**
     * Генерация наименования хранилища.
     *
     * @return Наименование хранилища
     */
    public static String generateStorageName() {

        return UUID.randomUUID().toString();
    }
}

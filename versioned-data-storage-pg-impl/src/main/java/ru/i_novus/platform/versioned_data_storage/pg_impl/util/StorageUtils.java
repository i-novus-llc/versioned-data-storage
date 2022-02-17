package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import java.util.UUID;

import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.StorageConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.addDoubleQuotes;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.isNullOrEmpty;

public class StorageUtils {

    private StorageUtils() {
        // Nothing to do.
    }

    /**
     * Преобразование текста в наименование, допустимое в SQL.
     *
     * @param text текст
     * @return Наименование, допустимое в SQL
     */
    public static String sqlName(String text) {

        if (isNullOrEmpty(text))
            return "";

        String name = SQL_NAME_WRONG_CHAR_PATTERN.matcher(text).replaceAll(SQL_NAME_WRONG_CHAR_REPLACE);
        if (isNullOrEmpty(name))
            return "";

        return limitSqlName(name);
    }

    /**
     * Ограничение наименования в SQL.
     *
     * @param name наименование
     * @return Ограниченное наименование
     */
    public static String limitSqlName(String name) {

        return (name.length() <= SQL_NAME_MAX_LENGTH) ? name : name.substring(0, SQL_NAME_MAX_LENGTH);
    }

    /**
     * Преобразование текста в наименование схемы.
     *
     * @param text текст
     * @return Наименование схемы
     */
    public static String sqlSchemaName(String text) {

        if (isNullOrEmpty(text))
            return "";

        String name = SCHEMA_NAME_WRONG_CHAR_PATTERN.matcher(text).replaceAll(SCHEMA_NAME_WRONG_CHAR_REPLACE);
        return limitSqlName(name.toLowerCase());
    }

    /** Экранирование системных наименований для SQL. */
    public static String escapeSystemName(String name) {

        return addDoubleQuotes(name); // Просто обрамление кавычками!
    }

    /** Экранирование пользовательских наименований для SQL. */
    public static String escapeCustomName(String name) {

        if (isNullOrEmpty(name))
            return "";

        String result = sqlName(name);
        return addDoubleQuotes(result);

        //return addDoubleQuotes(name); // todo: Заменить!
    }

    /**
     * Преобразование кода хранилища в наименование схемы.
     *
     * @param storageCode код хранилища
     * @return Наименование схемы
     */
    public static String toSchemaName(String storageCode) {

        if (isNullOrEmpty(storageCode))
            return DATA_SCHEMA_NAME;

        int separatorIndex = storageCode.indexOf(CODE_SEPARATOR);
        if (!(separatorIndex > 0))
            return DATA_SCHEMA_NAME;

        String name = sqlSchemaName(storageCode.substring(0, separatorIndex));
        return isNullOrEmpty(name) ? DATA_SCHEMA_NAME : name;
    }

    /**
     * Преобразование кода хранилища в наименование таблицы.
     *
     * @param storageCode код хранилища
     * @return Наименование таблицы
     */
    public static String toTableName(String storageCode) {

        if (isNullOrEmpty(storageCode))
            return "";

        int separatorIndex = storageCode.indexOf(CODE_SEPARATOR);
        String name = (separatorIndex >= 0) ? storageCode.substring(separatorIndex + 1) : storageCode;
        if (isNullOrEmpty(name))
            return "";

        return isNullOrEmpty(name) ? "" : sqlName(name);
    }

    /**
     * Преобразование наименования схемы и таблицы в код хранилища.
     *
     * @param schemaName наименование схемы
     * @param tableName  наименование таблицы
     * @return Код хранилища
     */
    public static String toStorageCode(String schemaName, String tableName) {

        return isDefaultSchema(schemaName) ? tableName : schemaName + CODE_SEPARATOR + tableName;
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

    /** Экранирование наименования схемы. */
    public static String escapeSchemaName(String schemaName) {

        String name = sqlSchemaName(schemaName);
        return isNullOrEmpty(name) ? DATA_SCHEMA_NAME : name;
    }

    /** Экранирование наименования таблицы на схеме. */
    public static String escapeTableName(String schemaName, String tableName) {

        return escapeSchemaName(schemaName) + NAME_SEPARATOR + escapeCustomName(tableName);
    }

    /** Экранирование наименования таблицы хранилища. */
    public static String escapeStorageTableName(String storageCode) {

        return escapeTableName(toSchemaName(storageCode), toTableName(storageCode));
    }

    /** Формирование полного наименования поля с псевдонимом таблицы. */
    public static String aliasColumnName(String tableAlias, String fieldName) {

        return tableAlias + NAME_SEPARATOR + fieldName;
    }

    /** Экранирование наименования системного поля. */
    public static String escapeSystemFieldName(String tableAlias, String fieldName) {

        String escapedFieldName = escapeSystemName(fieldName);
        return isNullOrEmpty(tableAlias) ? escapedFieldName : aliasColumnName(tableAlias, escapedFieldName);
    }

    /** Экранирование наименования пользовательского поля. */
    public static String escapeFieldName(String tableAlias, String fieldName) {

        String escapedFieldName = escapeCustomName(fieldName);
        return isNullOrEmpty(tableAlias) ? escapedFieldName : aliasColumnName(tableAlias, escapedFieldName);
    }

    /** Формирование наименования последовательности для таблицы. */
    public static String tableSequenceName(String tableName) {

        return tableName + NAME_CONNECTOR + SYS_PRIMARY_COLUMN + TABLE_SEQUENCE_SUFFIX;
    }

    /** Экранирование наименования последовательности для таблицы. */
    public static String escapeSequenceName(String tableName) {

        return escapeCustomName(tableSequenceName(tableName));
    }

    /** Экранирование наименования последовательности для таблицы на схеме. */
    public static String escapeSchemaSequenceName(String schemaName, String tableName) {

        return escapeSchemaName(schemaName) + NAME_SEPARATOR + escapeSequenceName(tableName);
    }

    /** Экранирование наименования последовательности для хранилища. */
    public static String escapeStorageSequenceName(String storageCode) {

        return escapeSchemaSequenceName(toSchemaName(storageCode), toTableName(storageCode));
    }

    /** Экранирование наименования индекса для таблицы. */
    public static String escapeTableIndexName(String tableName, String indexName) {

        return escapeCustomName(tableName + NAME_CONNECTOR + indexName);
    }

    /** Экранирование наименования функции для хранилища. */
    public static String escapeStorageFunctionName(String storageCode, String functionName) {

        return toSchemaName(storageCode) + NAME_SEPARATOR +
                escapeCustomName(toTableName(storageCode) + NAME_CONNECTOR + functionName);
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

package ru.i_novus.platform.datastorage.temporal.util;

import static ru.i_novus.platform.datastorage.temporal.model.StorageConstants.*;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.isNullOrEmpty;

public class StorageUtils {

    private StorageUtils() {
        throw new UnsupportedOperationException();
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
        if (separatorIndex > 0) {
            return storageCode.substring(0, separatorIndex);
        }

        return DATA_SCHEMA_NAME;
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
        if (separatorIndex >= 0) {
            return storageCode.substring(separatorIndex + 1);
        }

        return storageCode;
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
     * Проверка наименования схемы нра корректность.
     *
     * @param schemaName наименование схемы
     * @return Результат проверки
     */
    public static boolean isValidSchemaName(String schemaName) {

        return SCHEMA_NAME_PATTERN.matcher(schemaName).matches();
    }
}

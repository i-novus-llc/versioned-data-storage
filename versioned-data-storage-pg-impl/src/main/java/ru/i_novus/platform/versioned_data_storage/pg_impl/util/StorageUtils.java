package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import ru.i_novus.platform.versioned_data_storage.pg_impl.model.StorageConstants;

import static ru.i_novus.platform.versioned_data_storage.pg_impl.model.StorageConstants.DATA_SCHEMA_NAME;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.model.StorageConstants.SCHEMA_NAME_PATTERN;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.isNullOrEmpty;

public class StorageUtils {

    private StorageUtils() {
        throw new UnsupportedOperationException();
    }

    public static String toSchemaName(String storageCode) {

        if (isNullOrEmpty(storageCode))
            return DATA_SCHEMA_NAME;

        int separatorIndex = storageCode.indexOf(StorageConstants.CODE_SEPARATOR);
        if (separatorIndex > 0) {
            return storageCode.substring(0, separatorIndex);
        }

        return DATA_SCHEMA_NAME;
    }

    public static String toTableName(String storageCode) {

        if (isNullOrEmpty(storageCode))
            return null;

        int separatorIndex = storageCode.indexOf(StorageConstants.CODE_SEPARATOR);
        if (separatorIndex >= 0) {
            return storageCode.substring(separatorIndex + 1);
        }

        return storageCode;
    }

    public static String toStorageCode(String schemaName, String tableName) {

        return isNullOrEmpty(schemaName) || DATA_SCHEMA_NAME.equals(schemaName)
                ? tableName
                : schemaName + StorageConstants.CODE_SEPARATOR + tableName;
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

    public static String getSchemaNameOrDefault(String schemaName) {

        return isNullOrEmpty(schemaName) ? DATA_SCHEMA_NAME : schemaName;
    }
}

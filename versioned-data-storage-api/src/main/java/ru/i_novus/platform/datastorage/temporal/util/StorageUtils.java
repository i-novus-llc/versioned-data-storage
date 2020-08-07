package ru.i_novus.platform.datastorage.temporal.util;

import static ru.i_novus.platform.datastorage.temporal.model.StorageConstants.*;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.isNullOrEmpty;

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
            return "";

        int separatorIndex = storageCode.indexOf(CODE_SEPARATOR);
        if (separatorIndex >= 0) {
            return storageCode.substring(separatorIndex + 1);
        }

        return storageCode;
    }

    public static String toStorageCode(String schemaName, String tableName) {

        return isDefaultSchema(schemaName) ? tableName : schemaName + CODE_SEPARATOR + tableName;
    }

    public static boolean isDefaultSchema(String schemaName) {

        return isNullOrEmpty(schemaName) || DATA_SCHEMA_NAME.equals(schemaName);
    }

    public static boolean isValidSchemaName(String schemaName) {

        return SCHEMA_NAME_PATTERN.matcher(schemaName).matches();
    }
}

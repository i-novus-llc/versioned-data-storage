package ru.i_novus.platform.datastorage.temporal.util;

import ru.i_novus.platform.datastorage.temporal.model.StorageConstants;

import static ru.i_novus.platform.datastorage.temporal.model.StorageConstants.DATA_SCHEMA_NAME;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.isNullOrEmpty;

public class StorageUtils {

    public StorageUtils() {
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
}

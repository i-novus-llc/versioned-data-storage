package ru.i_novus.platform.datastorage.temporal.model;

import java.util.regex.Pattern;

public class StorageConstants {

    // В postgres максимальная длина имени = NAMEDATALEN - 1 = 64 - 1 .
    private static final String SCHEMA_NAME_REGEX = "[a-z][a-z\\d_]{0,62}";
    public static final Pattern SCHEMA_NAME_PATTERN = Pattern.compile(SCHEMA_NAME_REGEX);

    public static final String CODE_SEPARATOR = ".";

    public static final String DATA_SCHEMA_NAME = "data";

    // Системные поля справочников.
    public static final String SYS_PRIMARY_COLUMN = "SYS_RECORDID";
    public static final String SYS_PUBLISHTIME = "SYS_PUBLISHTIME";
    public static final String SYS_CLOSETIME = "SYS_CLOSETIME";
    public static final String SYS_HASH = "SYS_HASH";
    public static final String SYS_PATH = "SYS_PATH";
    public static final String SYS_FTS = "FTS";

    public static final String REFERENCE_VALUE_NAME = "value";
    public static final String REFERENCE_DISPLAY_VALUE_NAME = "displayValue";
    public static final String REFERENCE_HASH_NAME = "hash";

    private StorageConstants() {
    }
}

package ru.i_novus.platform.datastorage.temporal.model;

public class DataConstants {

    public static final String DATA_SCHEMA_NAME = "data";

    // Системные поля справочников.
    public static final String SYS_PRIMARY_COLUMN = "SYS_RECORDID";
    public static final String SYS_PUBLISHTIME = "SYS_PUBLISHTIME";
    public static final String SYS_CLOSETIME = "SYS_CLOSETIME";
    public static final String SYS_HASH = "SYS_HASH";
    public static final String SYS_PATH = "SYS_PATH";
    public static final String SYS_FULL_TEXT_SEARCH = "FTS";

    public static final String REFERENCE_VALUE_NAME = "value";
    public static final String REFERENCE_DISPLAY_VALUE_NAME = "displayValue";

    private DataConstants() {
    }
}

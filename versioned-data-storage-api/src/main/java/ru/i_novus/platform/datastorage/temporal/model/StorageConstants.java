package ru.i_novus.platform.datastorage.temporal.model;

import ru.i_novus.platform.datastorage.temporal.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StorageConstants {

    // Наименование схемы для хранилищ может быть только в нижнем регистре.
    // В postgres максимальная длина имени = NAMEDATALEN - 1 = 64 - 1 .
    private static final String SCHEMA_NAME_REGEX = "[a-z][a-z\\d_]{0,62}";
    public static final Pattern SCHEMA_NAME_PATTERN = Pattern.compile(SCHEMA_NAME_REGEX);

    public static final String CODE_SEPARATOR = ".";

    public static final String NAME_SEPARATOR = ".";
    public static final String NAME_CONNECTOR = "_";

    public static final String DATA_SCHEMA_NAME = "data";

    public static final String TABLE_SEQUENCE_SUFFIX = "_seq";
    public static final String TABLE_INDEX_SUFFIX = "_idx";

    // Системные поля справочников.
    public static final String SYS_PRIMARY_COLUMN = "SYS_RECORDID"; // Первичный ключ записи
    public static final String SYS_PUBLISHTIME = "SYS_PUBLISHTIME"; // Дата публикации записи
    public static final String SYS_CLOSETIME = "SYS_CLOSETIME"; // Дата прекращения действия записи
    public static final String SYS_HASH = "SYS_HASH"; // Хеш
    public static final String SYS_PATH = "SYS_PATH";
    public static final String SYS_FTS = "FTS"; // Значение для полнотекстового поиска

    private static final List<String> SYS_RECORD_LIST = Arrays.asList(SYS_PRIMARY_COLUMN,
            SYS_PUBLISHTIME, SYS_CLOSETIME,
            SYS_HASH, SYS_PATH, SYS_FTS
    );
    public static final String SYS_RECORDS_TEXT = SYS_RECORD_LIST.stream()
            .map(StringUtils::addSingleQuotes)
            .collect(Collectors.joining(", "));

    public static final String REFERENCE_VALUE_NAME = "value";
    public static final String REFERENCE_DISPLAY_VALUE_NAME = "displayValue";
    public static final String REFERENCE_HASH_NAME = "hash";

    private StorageConstants() {
    }

    // NB: Workaround to sonar issue "squid-S2386".
    public static List<String> systemFieldList() {
        return SYS_RECORD_LIST;
    }
}

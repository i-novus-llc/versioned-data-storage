package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;

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
    public static final String SYS_PATH = "SYS_PATH"; // не используется
    public static final String SYS_FTS = "FTS"; // Значение для полнотекстового поиска

    private static final List<String> SYS_FIELD_NAMES = Arrays.asList(SYS_PRIMARY_COLUMN,
            SYS_HASH, SYS_PATH, SYS_FTS,
            SYS_PUBLISHTIME, SYS_CLOSETIME
    );

    private static final List<String> VERSIONED_SYS_FIELD_NAMES = Arrays.asList(SYS_PUBLISHTIME, SYS_CLOSETIME);

    private static final List<String> ESCAPED_VERSIONED_SYS_FIELD_NAMES = VERSIONED_SYS_FIELD_NAMES.stream()
            .map(StringUtils::addDoubleQuotes).collect(toList());

    private static final List<String> TRIGGERED_SYS_FIELD_NAMES = Arrays.asList(SYS_HASH, SYS_FTS);

    private static final List<String> ESCAPED_TRIGGERED_SYS_FIELD_NAMES = TRIGGERED_SYS_FIELD_NAMES.stream()
                .map(StringUtils::addDoubleQuotes).collect(toList());

    public static final String REFERENCE_VALUE_NAME = "value";
    public static final String REFERENCE_DISPLAY_VALUE_NAME = "displayValue";
    public static final String REFERENCE_HASH_NAME = "hash";

    private StorageConstants() {
        // Nothing to do.
    }

    // NB: Workaround to sonar issue "squid-S2386".
    public static List<String> systemFieldNames() {
        return SYS_FIELD_NAMES;
    }

    public static List<String> versionFieldNames() {
        return VERSIONED_SYS_FIELD_NAMES;
    }

    public static List<String> escapedVersionFieldNames() {
        return ESCAPED_VERSIONED_SYS_FIELD_NAMES;
    }

    public static List<String> escapedTriggeredFieldNames() {
        return ESCAPED_TRIGGERED_SYS_FIELD_NAMES;
    }
}

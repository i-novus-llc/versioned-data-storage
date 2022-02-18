package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import ru.i_novus.platform.versioned_data_storage.pg_impl.util.StorageUtils;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;

public class StorageConstants {

    // Наименование идентификатора.
    // Допустимыми являются только однобайтные символы:
    // латинские буквы в верхнем и нижнем регистре, цифры, подчёркивание и дефис.
    //
    // SQL identifiers and key words must begin with a letter
    // (a-z, but also letters with diacritical marks and non-Latin letters) or an underscore (_).
    // Subsequent characters in an identifier or key word can be
    // letters, underscores, digits (0-9), or dollar signs ($).
    // Note that dollar signs are not allowed in identifiers according
    // to the letter of the SQL standard, so their use might render applications less portable.
    // The SQL standard will not define a key word that contains digits or starts or ends with an underscore,
    // so identifiers of this form are safe against possible conflict with future extensions of the standard.
    private static final String SQL_NAME_WRONG_CHAR_REGEX = "[^A-Za-z0-9_-]+";
    public static final Pattern SQL_NAME_WRONG_CHAR_PATTERN = Pattern.compile(SQL_NAME_WRONG_CHAR_REGEX);
    public static final String SQL_NAME_WRONG_CHAR_REPLACE = "";

    // В postgres максимальная длина имени = NAMEDATALEN - 1 = 64 - 1 байт.
    public static final int SQL_NAME_MAX_LENGTH = 64 - 1; // without terminated null !

    // Наименование схемы для хранилищ может быть только в нижнем регистре.
    private static final String SCHEMA_NAME_REGEX = "[a-z][a-z\\d_]{0,62}";
    public static final Pattern SCHEMA_NAME_PATTERN = Pattern.compile(SCHEMA_NAME_REGEX);

    private static final String SCHEMA_NAME_WRONG_CHAR_REGEX = "[^A-Za-z0-9_]";
    public static final Pattern SCHEMA_NAME_WRONG_CHAR_PATTERN = Pattern.compile(SCHEMA_NAME_WRONG_CHAR_REGEX);
    public static final String SCHEMA_NAME_WRONG_CHAR_REPLACE = "0";

    public static final String CODE_SEPARATOR = ".";

    public static final String NAME_SEPARATOR = ".";
    public static final String NAME_CONNECTOR = "_";

    public static final String DATA_SCHEMA_NAME = "data";

    public static final String TABLE_SEQUENCE_SUFFIX = "_seq";
    public static final String TABLE_INDEX_SUFFIX = "_idx";

    // Системные поля хранилищ.
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
            .map(StorageUtils::escapeSystemFieldName)
            .collect(toList());

    private static final List<String> TRIGGERED_SYS_FIELD_NAMES = Arrays.asList(SYS_HASH, SYS_FTS);

    private static final List<String> ESCAPED_TRIGGERED_SYS_FIELD_NAMES = TRIGGERED_SYS_FIELD_NAMES.stream()
            .map(StorageUtils::escapeSystemFieldName)
            .collect(toList());

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

    public static List<String> versionedFieldNames() {
        return VERSIONED_SYS_FIELD_NAMES;
    }

    public static List<String> escapedVersionedFieldNames() {
        return ESCAPED_VERSIONED_SYS_FIELD_NAMES;
    }

    public static List<String> escapedTriggeredFieldNames() {
        return ESCAPED_TRIGGERED_SYS_FIELD_NAMES;
    }
}

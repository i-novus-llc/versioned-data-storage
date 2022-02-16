package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import java.time.format.DateTimeFormatter;

import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.StorageConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StorageUtils.escapeSystemFieldName;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.addSingleQuotes;

/**
 * @author lgalimova
 * @since 22.03.2018
 */
@SuppressWarnings("java:S1192")
public class QueryConstants {

    public static final int TRANSACTION_ROW_LIMIT = 1000;

    public static final String DEFAULT_TABLE_ALIAS = "d";

    public static final String BASE_TABLE_ALIAS = "b";

    static final String TRIGGER_NEW_ALIAS = "NEW";

    // Специальные выражения в запросах.
    static final String QUERY_NEW_LINE = " \n";
    static final String QUERY_BIND_CHAR = ":";
    public static final String QUERY_NULL_VALUE = "null";

    public static final String SELECT_COUNT_ONLY = "SELECT count(*) \n";

    public static final String LIKE_ESCAPE_MANY_CHAR = "%";
    public static final String TO_ANY_BIGINT = "ANY(%s\\:\\:bigint[])";
    public static final String TO_ANY_TEXT = "ANY(%s\\:\\:text[])";

    // Подстановки в запросах.
    static final String QUERY_VALUE_SUBST = "?";
    static final String QUERY_LTREE_SUBST = QUERY_VALUE_SUBST + "\\:\\:ltree";

    // Формат даты для использования в запросах.
    static final String DATE_FORMAT_FOR_INSERT_ROW = "yyyy-MM-dd";
    public static final String QUERY_DATE_FORMAT = "DD.MM.YYYY";

    // Формат даты-времени для использования в запросах.
    static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DATETIME_FORMAT);
    static final String QUERY_TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS";

    public static final String MIN_TIMESTAMP_VALUE = "'-infinity'";
    public static final String MAX_TIMESTAMP_VALUE = "'infinity'";
    public static final String TO_TIMESTAMP = "to_timestamp(%s, '" + QUERY_TIMESTAMP_FORMAT + "')";
    public static final String TIMESTAMP_WITHOUT_TIME_ZONE = "\\:\\:timestamp without time zone";

    public static final String REFERENCE_FIELD_SQL_TYPE = "jsonb";
    public static final String REFERENCE_FIELD_VALUE_OPERATOR = "->>";

    static final String BIND_INFO_SCHEMA_NAME = "schemaName";
    static final String BIND_INFO_TABLE_NAME = "tableName";
    static final String BIND_INFO_COLUMN_NAME = "columnName";

    public static final String SELECT_SCHEMA_EXISTS = "SELECT EXISTS(\n" +
            "SELECT * \n" +
            "  FROM information_schema.schemata \n" +
            " WHERE true \n" +
            "  AND schema_name = :schemaName \n" +
            ")";

    public static final String SELECT_EXISTENT_SCHEMA_NAME_LIST_BY = "SELECT schema_name \n" +
            "  FROM information_schema.schemata \n" +
            " WHERE true \n" +
            "  AND schema_name = ";

    public static final String SELECT_TABLE_EXISTS = "SELECT EXISTS(\n" +
            "SELECT * \n" +
            "  FROM information_schema.tables \n" +
            " WHERE true \n" +
            "  AND table_schema = :schemaName \n" +
            "  AND table_name = :tableName \n" +
            ")";

    public static final String SELECT_EXISTENT_TABLE_SCHEMA_NAME_LIST_BY = "SELECT table_schema \n" +
            "  FROM information_schema.tables \n" +
            " WHERE true \n" +
            "  AND table_name = :tableName \n" +
            "  AND table_schema = ";

    public static final String SELECT_COLUMN_EXISTS = "SELECT EXISTS(\n" +
            "SELECT * \n" +
            "  FROM information_schema.columns \n" +
            " WHERE true \n" +
            "  AND table_schema = :schemaName \n" +
            "  AND table_name = :tableName \n" +
            "  AND column_name = :columnName \n" +
            ")";

    // Часть для получения значения поля специального типа.
    private static final String INFO_COLUMN_VALUE_SUFFIX = "(case " +
            " when data_type = " + addSingleQuotes(REFERENCE_FIELD_SQL_TYPE) +
            " then " + addSingleQuotes(REFERENCE_FIELD_VALUE_OPERATOR +
            // Закавычивание дважды из-за двойного вызова для двух запросов!
            addSingleQuotes(addSingleQuotes(REFERENCE_VALUE_NAME))) +
            " else '' end)";

    static final String SELECT_ESCAPED_FIELD_NAMES = "SELECT '\"' || column_name || '\"' \n" +
            "  FROM information_schema.columns \n" +
            " WHERE true \n" +
            "  AND table_schema = :schemaName \n" +
            "  AND table_name = :tableName \n";

    static final String SELECT_HASH_USED_FIELD_NAMES = "SELECT '\"' || column_name || '\"' \n" +
            " || " + INFO_COLUMN_VALUE_SUFFIX +
            "  FROM information_schema.columns \n" +
            " WHERE true \n" +
            "  AND table_schema = :schemaName \n" +
            "  AND table_name = :tableName \n";

    static final String AND_INFO_COLUMN_NOT_IN_SYS_LIST = "  AND column_name NOT IN (%s)";

    public static final String SELECT_FIELD_TYPE = "SELECT data_type \n" +
            "  FROM information_schema.columns \n" +
            " WHERE true \n" +
            "  AND table_schema = :schemaName \n" +
            "  AND table_name = :tableName \n" +
            "  AND column_name = :columnName \n";

    public static final String SELECT_FIELD_NAMES_AND_TYPES = "SELECT column_name, data_type \n" +
            "  FROM information_schema.columns \n" +
            " WHERE true \n" +
            "  AND table_schema = :schemaName \n" +
            "  AND table_name = :tableName \n";

    public static final String SELECT_DDL_INDEXES = "SELECT indexdef \n" +
            "  FROM pg_indexes \n" +
            " WHERE schemaname = :schemaName \n" +
            "   AND tablename = :tableName \n";
    public static final String AND_DDL_INDEX_NOT_LIKE =
            "   AND NOT indexdef LIKE :notLikeIndexes \n";

    static final String DATE_BEGIN = "DATEBEG";
    static final String DATE_END = "DATEEND";
    static final String HAS_CHILD_BRANCH = "SYS_HAS_CHILD_BRANCH";

    private static final String COALESCE_PUBLISH_TIME_FIELD = "coalesce(%s.\"%s\", '-infinity')";
    private static final String COALESCE_CLOSE_TIME_FIELD = "coalesce(%s.\"%s\", 'infinity')";
    private static final String COALESCE_PUBLISH_TIME_VALUE = "coalesce(to_timestamp('${publishTime}', " +
            "'" + QUERY_TIMESTAMP_FORMAT + "')\\:\\:timestamp without time zone, '-infinity')";
    private static final String COALESCE_CLOSE_TIME_VALUE = "coalesce(to_timestamp('${closeTime}', " +
            "'" + QUERY_TIMESTAMP_FORMAT + "')\\:\\:timestamp without time zone, 'infinity')";

    public static final String DRAFT_TABLE_ALIAS = "d";
    public static final String VERSION_TABLE_ALIAS = "v";

    private static final String COALESCE_VERSION_PUBLISH_TIME_FIELD = String.format(COALESCE_PUBLISH_TIME_FIELD, "${versionAlias}", SYS_PUBLISHTIME);
    private static final String COALESCE_VERSION_CLOSE_TIME_FIELD = String.format(COALESCE_CLOSE_TIME_FIELD, "${versionAlias}", SYS_CLOSETIME);

    //todo: get rid of infinity
    private static final String AND_IS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = "   AND (\n" +
            "        (" + COALESCE_PUBLISH_TIME_VALUE + ", " + COALESCE_CLOSE_TIME_VALUE + ")\n" +
            "        OVERLAPS \n" +
            "        (" + COALESCE_VERSION_PUBLISH_TIME_FIELD + ", " + COALESCE_VERSION_CLOSE_TIME_FIELD + ")\n" +
            "        OR (" + COALESCE_PUBLISH_TIME_VALUE + " = " + COALESCE_VERSION_CLOSE_TIME_FIELD + ")\n" +
            "   )\n";

    static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s;";

    static final String CREATE_DRAFT_TABLE = "CREATE TABLE %1$s (" +
            "  \"SYS_RECORDID\" bigserial NOT NULL," +
            "  %2$s" +
            "  \"FTS\" tsvector," +
            "  \"SYS_HASH\" char(32) UNIQUE," +
            "  CONSTRAINT \"%3$s_pkey\" PRIMARY KEY (\"SYS_RECORDID\")" +
            ");";
    static final String DROP_TABLE = "DROP TABLE IF EXISTS %1$s";

    static final String CREATE_TABLE_COPY = "CREATE TABLE %1$s AS " +
            "SELECT * FROM %2$s WITH NO DATA;";

    static final String CREATE_TABLE_SEQUENCE = "CREATE SEQUENCE %s start 1";
    static final String SELECT_PRIMARY_MAX = "SELECT max(%2$s) FROM %1$s";
    // ALTER SEQUENCE не позволяет использовать SELECT.
    static final String UPDATE_TABLE_SEQUENCE = "DO $$\n" +
                "BEGIN \n" +
                "    if EXISTS(\n" +
                "       SELECT * FROM pg_class \n" +
                "        WHERE relkind = 'S' AND oid\\:\\:regclass\\:\\:text = '%1$s' \n" +
                "       ) then\n" +
                "       PERFORM setval('%1$s', (%2$s)); \n" +
                "    end if;\n" +
                "END$$;";
    static final String DROP_TABLE_SEQUENCE = "DROP SEQUENCE IF EXISTS %s CASCADE";

    static final String CREATE_TRIGGER = "CREATE OR REPLACE FUNCTION %2$s() \n" +
            "  RETURNS trigger AS \n" +
            "$BODY$ \n" +
            "  BEGIN \n" +
            "    %5$s \n" +
            "    RETURN NEW; \n" +
            "  END; \n" +
            "$BODY$ LANGUAGE plpgsql; \n" +
            "\n" +
            "CREATE TRIGGER %3$s \n" +
            "  BEFORE INSERT OR UPDATE OF %4$s \n" +
            "  ON %1$s \n" +
            "  FOR EACH ROW \n" +
            "  EXECUTE PROCEDURE %2$s();";

    static final String DROP_TRIGGER = "DROP TRIGGER IF EXISTS %1$s ON %2$s;";

    static final String SWITCH_TRIGGERS = "ALTER TABLE %1$s %2$s TRIGGER ALL;";

    static final String DROP_FUNCTION = "DROP FUNCTION IF EXISTS %1$s;";

    static final String HASH_FUNCTION_NAME = "hash_tf";
    static final String HASH_TRIGGER_NAME = "hash_tg";
    public static final String HASH_EXPRESSION = "md5(ROW(%s)||'')";

    static final String FTS_FUNCTION_NAME = "fts_vector_tf";
    static final String FTS_TRIGGER_NAME = "fts_vector_tg";

    static final String ALTER_ADD_PRIMARY_KEY = "ALTER TABLE %1$s ADD PRIMARY KEY (%2$s);";
    static final String ALTER_SET_SEQUENCE_FOR_PRIMARY_KEY = "ALTER TABLE %1$s \n" +
            "  ALTER COLUMN %2$s SET DEFAULT nextval('%3$s');";

    static final String UPDATE_FIELD = "UPDATE %1$s SET %2$s;";

    static final String ALTER_ADD_COLUMN = "ALTER TABLE %1$s ADD COLUMN %2$s %3$s %4$s;";
    static final String ALTER_DELETE_COLUMN = "ALTER TABLE %1$s DROP COLUMN %2$s CASCADE;";
    static final String ALTER_COLUMN_WITH_USING = "ALTER TABLE %1$s ALTER COLUMN %2$s SET DATA TYPE %3$s USING %4$s";

    public static final String INSERT_RECORD = "INSERT INTO %1$s (%2$s) \n";
    public static final String INSERT_VALUES = "VALUES %s \n";
    static final String INSERT_SELECT = "SELECT %4$s \n" + "  FROM %1$s.%2$s as %3$s \n WHERE true \n";
    static final String DELETE_RECORD = "DELETE FROM %1$s \n";
    static final String UPDATE_RECORD = "UPDATE %1$s as %2$s SET %3$s WHERE %4$s \n";

    private static final String AND_EXISTS_VERSION_REF_VALUE = "   AND ${versionAlias}.${refFieldName} is not null \n" +
            "   AND (${versionAlias}.${refFieldName} -> 'value') is not null \n";

    static final String COUNT_REFERENCE_IN_REF_ROWS = SELECT_COUNT_ONLY +
            "  FROM ${versionTable} AS ${versionAlias} \n" +
            " WHERE true \n" +
            AND_EXISTS_VERSION_REF_VALUE;

    static final String SELECT_REFERENCE_IN_REF_ROWS = "\nSELECT ${versionAlias}.\"SYS_RECORDID\" \n" +
            "  FROM ${versionTable} AS ${versionAlias} \n" +
            " WHERE true \n" +
            AND_EXISTS_VERSION_REF_VALUE +
            " ORDER BY ${versionAlias}.\"SYS_RECORDID\" \n" +
            " LIMIT ${limit} \n" +
            "OFFSET ${offset} \n";

    static final String REFERENCE_VALUATION_UPDATE_TABLE_ALIAS = "b";
    static final String REFERENCE_VALUATION_SELECT_TABLE_ALIAS = "d";
    static final String REFERENCE_VALUATION_SELECT_SUBST = "select jsonb_build_object('value', ?)";
    static final String REFERENCE_VALUATION_SELECT_EXPRESSION = "select jsonb_build_object(" +
            addSingleQuotes(REFERENCE_VALUE_NAME) + ", %3$s, " +
            addSingleQuotes(REFERENCE_DISPLAY_VALUE_NAME) + ", %4$s, " +
            addSingleQuotes(REFERENCE_HASH_NAME) + ", " + escapeSystemFieldName("%2$s", SYS_HASH) +
            ") " +
            "  from %1$s as %2$s " +
            " where %3$s = %5$s\\:\\:%6$s " +
            " %7$s";
    static final String REFERENCE_VALUATION_OLD_VALUE =
            "(case when %1$s is null then null else %1$s->>'value' end)";

    public static final String IS_VERSION_NOT_EMPTY = "SELECT EXISTS(SELECT * FROM data.%s);";
    public static final String IS_FIELD_NOT_NULL = "SELECT EXISTS(SELECT * FROM %1$s AS %2$s WHERE %3$s IS NOT NULL);";
    public static final String IS_FIELD_CONTAIN_NULL_VALUES = "SELECT EXISTS(SELECT * FROM %1$s AS %2$s WHERE %3$s IS NULL);";
    public static final String IS_RELATED_VALUE_EXIST = "SELECT EXISTS(SELECT * FROM data.%s where %s.%s = %s)";

    static final String CREATE_TABLE_INDEX = "CREATE INDEX IF NOT EXISTS %1$s ON %2$s %3$s(%4$s);";
    static final String DROP_TABLE_INDEX = "DROP INDEX IF EXISTS %1$s;";
    static final String IF_TABLE_INDEX_EXISTS = "SELECT EXISTS(SELECT * \n" +
            "              FROM \n" +
            "                pg_class t,\n" +
            "                pg_class i,\n" +
            "                pg_index ix,\n" +
            "                pg_attribute a\n" +
            "              WHERE t.oid = ix.indrelid\n" +
            "                AND i.oid = ix.indexrelid\n" +
            "                AND a.attrelid = t.oid\n" +
            "                AND a.attnum = ANY(ix.indkey)\n" +
            "                AND t.relkind = 'r'\n" +
            "                AND t.relname = '%s'\n" +
            "                AND a.attname = '%s'\n" +
            "                AND i.relname = '%s'\n" +
            "              ORDER BY \n" +
            "                t.relname,\n" +
            "                i.relname\n" +
            ");";

    public static final String TABLE_INDEX_SYSDATE_NAME = "SYSDATE";
    public static final String TABLE_INDEX_SYSHASH_NAME = "sys_hash_ix";

    static final String TABLE_INDEX_FTS_NAME = "fts";
    static final String TABLE_INDEX_FTS_USING = "USING gin";
    static final String TABLE_INDEX_LTREE_USING = "USING gist";

    public static final String ROW_TYPE_VAR_NAME = "row";

    // to-do: Переделать остальные DO-DECLARE-BEGIN_END под такую конструкцию.
    public static final String INSERT_DATA_BY_SELECT_FROM_TABLE = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            "  row record;\n" +
            "  i int;\n" +
            "\n" +
            "BEGIN \n" +
            "    OPEN tbl_cursor FOR \n" +
            "    ${sqlSelect};\n" +
            "\n" +
            "    MOVE FORWARD ${offset} FROM tbl_cursor;\n" +
            "    i \\:= 0;\n" +
            "    while i < ${limit} loop \n" +
            "       FETCH FROM tbl_cursor INTO row;\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "\n" +
            "       ${sqlInsert}\n" +
            "\n" +
            "       i \\:= i + 1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    public static final String SELECT_FROM_SOURCE_TABLE = "SELECT ${sourceColumns} \n" +
            "  FROM ${sourceTable} AS ${sourceAlias} \n";

    public static final String INSERT_INTO_TARGET_TABLE = "INSERT INTO ${targetTable}(${strColumns})\n" +
            "VALUES(${rowColumns});";

    //todo: get rid of infinity
    public static final String INSERT_ALL_VAL_FROM_DRAFT = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            "  row ${draftTable}%rowtype;\n" +
            "  i int;\n" +
            "\n" +
            "BEGIN \n" +
            "    OPEN tbl_cursor FOR \n" +
            "    SELECT ${draftAlias}.* FROM ${draftTable} AS ${draftAlias} \n" +
            "    ORDER BY ${draftAlias}.\"SYS_RECORDID\";\n" +
            "\n" +
            "    MOVE FORWARD ${offset} FROM tbl_cursor;\n" +
            "    i \\:= 0;\n" +
            "    while i < ${limit} loop \n" +
            "       FETCH FROM tbl_cursor INTO row;\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "\n" +
            "       row.\"SYS_RECORDID\" \\:= nextval('${targetSequence}');\n" +
            "       INSERT INTO ${targetTable}(\"SYS_RECORDID\", ${strColumns}, \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\")\n" +
            "       VALUES(row.\"SYS_RECORDID\", ${rowColumns}," +
            " to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone," +
            " coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity'));\n" +
            "\n" +
            "       i \\:= i + 1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    public static final String SELECT_ROWS_FROM_DATA_BY_FIELD_EQ = " SELECT %1$s FROM %2$s WHERE %3$s = %4$s ";

    public static final String COUNT_OLD_VAL_FROM_VERSION_WITH_CLOSE_TIME = SELECT_COUNT_ONLY +
            "  FROM %1$s AS v \n" +
            " WHERE NOT ( \n" +
            "       v.\"SYS_CLOSETIME\" IS NULL AND (to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\" OR \n" +
            "                                        to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_PUBLISHTIME\" AND \n" +
            "                                        ('%4$s' = 'null' OR to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\")) \n" +
            "       OR \n" +
            "       v.\"SYS_CLOSETIME\" IS NOT NULL AND (v.\"SYS_PUBLISHTIME\" = to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR \n" +
            "                                            v.\"SYS_CLOSETIME\" = to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR \n" +
            "                                            '%4$s' = 'null' AND to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_CLOSETIME\" OR \n" +
            "                                            (to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone) \n" +
            "                                            OVERLAPS \n" +
            "                                            (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\")) \n" +
            "       );";

    //todo: get rid of infinity
    public static final String INSERT_OLD_VAL_FROM_VERSION_WITH_CLOSE_DATE = "DO $$" +
            "DECLARE tbl_cursor refcursor; \n" +
            "  row record; \n" +
            "  i int; \n" +
            "\n" +
            "BEGIN \n" +
            "    OPEN tbl_cursor FOR \n" +
            "    SELECT \"SYS_RECORDID\", %7$s, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" \n" +
            "      FROM %2$s v \n" +
            "     WHERE NOT ( \n" +
            "           v.\"SYS_CLOSETIME\" IS NULL AND (to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\" OR \n" +
            "                                            to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_PUBLISHTIME\" AND \n" +
            "                                            ('%10$s' = 'null' OR to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\")) \n" +
            "           OR \n" +
            "           v.\"SYS_CLOSETIME\" IS NOT NULL AND (v.\"SYS_PUBLISHTIME\" = to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR \n" +
            "                                                v.\"SYS_CLOSETIME\" = to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR \n" +
            "                                                '%10$s' = 'null' AND to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_CLOSETIME\" OR \n" +
            "                                                (to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone) \n" +
            "                                                OVERLAPS \n" +
            "                                                (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\"))\n" +
            "           ) \n" +
            "     ORDER BY v.\"SYS_RECORDID\"; \n" +
            "\n" +
            "    MOVE FORWARD %4$s FROM tbl_cursor; \n" +
            "    i \\:= 0; \n" +
            "    while i < %5$s loop \n" +
            "       FETCH FROM tbl_cursor INTO row; \n" +
            "       EXIT WHEN NOT FOUND; \n" +
            "\n" +
            "       row.\"SYS_RECORDID\" \\:= nextval('%6$s'); \n" +
            "       INSERT INTO %1$s(\"SYS_RECORDID\", %7$s, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\") \n" +
            "       VALUES (row.\"SYS_RECORDID\", %8$s, row.\"FTS\", row.\"SYS_HASH\", row.\"SYS_PUBLISHTIME\", row.\"SYS_CLOSETIME\"); \n" +
            "\n" +
            "       i \\:= i + 1; \n" +
            "    end loop; \n" +
            "    CLOSE tbl_cursor; \n" +
            "END$$;";

    private static final String WHERE_EXISTS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = " WHERE exists(\n" +
            "       SELECT 1 FROM ${versionTable} AS ${versionAlias} \n" +
            "        WHERE ${versionAlias}.\"SYS_HASH\" = ${draftAlias}.\"SYS_HASH\"\n" +
            AND_IS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME +
            "       )\n";

    static final String COUNT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = SELECT_COUNT_ONLY +
            "  FROM ${draftTable} AS ${draftAlias} \n" +
            WHERE_EXISTS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME;

    static final String INSERT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = "DO $$ \n" +
            "DECLARE tbl_cursor refcursor; \n" +
            "  row record; \n" +
            "  i int; \n" +
            "\n" +
            "BEGIN \n" +
            "    OPEN tbl_cursor FOR \n" +
            "    SELECT ${draftAlias}.\"SYS_RECORDID\", ${draftColumns}, ${draftAlias}.\"FTS\", ${draftAlias}.\"SYS_HASH\" \n" +
            "      FROM ${draftTable} AS ${draftAlias} \n" +
            "    " + WHERE_EXISTS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME +
            "     ORDER BY ${draftAlias}.\"SYS_RECORDID\" \n" +
            ";\n" +
            "    MOVE FORWARD ${offset} FROM tbl_cursor; \n" +
            "    i \\:= 0; \n" +
            "    while i < ${limit} loop \n" +
            "      FETCH FROM tbl_cursor INTO row; \n" +
            "      EXIT WHEN NOT FOUND; \n" +
            "\n" +
            "      row.\"SYS_RECORDID\" \\:= nextval('${targetSequence}'); \n" +
            "      INSERT INTO ${targetTable} (\"SYS_RECORDID\", ${strColumns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\") \n" +
            "      SELECT \"SYS_RECORDID\", ${rowColumns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" \n" +
            "        FROM data.merged_actual_rows('${strColumns}'\\:\\:text, row.\"SYS_HASH\", '${versionTable}'\\:\\:text, \n" +
            "                  to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, \n" +
            "                  coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity'), \n" +
            "                  row.\"SYS_RECORDID\") \n" +
            "             t(\"SYS_RECORDID\" bigint, ${typedColumns}, \"FTS\" tsvector, \"SYS_HASH\" character(32), \n" +
            "               \"SYS_PUBLISHTIME\" timestamp without time zone, \"SYS_CLOSETIME\" timestamp without time zone); \n" +
            "\n" +
            "      i \\:= i + 1; \n" +
            "  end loop; \n" +
            "  CLOSE tbl_cursor; \n" +
            "END$$;";

    //todo: get rid of infinity
    public static final String COUNT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME = SELECT_COUNT_ONLY +
            "    FROM ${draftTable} d \n" +
            "   WHERE NOT exists( \n" +
            "         SELECT 1 \n" +
            "           FROM ${versionTable} v \n" +
            "          WHERE v.\"SYS_HASH\" = d.\"SYS_HASH\" \n" +
            "            AND ( (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'), \n" +
            "                   coalesce(to_timestamp('${closeTime}',   'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone,  'infinity')) \n" +
            "                  OVERLAPS \n" +
            "                  (coalesce(v.\"SYS_PUBLISHTIME\", '-infinity'), coalesce(v.\"SYS_CLOSETIME\", 'infinity')) \n" +
            "               OR (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity') = \n" +
            "                   coalesce(v.\"SYS_CLOSETIME\", 'infinity')) ) \n" +
            "         )";


    //todo: get rid of infinity
    public static final String INSERT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME = "DO $$ \n" +
            "DECLARE tbl_cursor refcursor; \n" +
            "   row record; \n" +
            "   i int; \n" +
            "\n" +
            "BEGIN \n" +
            "    OPEN tbl_cursor FOR \n" +
            "    SELECT \"SYS_RECORDID\", ${fields}, \"FTS\", \"SYS_HASH\" \n" +
            "      FROM ${draftTable} d \n" +
            "     WHERE NOT exists( \n" +
            "           SELECT 1 FROM ${versionTable} v \n" +
            "            WHERE v.\"SYS_HASH\" = d.\"SYS_HASH\" \n" +
            "              AND ( (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'), \n" +
            "                     coalesce(to_timestamp('${closeTime}',   'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone,  'infinity')) \n" +
            "                    OVERLAPS \n" +
            "                    (coalesce( v.\"SYS_PUBLISHTIME\", '-infinity'), coalesce(v.\"SYS_CLOSETIME\", 'infinity')) \n" +
            "                 OR (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity') = \n" +
            "                     coalesce(v.\"SYS_CLOSETIME\", 'infinity')) ) \n" +
            "           ) \n" +
            "     ORDER BY d.\"SYS_RECORDID\"; \n" +
            "\n" +
            "    MOVE FORWARD ${offset} FROM tbl_cursor; \n" +
            "    i \\:= 0;\n" +
            "    while i < ${limit} loop \n" +
            "       FETCH FROM tbl_cursor INTO row; \n" +
            "       EXIT WHEN NOT FOUND; \n" +
            "\n" +
            "       row.\"SYS_RECORDID\" \\:= nextval('${sequenceName}'); \n" +
            "       INSERT INTO ${targetTable} (\"SYS_RECORDID\", ${fields}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\") \n" +
            "       VALUES (row.\"SYS_RECORDID\", ${rowFields}, row.\"FTS\", row.\"SYS_HASH\", \n" +
            "               to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, \n" +
            "               case when '${closeTime}' = 'null' \n" +
            "                    then null\\:\\:timestamp without time zone \n" +
            "                    else to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone \n " +
            "               end); \n" +
            "\n" +
            "       i \\:= i + 1; \n" +
            "    end loop; \n" +
            "    CLOSE tbl_cursor; \n" +
            "END$$;";

    //todo: get rid of infinity
    public static final String COUNT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME = SELECT_COUNT_ONLY +
            "FROM ${versionTable} AS v \n" +
            " WHERE (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'), \n" +
            "        coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity')) \n" +
            "       OVERLAPS \n" +
            "       (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\") \n" +
            "   AND NOT exists(SELECT 1 FROM ${draftTable} AS d WHERE d.\"SYS_HASH\" = v.\"SYS_HASH\");";

    //todo: get rid of infinity
    public static final String INSERT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME = "DO $$ \n" +
            "DECLARE tbl_cursor refcursor; \n" +
            "  row record; \n" +
            "  i int; \n" +
            "\n" +
            "BEGIN \n" +
            "    OPEN tbl_cursor FOR \n" +
            "    SELECT \"SYS_RECORDID\", ${strColumns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" \n" +
            "      FROM ${versionTable} AS v \n" +
            "     WHERE (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'),\n" +
            "            coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity')) \n" +
            "           OVERLAPS \n" +
            "           (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\") \n" +
            "       AND NOT exists(SELECT 1 FROM ${draftTable} AS d WHERE d.\"SYS_HASH\" = v.\"SYS_HASH\") \n" +
            "     ORDER BY v.\"SYS_RECORDID\";" +
            "\n" +
            "    MOVE FORWARD ${offset} FROM tbl_cursor; \n" +
            "    i \\:= 0; \n" +
            "    while i < ${limit} loop \n" +
            "       FETCH FROM tbl_cursor INTO row; \n" +
            "       EXIT WHEN NOT FOUND; \n" +
            "\n" +
            "       INSERT INTO ${targetTable}(\"SYS_RECORDID\", ${strColumns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\") \n" +
            "       SELECT \"SYS_RECORDID\", ${strColumns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" \n" +
            "         FROM data.closed_now_records('\"SYS_RECORDID\", ${strColumns}, \"FTS\", \"SYS_HASH\", \n" +
            "                   \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\"'\\:\\:text, row.\"SYS_RECORDID\", '${versionTable}'\\:\\:text, \n" +
            "                   to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, \n" +
            "                   to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, \n" +
            "                   '${sequenceName}'\\:\\:text) \n" +
            "              t(\"SYS_RECORDID\" bigint, ${typedColumns}, \"FTS\" tsvector, \"SYS_HASH\" character(32), \n" +
            "                \"SYS_PUBLISHTIME\" timestamp without time zone, \"SYS_CLOSETIME\" timestamp without time zone); \n" +
            "\n" +
            "       i \\:= i + 1; \n" +
            "    end loop; \n" +
            "    CLOSE tbl_cursor; \n" +
            "END$$;";

    public static final String INSERT_FROM_SELECT_ACTUAL_DATA = "INSERT INTO data.%1$s(%2$s) SELECT %2$s FROM data.%3$s WHERE \"SYS_CLOSETIME\" IS NULL;";

    private QueryConstants() {
        // Nothing to do.
    }
}

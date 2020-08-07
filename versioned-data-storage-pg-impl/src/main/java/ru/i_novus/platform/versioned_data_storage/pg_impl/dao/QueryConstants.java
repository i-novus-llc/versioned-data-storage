package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import ru.i_novus.platform.datastorage.temporal.util.StringUtils;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static ru.i_novus.platform.datastorage.temporal.model.StorageConstants.*;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.addDoubleQuotes;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.addSingleQuotes;

/**
 * @author lgalimova
 * @since 22.03.2018
 */
public class QueryConstants {

    public static final int TRANSACTION_ROW_LIMIT = 1000;

    // Формат даты для использования в запросах.
    static final String DATE_FORMAT_FOR_INSERT_ROW = "yyyy-MM-dd";
    static final String QUERY_DATE_FORMAT = "DD.MM.YYYY";

    // Формат даты-времени для использования в запросах.
    static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DATETIME_FORMAT);
    static final String QUERY_TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS";

    public static final String NAME_SEPARATOR = ".";
    public static final String ALIAS_OPERATOR = " as ";
    public static final String DEFAULT_TABLE_ALIAS = "d";
    static final String TRIGGER_NEW_ALIAS = "NEW";

    private static final List<String> SYS_RECORD_LIST = Arrays.asList(SYS_PRIMARY_COLUMN,
            SYS_PUBLISHTIME, SYS_CLOSETIME,
            SYS_HASH, SYS_PATH, SYS_FTS
    );
    private static final String SYS_RECORDS_TEXT = SYS_RECORD_LIST.stream()
            .map(StringUtils::addSingleQuotes)
            .collect(Collectors.joining(", "));

    static final String DATE_BEGIN = "DATEBEG";
    static final String DATE_END = "DATEEND";
    static final String HAS_CHILD_BRANCH = "SYS_HAS_CHILD_BRANCH";

    static final String QUERY_NULL_VALUE = "null";
    static final String QUERY_VALUE_SUBST = "?";
    static final String QUERY_LTREE_SUBST = QUERY_VALUE_SUBST + "\\:\\:ltree";

    public static final String MIN_TIMESTAMP_VALUE = "'-infinity'";
    public static final String MAX_TIMESTAMP_VALUE = "'infinity'";
    public static final String TO_TIMESTAMP = "to_timestamp(%s, '" + QUERY_TIMESTAMP_FORMAT + "')";
    public static final String TIMESTAMP_WITHOUT_TIME_ZONE = "\\:\\:timestamp without time zone";

    public static final String REFERENCE_FIELD_SQL_TYPE = "jsonb";
    public static final String REFERENCE_FIELD_VALUE_OPERATOR = "->>";

    static final String SELECT_COUNT_ONLY = "SELECT count(*)\n";
    static final String WHERE_DEFAULT = " true"; // 1 = 1
    static final String SELECT_FROM = "  FROM ";
    static final String SELECT_WHERE = " WHERE" + WHERE_DEFAULT + "\n";
    static final String SELECT_ORDER = " ORDER BY ";
    private static final String ORDER_BY_ONE_FIELD = SELECT_ORDER + " %1$s.\"%2$s\"\n";
    private static final String SELECT_LIMIT = " LIMIT ${limit}";
    private static final String SELECT_OFFSET = " OFFSET ${offset}";
    private static final String SUBQUERY_INDENT = "       ";
    private static final String ROUTINE_INDENT = "    ";

    private static final String COALESCE_PUBLISH_TIME_FIELD = "coalesce(%s.\"%s\", '-infinity')";
    private static final String COALESCE_CLOSE_TIME_FIELD = "coalesce(%s.\"%s\", 'infinity')";
    private static final String COALESCE_PUBLISH_TIME_VALUE = "coalesce(to_timestamp('${publishTime}', " +
            "'" + QUERY_TIMESTAMP_FORMAT + "')\\:\\:timestamp without time zone, '-infinity')";
    private static final String COALESCE_CLOSE_TIME_VALUE = "coalesce(to_timestamp('${closeTime}', " +
            "'" + QUERY_TIMESTAMP_FORMAT + "')\\:\\:timestamp without time zone, '-infinity')";

    static final String DRAFT_TABLE_ALIAS = "d";
    static final String VERSION_TABLE_ALIAS = "v";

    private static final String FROM_DRAFT_TABLE = "  FROM ${draftTable} AS ${draftAlias} \n";
    private static final String FROM_VERSION_TABLE = "  FROM ${versionTable} AS ${versionAlias} \n";

    private static final String ORDER_DRAFT_BY_SYS_RECORDID = String.format(ORDER_BY_ONE_FIELD, "${draftAlias}", SYS_PRIMARY_COLUMN);
    private static final String ORDER_VERSION_BY_SYS_RECORDID = String.format(ORDER_BY_ONE_FIELD, "${versionAlias}", SYS_PRIMARY_COLUMN);

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

    static final String CREATE_DRAFT_TABLE = "CREATE TABLE %1$s.%2$s (" +
            "  \"SYS_RECORDID\" bigserial NOT NULL," +
            "  %3$s" +
            "  \"FTS\" tsvector," +
            "  \"SYS_HASH\" char(32) UNIQUE," +
            "  CONSTRAINT \"%4$s_pkey\" PRIMARY KEY (\"SYS_RECORDID\")" +
            ");";
    static final String DROP_TABLE = "DROP TABLE IF EXISTS %1$s.%2$s";
    static final String TRUNCATE_TABLE = "TRUNCATE TABLE %1$s.%2$s;";

    static final String CREATE_TABLE_COPY = "CREATE TABLE %1$s.%2$s AS " +
            "SELECT * FROM %3$s.%4$s WITH NO DATA;";

    static final String CREATE_TABLE_SEQUENCE = "CREATE SEQUENCE %1$s.\"%2$s_%3$s_seq\" start 1";
    static final String DROP_TABLE_SEQUENCE = "DROP SEQUENCE IF EXISTS %1$s.\"%2$s_%3$s_seq\" CASCADE";

    static final String CREATE_TRIGGER = "CREATE OR REPLACE FUNCTION %1$s.\"%2$s_%3$s\"()\n" +
            "  RETURNS trigger AS\n" +
            "$BODY$\n" +
            "  BEGIN\n" +
            "    %6$s\n" +
            "    RETURN NEW;\n" +
            "  END;\n" +
            "$BODY$ LANGUAGE plpgsql;\n" +
            "\n" +
            "CREATE TRIGGER %4$s \n" +
            "  BEFORE INSERT OR UPDATE OF %5$s\n" +
            "  ON %1$s.\"%2$s\"\n" +
            "  FOR EACH ROW\n" +
            "  EXECUTE PROCEDURE %1$s.\"%2$s_%3$s\"();";

    static final String DROP_TRIGGER = "DROP TRIGGER IF EXISTS %1$s ON %2$s.%3$s;";

    static final String HASH_FUNCTION_NAME = "hash_tf";
    static final String HASH_TRIGGER_NAME = "hash_tg";
    public static final String HASH_EXPRESSION = "md5(ROW(%s)||'')";

    static final String FTS_FUNCTION_NAME = "fts_vector_tf";
    static final String FTS_TRIGGER_NAME = "fts_vector_tg";

    static final String ALTER_ADD_PRIMARY_KEY = "ALTER TABLE %1$s.%2$s ADD PRIMARY KEY (\"%3$s\");";
    static final String ALTER_SET_SEQUENCE_FOR_PRIMARY_KEY = "ALTER TABLE %1$s.%2$s \n" +
            "  ALTER COLUMN \"%3$s\" SET DEFAULT nextval('%1$s.\"%4$s_%3$s_seq\"');";

    static final String ASSIGN_FIELD = "%1$s = %2$s";
    static final String UPDATE_FIELD = "UPDATE %1$s.%2$s SET %3$s;";

    static final String ALTER_ADD_COLUMN = "ALTER TABLE %1$s.\"%2$s\" ADD COLUMN \"%3$s\" %4$s %5$s;";
    static final String COLUMN_DEFAULT = "DEFAULT %s";
    static final String ALTER_DELETE_COLUMN = "ALTER TABLE %1$s.\"%2$s\" DROP COLUMN \"%3$s\" CASCADE;";
    static final String ALTER_COLUMN_WITH_USING = "ALTER TABLE %1$s.%2$s ALTER COLUMN %3$s SET DATA TYPE %4$s USING %5$s";

    static final String INSERT_RECORD = "INSERT INTO %1$s.%2$s (%3$s)\n";
    static final String INSERT_VALUES = "VALUES(%s)\n";
    static final String INSERT_SELECT = "SELECT %4$s \n" + "  FROM %1$s.%2$s as %3$s \n" + SELECT_WHERE;
    static final String DELETE_RECORD = "DELETE FROM %1$s.%2$s WHERE %3$s";
    static final String UPDATE_RECORD = "UPDATE %1$s.%2$s as %3$s SET %4$s WHERE %5$s";
    static final String UPDATE_VALUE = "%1$s = %2$s";

    static final String CONDITION_IN = "%1$s IN (%2$s)";
    static final String CONDITION_ANY_BIGINT = "%1$s = ANY(%2$s\\:\\:bigint[])";
    static final String CONDITION_POINT_DATED = " \"SYS_PUBLISHTIME\" = \"SYS_CLOSETIME\" ";

    private static final String AND_EXISTS_VERSION_REF_VALUE = "   AND ${versionAlias}.${refFieldName} is not null \n" +
            "   AND (${versionAlias}.${refFieldName} -> 'value') is not null \n";

    static final String COUNT_REFERENCE_IN_REF_ROWS = SELECT_COUNT_ONLY +
            FROM_VERSION_TABLE +
            SELECT_WHERE +
            AND_EXISTS_VERSION_REF_VALUE;

    static final String SELECT_REFERENCE_IN_REF_ROWS = "\nSELECT ${versionAlias}.\"SYS_RECORDID\" \n" +
            FROM_VERSION_TABLE +
            SELECT_WHERE +
            AND_EXISTS_VERSION_REF_VALUE +
            ORDER_VERSION_BY_SYS_RECORDID +
            SELECT_LIMIT + SELECT_OFFSET;

    static final String REFERENCE_VALUATION_UPDATE_TABLE = "b";
    static final String REFERENCE_VALUATION_SELECT_TABLE = "d";
    static final String REFERENCE_VALUATION_SELECT_SUBST = "select jsonb_build_object('value', ?)";
    static final String REFERENCE_VALUATION_SELECT_EXPRESSION = "select jsonb_build_object(" +
            addSingleQuotes(REFERENCE_VALUE_NAME) + ", %3$s.%4$s, " +
            addSingleQuotes(REFERENCE_DISPLAY_VALUE_NAME) + ", %5$s, " +
            addSingleQuotes(REFERENCE_HASH_NAME) + ", %3$s." + addDoubleQuotes(SYS_HASH) +
            ") " +
            "  from %1$s.%2$s as %3$s " +
            " where %3$s.%4$s=%6$s\\:\\:%7$s %8$s";
    static final String REFERENCE_VALUATION_OLD_VALUE =
            "(case when %1$s is null then null else %1$s->>'value' end)";

    public static final String IS_VERSION_NOT_EMPTY = "SELECT exists(SELECT * FROM data.%s);";
    public static final String IS_FIELD_NOT_EMPTY = "SELECT exists(SELECT * FROM data.%s WHERE %s.%s IS NOT NULL);";
    public static final String IS_FIELD_CONTAIN_EMPTY_VALUES = "SELECT exists(SELECT * FROM data.%s WHERE %s.%s IS NULL);";
    public static final String IS_RELATED_VALUE_EXIST = "SELECT exists(SELECT * FROM data.%s where %s.%s = %s)";

    static final String BIND_INFO_SCHEMA_NAME = "schemaName";
    static final String BIND_INFO_TABLE_NAME = "tableName";
    static final String BIND_INFO_COLUMN_NAME = "columnName";

    public static final String SELECT_SCHEMA_EXISTS = "SELECT EXISTS(\n" +
            "  SELECT 1 \n" +
            "    FROM \"information_schema\".\"schemata\" \n" +
            "   WHERE schema_name = :schemaName \n" +
            ")";

    public static final String SELECT_TABLE_EXISTS = "SELECT EXISTS(\n" +
            "  SELECT 1 \n" +
            "    FROM \"information_schema\".\"tables\" \n" +
            "   WHERE table_schema = :schemaName \n" +
            "     AND table_name = :tableName \n" +
            ")";

    private static final String SELECT_ESCAPED_COLUMN_NAME = "SELECT '\"' || column_name || '\"' \n";
    private static final String FROM_INFO_SCHEMA_COLUMNS = "  FROM \"information_schema\".\"columns\" \n";

    private static final String AND_INFO_SCHEMA_NAME = "  AND table_schema = :schemaName \n";
    private static final String AND_INFO_TABLE_NAME = "  AND table_name = :tableName \n";
    private static final String AND_INFO_COLUMN_NAME = "  AND column_name = :columnName \n";
    static final String AND_INFO_COLUMN_NOT_IN_SYS_COLUMNS = "  AND column_name NOT IN (" + SYS_RECORDS_TEXT + ")";

    static final String SELECT_ESCAPED_FIELD_NAMES = SELECT_ESCAPED_COLUMN_NAME +
            FROM_INFO_SCHEMA_COLUMNS +
            SELECT_WHERE +
            AND_INFO_SCHEMA_NAME +
            AND_INFO_TABLE_NAME;
    static final String SELECT_HASH_USED_FIELD_NAMES = SELECT_ESCAPED_COLUMN_NAME +
            "       || (case when data_type = '" + REFERENCE_FIELD_SQL_TYPE + "' then '->>''value''' else '' end)\n" +
            FROM_INFO_SCHEMA_COLUMNS +
            SELECT_WHERE +
            AND_INFO_SCHEMA_NAME +
            AND_INFO_TABLE_NAME +
            AND_INFO_COLUMN_NOT_IN_SYS_COLUMNS;
    public static final String SELECT_FIELD_TYPE = "SELECT data_type \n" +
            FROM_INFO_SCHEMA_COLUMNS +
            SELECT_WHERE +
            AND_INFO_SCHEMA_NAME +
            AND_INFO_TABLE_NAME +
            AND_INFO_COLUMN_NAME;
    public static final String SELECT_FIELD_NAMES_AND_TYPES = "SELECT column_name, data_type \n" +
            FROM_INFO_SCHEMA_COLUMNS +
            SELECT_WHERE +
            AND_INFO_SCHEMA_NAME +
            AND_INFO_TABLE_NAME;

    public static final String SELECT_DDL_INDEXES = "SELECT indexdef \n" +
            "  FROM pg_indexes \n" +
            " WHERE schemaname = :schemaName \n" +
            "   AND tablename = :tableName \n";
    public static final String AND_NOT_SYS_HASH_DDL_INDEX =
            "   AND NOT indexdef LIKE '%" + addDoubleQuotes(SYS_HASH)  + "%'\n";

    static final String CREATE_TABLE_INDEX = "CREATE INDEX %1$s ON %2$s.%3$s(%4$s);";
    static final String DROP_TABLE_INDEX = "DROP INDEX IF EXISTS %1$s.%2$s;";
    static final String CREATE_FTS_INDEX = "CREATE INDEX %1$s ON %2$s.%3$s USING gin (%4$s);";
    static final String CREATE_LTREE_INDEX = "CREATE INDEX %1$s ON %2$s.%3$s USING gist (%4$s);";
    static final String IF_TABLE_INDEX_EXISTS = "SELECT exists(SELECT * \n" +
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

    static final String ROW_TYPE_VAR_NAME = "row";

    public static final String INSERT_DATA_BY_SELECT_FROM_TABLE = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            "  row ${sourceTable}%rowtype;\n" +
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

    public static final String SELECT_ALL_DATA_BY_FROM_TABLE = "SELECT ${sourceAlias}.* \n" +
            "  FROM ${sourceTable} AS ${sourceAlias} \n" +
            " ORDER BY ${sourceAlias}." + addDoubleQuotes(SYS_PRIMARY_COLUMN);

    public static final String INSERT_ALL_DATA_BY_FROM_TABLE = "INSERT INTO ${targetTable}(${strColumns})\n" +
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

    public static final String SELECT_ROWS_FROM_DATA = " select %s from data.%s d where %s";

    public static final String SELECT_ROWS_FROM_DATA_BY_FIELD_ONE = " select %1$s from %2$s.%3$s where %4$s = %5$s ";

    public static final String SELECT_ROWS_FROM_DATA_BY_FIELD_ANY = " select %1$s from %2$s.%3$s where %4$s = ANY(%5$s\\:\\:bigint[]) ";

    public static final String SELECT_RELATION_ROW_FROM_DATA = " select %s from data.%s where %s=? limit 1;\n";

    public static final String COUNT_OLD_VAL_FROM_VERSION_WITH_CLOSE_TIME = SELECT_COUNT_ONLY +
            "  FROM data.%1$s v \n" +
            " WHERE NOT (\n" +
            "       v.\"SYS_CLOSETIME\" IS NULL AND (to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\" OR \n" +
            "                                        to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_PUBLISHTIME\" AND \n" +
            "                                        ('%4$s' = 'null' OR to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\"))\n" +
            "       OR \n" +
            "       v.\"SYS_CLOSETIME\" IS NOT NULL AND (v.\"SYS_PUBLISHTIME\" = to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR \n" +
            "                                            v.\"SYS_CLOSETIME\" = to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR \n" +
            "                                            '%4$s' = 'null' AND to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_CLOSETIME\" OR \n" +
            "                                            (to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone)\n" +
            "                                            OVERLAPS \n" +
            "                                            (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\"))\n" +
            "       );";

    //todo: get rid of infinity
    public static final String INSERT_OLD_VAL_FROM_VERSION_WITH_CLOSE_DATE = "DO $$" +
            "DECLARE tbl_cursor refcursor;\n" +
            "  row record;\n" +
            "  i int;\n" +
            "\n" +
            "BEGIN \n" +
            "    OPEN tbl_cursor FOR \n" +
            "    SELECT \"SYS_RECORDID\", %7$s, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" \n" +
            "      FROM data.%2$s v \n" +
            "     WHERE NOT (\n" +
            "           v.\"SYS_CLOSETIME\" IS NULL AND (to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\" OR \n" +
            "                                            to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_PUBLISHTIME\" AND \n" +
            "                                            ('%10$s' = 'null' OR to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\"))\n" +
            "           OR \n" +
            "           v.\"SYS_CLOSETIME\" IS NOT NULL AND (v.\"SYS_PUBLISHTIME\" = to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR \n" +
            "                                                v.\"SYS_CLOSETIME\" = to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR \n" +
            "                                                '%10$s' = 'null' AND to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_CLOSETIME\" OR \n" +
            "                                                (to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone)\n" +
            "                                                OVERLAPS \n" +
            "                                                (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\"))\n" +
            "           )\n" +
            "     ORDER BY v.\"SYS_RECORDID\";\n" +
            "\n" +
            "    MOVE FORWARD %4$s FROM tbl_cursor;\n" +
            "    i \\:= 0;\n" +
            "    while i < %5$s loop \n" +
            "       FETCH FROM tbl_cursor INTO row;\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "\n" +
            "       row.\"SYS_RECORDID\" \\:= nextval('data.%6$s');\n" +
            "       INSERT INTO data.%1$s(\"SYS_RECORDID\", %7$s, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\") \n" +
            "       VALUES (row.\"SYS_RECORDID\", %8$s, row.\"FTS\", row.\"SYS_HASH\", row.\"SYS_PUBLISHTIME\", row.\"SYS_CLOSETIME\");\n" +
            "\n" +
            "       i \\:= i + 1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    public static final String COPY_DATA = " DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            "  row data.%1$s%%rowtype;\n" +
            "  i int;\n" +
            "\n" +
            "BEGIN\n" +
            "    OPEN tbl_cursor FOR\n" +
            "    SELECT * from data.%2$s\n" +
            "     ORDER BY \"SYS_RECORDID\";\n" +
            "\n" +
            "    MOVE FORWARD %3$s FROM tbl_cursor;\n" +
            "    i \\:= 0;\n" +
            "    while i < %4$s loop \n" +
            "      FETCH FROM tbl_cursor INTO row;\n" +
            "      EXIT WHEN NOT FOUND;\n" +
            "\n" +
            "      row.\"SYS_RECORDID\" \\:= nextval('data.%5$s');\n" +
            "      INSERT INTO data.%1$s VALUES(row.*);\n" +
            "\n" +
            "      i \\:= i + 1;\n" +
            "  end loop;\n" +
            "  CLOSE tbl_cursor;\n" +
            "END$$;";

    private static final String WHERE_EXISTS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = " WHERE exists(\n" +
            "       SELECT 1 " + FROM_VERSION_TABLE +
            "        WHERE ${versionAlias}.\"SYS_HASH\" = ${draftAlias}.\"SYS_HASH\"\n" +
            SUBQUERY_INDENT +
            AND_IS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME.replace("\n", "\n" + SUBQUERY_INDENT) +
            "       )\n";

    static final String COUNT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = SELECT_COUNT_ONLY +
            FROM_DRAFT_TABLE +
            WHERE_EXISTS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME;

    static final String INSERT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            "  row record;\n" +
            "  i int;\n" +
            "\n" +
            "BEGIN \n" +
            "    OPEN tbl_cursor FOR \n" +
            "    SELECT ${draftAlias}.\"SYS_RECORDID\", ${draftColumns}, ${draftAlias}.\"FTS\", ${draftAlias}.\"SYS_HASH\" \n" +
            ROUTINE_INDENT + FROM_DRAFT_TABLE +
            ROUTINE_INDENT + WHERE_EXISTS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME +
            ROUTINE_INDENT + ORDER_DRAFT_BY_SYS_RECORDID + ";\n" +
            "\n" +
            "    MOVE FORWARD ${offset} FROM tbl_cursor;\n" +
            "    i \\:= 0;\n" +
            "    while i < ${limit} loop \n" +
            "      FETCH FROM tbl_cursor INTO row;\n" +
            "      EXIT WHEN NOT FOUND;\n" +
            "\n" +
            "      row.\"SYS_RECORDID\" \\:= nextval('${targetSequence}');\n" +
            "      INSERT INTO ${targetTable} (\"SYS_RECORDID\", ${strColumns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\")\n" +
            "      SELECT \"SYS_RECORDID\", ${strColumns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" " +
            "        FROM data.merged_actual_rows('${strColumns}'\\:\\:text, row.\"SYS_HASH\", '${versionTable}'\\:\\:text,\n" +
            "                  to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone,\n" +
            "                  coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity'),\n" +
            "                  row.\"SYS_RECORDID\")\n" +
            "             t(\"SYS_RECORDID\" bigint, ${typedColumns}, \"FTS\" tsvector, \"SYS_HASH\" character(32),\n" +
            "               \"SYS_PUBLISHTIME\" timestamp without time zone, \"SYS_CLOSETIME\" timestamp without time zone);\n" +
            "\n" +
            "      i \\:= i + 1;\n" +
            "  end loop;\n" +
            "  CLOSE tbl_cursor;\n" +
            "END$$;";

    //todo: get rid of infinity
    public static final String COUNT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME = SELECT_COUNT_ONLY +
            "    FROM ${draftTable} d \n" +
            "   WHERE NOT exists(\n" +
            "         SELECT 1 \n" +
            "           FROM ${versionTable} v \n" +
            "          WHERE v.\"SYS_HASH\" = d.\"SYS_HASH\" \n" +
            "            AND ( (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'),\n" +
            "                   coalesce(to_timestamp('${closeTime}',   'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone,  'infinity'))\n" +
            "                  OVERLAPS \n" +
            "                  (coalesce(v.\"SYS_PUBLISHTIME\", '-infinity'), coalesce(v.\"SYS_CLOSETIME\", 'infinity'))\n" +
            "               OR (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity') =\n" +
            "                   coalesce(v.\"SYS_CLOSETIME\", 'infinity')) )\n" +
            "         )";


    //todo: get rid of infinity
    public static final String INSERT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            "   row record;\n" +
            "   i int;\n" +
            "\n" +
            "BEGIN \n" +
            "    OPEN tbl_cursor FOR \n" +
            "    SELECT \"SYS_RECORDID\", ${fields}, \"FTS\", \"SYS_HASH\"\n" +
            "      FROM ${draftTable} d \n" +
            "     WHERE NOT exists(\n" +
            "           SELECT 1 FROM ${versionTable} v \n" +
            "            WHERE v.\"SYS_HASH\" = d.\"SYS_HASH\"\n" +
            "              AND ( (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'),\n" +
            "                     coalesce(to_timestamp('${closeTime}',   'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone,  'infinity'))\n" +
            "                    OVERLAPS \n" +
            "                    (coalesce( v.\"SYS_PUBLISHTIME\", '-infinity'), coalesce(v.\"SYS_CLOSETIME\", 'infinity'))\n" +
            "                 OR (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity') =\n" +
            "                     coalesce(v.\"SYS_CLOSETIME\", 'infinity')) )\n" +
            "           )\n" +
            "     ORDER BY d.\"SYS_RECORDID\"; "+
            "\n" +
            "    MOVE FORWARD ${offset} FROM tbl_cursor;\n" +
            "    i \\:= 0;\n" +
            "    while i < ${limit} loop \n" +
            "       FETCH FROM tbl_cursor INTO row;\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "\n" +
            "       row.\"SYS_RECORDID\" \\:= nextval('${sequenceName}');\n" +
            "       INSERT INTO ${targetTable} (\"SYS_RECORDID\", ${fields}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\")\n" +
            "       VALUES (row.\"SYS_RECORDID\", ${rowFields}, row.\"FTS\", row.\"SYS_HASH\"," +
            " to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone," +
            " case when '${closeTime}' = 'null' then null\\:\\:timestamp without time zone else to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone end);\n" +
            "\n" +
            "       i \\:= i + 1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    //todo: get rid of infinity
    public static final String COUNT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME = SELECT_COUNT_ONLY +
            "FROM ${versionTable} AS v\n" +
            " WHERE (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'),\n" +
            "        coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity'))\n" +
            "       OVERLAPS \n" +
            "       (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\")\n" +
            "   AND NOT exists(SELECT 1 FROM ${draftTable} AS d WHERE d.\"SYS_HASH\" = v.\"SYS_HASH\");";

    //todo: get rid of infinity
    public static final String INSERT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            "  row record;\n" +
            "  i int;\n" +
            "\n" +
            "BEGIN \n" +
            "    OPEN tbl_cursor FOR \n" +
            "    SELECT \"SYS_RECORDID\", ${strColumns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" \n" +
            "      FROM ${versionTable} AS v \n" +
            "     WHERE (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'),\n" +
            "            coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity'))\n" +
            "           OVERLAPS\n" +
            "           (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\")\n" +
            "       AND NOT exists(SELECT 1 FROM ${draftTable} AS d WHERE d.\"SYS_HASH\" = v.\"SYS_HASH\")\n" +
            "     ORDER BY v.\"SYS_RECORDID\";" +
            "\n" +
            "    MOVE FORWARD ${offset} FROM tbl_cursor;\n" +
            "    i \\:= 0;\n" +
            "    while i < ${limit} loop\n" +
            "       FETCH FROM tbl_cursor INTO row;\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "\n" +
            "       INSERT INTO ${targetTable}(\"SYS_RECORDID\", ${strColumns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\")\n" +
            "       SELECT \"SYS_RECORDID\", ${strColumns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" \n" +
            "         FROM data.closed_now_records('\"SYS_RECORDID\", ${strColumns}, \"FTS\", \"SYS_HASH\",\n" +
            "                   \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\"'\\:\\:text, row.\"SYS_RECORDID\", '${versionTable}'\\:\\:text,\n" +
            "                   to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone,\n" +
            "                   to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone,\n" +
            "                   '${sequenceName}'\\:\\:text)\n" +
            "              t(\"SYS_RECORDID\" bigint, ${typedColumns}, \"FTS\" tsvector, \"SYS_HASH\" character(32),\n" +
            "                \"SYS_PUBLISHTIME\" timestamp without time zone, \"SYS_CLOSETIME\" timestamp without time zone);\n" +
            "\n" +
            "       i \\:= i + 1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    public static final String INSERT_FROM_SELECT_ACTUAL_DATA = "INSERT INTO data.%1$s(%2$s) SELECT %2$s FROM data.%3$s WHERE \"SYS_CLOSETIME\" IS NULL;";

    private QueryConstants() {
    }

    // NB: Workaround to sonar issue "squid-S2386".
    public static List<String> systemFieldList() {
        return SYS_RECORD_LIST;
    }
}

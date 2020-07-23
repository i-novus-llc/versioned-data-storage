package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static ru.i_novus.platform.datastorage.temporal.model.DataConstants.*;

/**
 * @author lgalimova
 * @since 22.03.2018
 */
public class QueryConstants {

    public static final int TRANSACTION_SIZE = 1000;

    static final String DATE_FORMAT_FOR_INSERT_ROW = "yyyy-MM-dd";
    static final String DATE_FORMAT_FOR_USING_CONVERTING = "DD.MM.YYYY";
    static final String TIMESTAMP_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String SQL_ALIAS_OPERATOR = " as ";
    public static final String DEFAULT_TABLE_ALIAS = "d";

    private static final List<String> SYS_RECORD_LIST = Arrays.asList(SYS_PRIMARY_COLUMN,
            SYS_PUBLISHTIME, SYS_CLOSETIME,
            SYS_HASH, SYS_PATH, SYS_FULL_TEXT_SEARCH
    );
    private static final String SYS_RECORDS_TEXT = SYS_RECORD_LIST.stream()
            .map(QueryUtil::addSingleQuotes)
            .collect(Collectors.joining(", "));

    static final String DATE_BEGIN = "DATEBEG";
    static final String DATE_END = "DATEEND";
    static final String HAS_CHILD_BRANCH = "SYS_HAS_CHILD_BRANCH";

    static final String QUERY_NULL_VALUE = "null";
    static final String QUERY_VALUE_SUBST = "?";
    static final String QUERY_LTREE_SUBST = QUERY_VALUE_SUBST + "\\:\\:ltree";

    public static final String MIN_DATETIME_VALUE = "'-infinity'";
    public static final String MAX_DATETIME_VALUE = "'infinity'";

    public static final String REFERENCE_FIELD_SQL_TYPE = "jsonb";
    public static final String REFERENCE_FIELD_VALUE_OPERATOR = "->>";

    static final String SELECT_COUNT_ONLY = "SELECT count(*)\n";
    static final String WHERE_DEFAULT = "1 = 1";
    private static final String SELECT_WHERE = " WHERE " + WHERE_DEFAULT + "\n";
    private static final String ORDER_BY_ONE_FIELD = " ORDER BY %s.\"%s\"\n";
    private static final String SELECT_LIMIT = " LIMIT ${limit}";
    private static final String SELECT_OFFSET = " OFFSET ${offset}";
    private static final String SUBQUERY_INDENT = "       ";

    private static final String COALESCE_PUBLISH_TIME_FIELD = "coalesce(%s.\"%s\", '-infinity')";
    private static final String COALESCE_CLOSE_TIME_FIELD = "coalesce(%s.\"%s\", 'infinity')";
    private static final String COALESCE_PUBLISH_TIME_VALUE = "coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity')";
    private static final String COALESCE_CLOSE_TIME_VALUE = "coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity')";

    private static final String DRAFT_TABLE_ALIAS = "d";
    private static final String VERSION_TABLE_ALIAS = "v";

    private static final String FROM_DRAFT_TABLE = "  FROM ${draftTable} " + DRAFT_TABLE_ALIAS + "\n";
    private static final String FROM_VERSION_TABLE = "  FROM ${versionTable} " + VERSION_TABLE_ALIAS + "\n";

    private static final String ORDER_DRAFT_BY_SYS_RECORDID = String.format(ORDER_BY_ONE_FIELD, DRAFT_TABLE_ALIAS, SYS_PRIMARY_COLUMN);
    private static final String ORDER_VERSION_BY_SYS_RECORDID = String.format(ORDER_BY_ONE_FIELD, VERSION_TABLE_ALIAS, SYS_PRIMARY_COLUMN);

    private static final String COALESCE_VERSION_PUBLISH_TIME_FIELD = String.format(COALESCE_PUBLISH_TIME_FIELD, VERSION_TABLE_ALIAS, SYS_PUBLISHTIME);
    private static final String COALESCE_VERSION_CLOSE_TIME_FIELD = String.format(COALESCE_CLOSE_TIME_FIELD, VERSION_TABLE_ALIAS, SYS_CLOSETIME);

    //todo: get rid of infinity
    private static final String AND_IS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = "   AND (\n" +
            "        (" + COALESCE_PUBLISH_TIME_VALUE + ", " + COALESCE_CLOSE_TIME_VALUE + ")\n" +
            "        OVERLAPS (" + COALESCE_VERSION_PUBLISH_TIME_FIELD + ", " + COALESCE_VERSION_CLOSE_TIME_FIELD + ")\n" +
            "        OR (" + COALESCE_PUBLISH_TIME_VALUE + " = " + COALESCE_VERSION_CLOSE_TIME_FIELD + ") )\n";

    public static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE data.%s (" +
            "  \"SYS_RECORDID\" bigserial NOT NULL," +
            "  %s," +
            "  \"FTS\" tsvector," +
            "  \"SYS_HASH\" char(32)," +
            "  \"SYS_PUBLISHTIME\" timestamp without time zone," +
            "  \"SYS_CLOSETIME\" timestamp without time zone," +
            "  CONSTRAINT \"%s_pkey\" PRIMARY KEY (\"SYS_RECORDID\")" +
            ");";

    public static final String CREATE_EMPTY_DRAFT_TABLE_TEMPLATE = "CREATE TABLE data.%s (" +
            " \"SYS_RECORDID\" bigserial NOT NULL," +
            "  \"FTS\" tsvector," +
            "  \"SYS_HASH\" char(32) UNIQUE," +
            "  CONSTRAINT \"%s_pkey\" PRIMARY KEY (\"SYS_RECORDID\")" +
            ");";

    public static final String CREATE_DRAFT_TABLE_TEMPLATE = "CREATE TABLE data.%s (" +
            "  \"SYS_RECORDID\" bigserial NOT NULL," +
            "  %s," +
            "  \"FTS\" tsvector," +
            "  \"SYS_HASH\" char(32) UNIQUE," +
            "  CONSTRAINT \"%s_pkey\" PRIMARY KEY (\"SYS_RECORDID\")" +
            ");";

    public static final String COPY_TABLE_TEMPLATE = "CREATE TABLE data.%s AS " +
            "SELECT * FROM data.%s WITH NO DATA;";

    public static final String DROP_TRIGGER = "DROP TRIGGER IF EXISTS %1$s ON %2$s.%3$s;";

    public static final String HASH_TRIGGER_NAME = "hash_tg";

    public static final String CREATE_HASH_TRIGGER = "CREATE OR REPLACE FUNCTION data.\"%s_hash_tf\"()\n" +
            "  RETURNS trigger AS\n" +
            "$BODY$\n" +
            "  BEGIN\n" +
            "    NEW.\"SYS_HASH\" = md5(ROW(%s)||'');\n" +
            "    RETURN NEW;\n" +
            "  END;\n" +
            "$BODY$ LANGUAGE plpgsql;\n" +
            "\n" +
            "CREATE TRIGGER " + HASH_TRIGGER_NAME + "\n" +
            "  BEFORE INSERT OR UPDATE OF %s\n" +
            "  ON data.%s\n" +
            "  FOR EACH ROW\n" +
            "  EXECUTE PROCEDURE data.\"%s_hash_tf\"();";

    public static final String UPDATE_HASH = "UPDATE data.%s SET \"SYS_HASH\" = md5(ROW(%s)||'');";

    public static final String FTS_TRIGGER_NAME = "fts_vector_tg";

    public static final String CREATE_FTS_TRIGGER = "CREATE OR REPLACE FUNCTION data.\"%s_fts_vector_tf\"()\n" +
            "  RETURNS trigger AS\n" +
            "$BODY$\n" +
            "  BEGIN\n" +
            "    NEW.\"FTS\" = %s;\n" +
            "    RETURN NEW;\n" +
            "  END;\n" +
            "$BODY$ LANGUAGE plpgsql;\n" +
            "\n" +
            "CREATE TRIGGER " + FTS_TRIGGER_NAME + "\n" +
            "  BEFORE INSERT OR UPDATE OF %s\n" +
            "  ON data.%s\n" +
            "  FOR EACH ROW\n" +
            "  EXECUTE PROCEDURE data.\"%s_fts_vector_tf\"();";
    public static final String UPDATE_FTS = "UPDATE data.%s SET \"FTS\" = %s;";

    public static final String ADD_NEW_COLUMN = "ALTER TABLE data.\"%s\" ADD COLUMN \"%s\" %s;";
    public static final String ADD_NEW_COLUMN_WITH_DEFAULT = "ALTER TABLE data.\"%s\" ADD COLUMN \"%s\" %s DEFAULT %s;";
    public static final String DELETE_COLUMN = "ALTER TABLE data.\"%s\" DROP COLUMN \"%s\" CASCADE;";
    public static final String ALTER_COLUMN_WITH_USING = "ALTER TABLE data.%s ALTER COLUMN %s SET DATA TYPE %s USING %s";

    public static final String INSERT_QUERY_TEMPLATE_WITH_ID = "INSERT INTO data.%s (%s) VALUES(%s) returning \"SYS_RECORDID\";";
    public static final String INSERT_QUERY_TEMPLATE = "INSERT INTO data.%s (%s) VALUES(%s);";
    public static final String COPY_QUERY_TEMPLATE = "INSERT INTO data.%s (%s) SELECT %s FROM data.%s d ";

    public static final String DELETE_QUERY_TEMPLATE = "DELETE FROM data.%s WHERE \"SYS_RECORDID\" IN (%s);";
    //todo
    public static final String DELETE_POINT_ROWS_QUERY_TEMPLATE = "DELETE FROM data.%s WHERE \"SYS_PUBLISHTIME\" = \"SYS_CLOSETIME\";";
    public static final String DELETE_ALL_RECORDS_FROM_TABLE_QUERY_TEMPLATE = "DELETE FROM data.%s;";
    public static final String DELETE_EMPTY_RECORDS_FROM_TABLE_QUERY_TEMPLATE = "DELETE FROM data.%s WHERE %s;";

    static final String UPDATE_QUERY_TEMPLATE = "UPDATE data.%s as b SET %s WHERE b.\"SYS_RECORDID\" IN (%s);";
    static final String UPDATE_REFERENCE_QUERY_TEMPLATE = "UPDATE data.%s as b SET %s WHERE b.\"SYS_RECORDID\" = ANY(%s\\:\\:bigint[]);";

    private static final String AND_EXISTS_VERSION_REF_VALUE = "   AND v.${refFieldName} is not null\n" +
            "   AND (v.${refFieldName} -> 'value') is not null\n";

    static final String COUNT_REFERENCE_IN_REF_ROWS = SELECT_COUNT_ONLY +
            FROM_VERSION_TABLE +
            SELECT_WHERE +
            AND_EXISTS_VERSION_REF_VALUE;

    static final String WHERE_REFERENCE_IN_REF_ROWS = "\nSELECT v.\"SYS_RECORDID\"\n" +
            FROM_VERSION_TABLE +
            SELECT_WHERE +
            AND_EXISTS_VERSION_REF_VALUE +
            ORDER_VERSION_BY_SYS_RECORDID +
            SELECT_LIMIT + SELECT_OFFSET;

    static final String REFERENCE_VALUATION_UPDATE_TABLE = "b";
    static final String REFERENCE_VALUATION_SELECT_TABLE = "d";
    static final String REFERENCE_VALUATION_SELECT_SUBST = "select jsonb_build_object('value', ?)";
    static final String REFERENCE_VALUATION_SELECT_EXPRESSION =
            "select jsonb_build_object('value', d.%1$s , 'displayValue', %2$s, 'hash', d.\"SYS_HASH\")\n" +
            "  from data.%3$s d where d.%1s=%4$s\\:\\:%5$s and %6$s";
    static final String REFERENCE_VALUATION_OLD_VALUE =
            "(case when %1$s is null then null else %1$s->>'value' end)";

    public static final String IS_VERSION_NOT_EMPTY = "SELECT exists(SELECT * FROM data.%s);";
    public static final String IS_FIELD_NOT_EMPTY = "SELECT exists(SELECT * FROM data.%s WHERE %s.%s IS NOT NULL);";
    public static final String IS_FIELD_CONTAIN_EMPTY_VALUES = "SELECT exists(SELECT * FROM data.%s WHERE %s.%s IS NULL);";
    public static final String IS_RELATED_VALUE_EXIST = "SELECT exists(SELECT * FROM data.%s where %s.%s = %s)";

    public static final String SELECT_TABLE_EXISTS = "SELECT EXISTS(\n" +
            "  SELECT 1 \n" +
            "    FROM \"information_schema\".\"tables\" \n" +
            "   WHERE table_schema = :schemaName \n" +
            "     AND table_name = :tableName \n" +
            ")";

    private static final String SELECT_COLUMN_NAME = "SELECT '\"' || column_name || '\"'\n";
    private static final String FROM_INFO_SCHEMA_COLUMNS = "FROM \"information_schema\".\"columns\"\n";
    private static final String WHERE_TABLE_AND_NOT_SYS_COLUMNS = "WHERE table_name = '%s'\n" +
            "  AND column_name NOT IN (" + SYS_RECORDS_TEXT + ")";

    public static final String SELECT_FIELD_NAMES = SELECT_COLUMN_NAME +
            FROM_INFO_SCHEMA_COLUMNS +
            WHERE_TABLE_AND_NOT_SYS_COLUMNS;
    public static final String SELECT_HASH_USED_FIELD_NAMES = SELECT_COLUMN_NAME +
            "       || (case when data_type = '" + REFERENCE_FIELD_SQL_TYPE + "' then '->>''value''' else '' end)\n" +
            FROM_INFO_SCHEMA_COLUMNS +
            WHERE_TABLE_AND_NOT_SYS_COLUMNS;
    public static final String SELECT_FIELD_TYPE = "SELECT data_type\n" +
            FROM_INFO_SCHEMA_COLUMNS +
            " WHERE table_name = '%s'\n" +
            "   AND column_name = '%s'";
    public static final String SELECT_FIELD_NAMES_AND_TYPES = "SELECT column_name, data_type\n" +
            FROM_INFO_SCHEMA_COLUMNS +
            " WHERE table_schema = 'data'\n" +
            "   AND table_name = :table";

    public static final String INSERT_QUERY_FROM_DRAFT_TEMPLATE = "INSERT INTO data.%s SELECT %s FROM data.%s WHERE \"SYS_CLOSETIME\" IS NULL;";
    public static final String SELECT_COUNT_QUERY_TEMPLATE = SELECT_COUNT_ONLY + "  FROM %1$s.%2$s;";
    public static final String TRUNCATE_QUERY_TEMPLATE = "TRUNCATE TABLE data.%s;";

    public static final String CREATE_TABLE_INDEX = "CREATE INDEX %1$s ON %2$s.%3$s(%4$s);";
    public static final String DROP_TABLE_INDEX = "DROP INDEX IF EXISTS %1$s.%2$s;";
    public static final String CREATE_FTS_INDEX = "CREATE INDEX %1$s ON %2$s.%3$s USING gin (%4$s);";
    public static final String CREATE_LTREE_INDEX = "CREATE INDEX %1$s ON %2$s.%3$s USING gist (%4$s);";
    public static final String IF_TABLE_INDEX_EXISTS = "SELECT exists(SELECT *\n" +
            "              FROM\n" +
            "                pg_class t,\n" +
            "                pg_class i,\n" +
            "                pg_index ix,\n" +
            "                pg_attribute a\n" +
            "              WHERE\n" +
            "                t.oid = ix.indrelid\n" +
            "                AND i.oid = ix.indexrelid\n" +
            "                AND a.attrelid = t.oid\n" +
            "                AND a.attnum = ANY(ix.indkey)\n" +
            "                AND t.relkind = 'r'\n" +
            "                AND t.relname = '%s'\n" +
            "                AND a.attname = '%s'\n" +
            "                AND i.relname = '%s'\n" +
            "              ORDER BY\n" +
            "                t.relname,\n" +
            "                i.relname\n" +
            ");";

    public static final String DROP_TABLE = "DROP TABLE IF EXISTS %1$s.%2$s";

    //todo: get rid of infinity
    public static final String INSERT_FROM_DRAFT_TEMPLATE_WITH_CLOSE_TIME = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            " row data.%1$s%%rowtype;\n" +
            " i int;\n" +
            "BEGIN\n" +
            "    OPEN tbl_cursor FOR select * from data.%1$s order by \"SYS_RECORDID\";\n" +
            "    MOVE FORWARD %2$s FROM tbl_cursor;\n" +
            "    i\\:=0;\n" +
            "    while i<%3$s loop\n" +
            "       FETCH FROM tbl_cursor INTO row;\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "       row.\"SYS_RECORDID\"\\:=nextval('data.%7$s');\n" +
            "       insert into data.%4$s(\"SYS_RECORDID\", %8$s, \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\")\n" +
            "       values(row.\"SYS_RECORDID\", %9$s, to_timestamp('%5$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, coalesce(to_timestamp('%6$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity'));\n" +
            "       i\\:=i+1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    public static final String SELECT_ROWS_FROM_DATA = " select %s from data.%s d where %s";

    public static final String SELECT_ROWS_FROM_DATA_BY_FIELD = " select %1$s from %2$s.%3$s where %4$s = %5$s ";

    public static final String SELECT_ROWS_FROM_DATA_BY_FIELD_LIST = " select %1$s from %2$s.%3$s where %4$s = ANY(%5$s\\:\\:bigint[]) ";

    public static final String SELECT_RELATION_ROW_FROM_DATA = " select %s from data.%s where %s=? limit 1;\n";

    public static final String COUNT_OLD_VAL_FROM_VERSION_WITH_CLOSE_TIME = SELECT_COUNT_ONLY +
            "  FROM data.%1$s v\n" +
            " WHERE NOT (\n" +
            "       v.\"SYS_CLOSETIME\" IS NULL AND (to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\" OR\n" +
            "                                        to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_PUBLISHTIME\" AND\n" +
            "                                        ('%4$s' = 'null' OR to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\"))\n" +
            "       OR\n" +
            "       v.\"SYS_CLOSETIME\" IS NOT NULL AND (v.\"SYS_PUBLISHTIME\" = to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR\n" +
            "                                            v.\"SYS_CLOSETIME\" = to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR\n" +
            "                                            '%4$s' = 'null' AND to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_CLOSETIME\" OR\n" +
            "                                            (to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone) OVERLAPS\n" +
            "                                            (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\"))\n" +
            "       );";

    //todo: get rid of infinity
    public static final String INSERT_OLD_VAL_FROM_VERSION_WITH_CLOSE_DATE = "DO $$" +
            "DECLARE tbl_cursor refcursor;\n" +
            " row record;\n" +
            " i int;\n" +
            "BEGIN\n" +
            "    OPEN tbl_cursor FOR SELECT \"SYS_RECORDID\", %7$s, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" FROM data.%2$s v\n" +
            "   WHERE NOT (\n" +
            "        v.\"SYS_CLOSETIME\" IS NULL AND (to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\" OR\n" +
            "                                           to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_PUBLISHTIME\" AND\n" +
            "                                           ('%10$s' = 'null' OR to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone >= v.\"SYS_PUBLISHTIME\"))\n" +
            "        OR\n" +
            "        v.\"SYS_CLOSETIME\" IS NOT NULL AND (v.\"SYS_PUBLISHTIME\" = to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR\n" +
            "                                               v.\"SYS_CLOSETIME\" = to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone OR\n" +
            "                                               '%10$s' = 'null' AND to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone < v.\"SYS_CLOSETIME\" OR\n" +
            "                                               (to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone) OVERLAPS\n" +
            "                                               (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\"))\n" +
            "      ) order by v.\"SYS_RECORDID\";\n" +
            "    MOVE FORWARD %4$s FROM tbl_cursor;\t\n" +
            "    i\\:=0;\t\n" +
            "    while i<%5$s loop\n" +
            "       FETCH FROM tbl_cursor INTO row;\t\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "       row.\"SYS_RECORDID\"\\:=nextval('data.%6$s');\n" +
            "       INSERT INTO data.%1$s(\"SYS_RECORDID\", %7$s, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\") VALUES (row.\"SYS_RECORDID\", %8$s, row.\"FTS\", row.\"SYS_HASH\", row.\"SYS_PUBLISHTIME\", row.\"SYS_CLOSETIME\");\n" +
            "       i\\:=i+1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    public static final String COPY_DATA = " DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            " row data.%1$s%%rowtype;\n" +
            " i int;\n" +
            "\n" +
            "BEGIN\n" +
            "  OPEN tbl_cursor FOR\n" +
            "SELECT * from data.%2$s\n" +
            " ORDER BY \"SYS_RECORDID\";\n" +
            "\n" +
            "  MOVE FORWARD %3$s FROM tbl_cursor;\t\n" +
            "  i \\:= 0;\n" +
            "  while i < %4$s loop\n" +
            "    FETCH FROM tbl_cursor INTO row;\t\n" +
            "    EXIT WHEN NOT FOUND;\n" +
            "    row.\"SYS_RECORDID\" \\:= nextval('data.%5$s');\n" +
            "    INSERT INTO data.%1$s VALUES( row.*);\n" +
            "    i \\:= i + 1;\n" +
            "  end loop;\n" +
            "\n" +
            "  CLOSE tbl_cursor;\n" +
            "END$$;";

    private static final String WHERE_EXISTS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = " WHERE exists(\n" +
            "       SELECT 1 " + FROM_VERSION_TABLE +
            "        WHERE v.\"SYS_HASH\" = d.\"SYS_HASH\"\n" +
            SUBQUERY_INDENT +
            AND_IS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME.replace("\n", "\n" + SUBQUERY_INDENT) +
            "       )\n";

    static final String COUNT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = SELECT_COUNT_ONLY +
            FROM_DRAFT_TABLE +
            WHERE_EXISTS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME;

    static final String INSERT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            " row record;\n" +
            " i int;\n" +
            "\n" +
            "BEGIN\n" +
            "  OPEN tbl_cursor FOR\n" +
            "SELECT d.\"SYS_RECORDID\", ${dColumns}, d.\"FTS\", d.\"SYS_HASH\" " + FROM_DRAFT_TABLE +
            WHERE_EXISTS_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME +
            ORDER_DRAFT_BY_SYS_RECORDID +
            ";\n" +
            "  MOVE FORWARD ${offset} FROM tbl_cursor;\t\n" +
            "  i \\:= 0;\n" +
            "  while i < ${transactionSize} loop\n" +
            "    FETCH FROM tbl_cursor INTO row;\n" +
            "    EXIT WHEN NOT FOUND;\n" +
            "    row.\"SYS_RECORDID\" \\:= nextval('${newTableSeqName}');\n" +
            "    INSERT INTO ${tableToInsert} (\"SYS_RECORDID\", ${columns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\")\n" +
            "    SELECT \"SYS_RECORDID\", ${columns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" " +
            "      FROM data.merged_actual_rows('${columns}'\\:\\:text, row.\"SYS_HASH\", '${versionTable}'\\:\\:text,\n" +
            "               to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone,\n" +
            "               coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity'),\n" +
            "               row.\"SYS_RECORDID\")\n" +
            "           t(\"SYS_RECORDID\" bigint, ${columnsWithType}, \"FTS\" tsvector, \"SYS_HASH\" character(32),\n" +
            "             \"SYS_PUBLISHTIME\" timestamp without time zone, \"SYS_CLOSETIME\" timestamp without time zone);\n" +
            "    i \\:= i + 1;\n" +
            "  end loop;\n" +
            "\n" +
            "  CLOSE tbl_cursor;\n" +
            "END$$;";

    //todo: get rid of infinity
    public static final String COUNT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME = SELECT_COUNT_ONLY +
            "    FROM ${draftTable} d \n" +
            "   WHERE NOT exists(SELECT 1 \n" +
            "                      FROM ${versionTable} v \n" +
            "                     WHERE v.\"SYS_HASH\" = d.\"SYS_HASH\" \n" +
            "                       AND ( (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'),\n" +
            "                              coalesce(to_timestamp('${closeTime}',   'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity'))\n" +
            "                             OVERLAPS " +
            "                             (coalesce(v.\"SYS_PUBLISHTIME\", '-infinity'), coalesce(v.\"SYS_CLOSETIME\", 'infinity'))\n" +
            "                            OR (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity') =\n" +
            "                                coalesce(v.\"SYS_CLOSETIME\", 'infinity')) )\n" +
            "         )";


    //todo: get rid of infinity
    public static final String
            INSERT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            " row record;\n" +
            " i int;\n" +
            "BEGIN\n" +
            "    OPEN tbl_cursor FOR SELECT \"SYS_RECORDID\", ${fields}, \"FTS\", \"SYS_HASH\"\n" +
            "     FROM ${draftTable} d\n" +
            "     WHERE NOT exists(SELECT 1\n" +
            "                      FROM ${versionTable} v\n" +
            "                      WHERE v.\"SYS_HASH\" = d.\"SYS_HASH\"\n" +
            "                           AND (( coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'), coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity') )" +
            "                                   OVERLAPS " +
            "                               (coalesce( v.\"SYS_PUBLISHTIME\", '-infinity') ,  coalesce(v.\"SYS_CLOSETIME\", 'infinity')) " +
            "                           OR (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity') = (coalesce(v.\"SYS_CLOSETIME\", 'infinity'))))" +
            "    ) order by d.\"SYS_RECORDID\"; "+
            "    MOVE FORWARD ${offset} FROM tbl_cursor;\t\n" +
            "    i\\:=0;\t\n" +
            "    while i<${transactionSize} loop\n" +
            "       FETCH FROM tbl_cursor INTO row;\t\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "       row.\"SYS_RECORDID\" \\:= nextval('${sequenceName}');\n" +
            "       INSERT INTO ${tableToInsert} (\"SYS_RECORDID\", ${fields}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\")\n" +
            "       VALUES (row.\"SYS_RECORDID\", ${rowFields}, row.\"FTS\", row.\"SYS_HASH\", to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, case when '${closeTime}' = 'null' then null\\:\\:timestamp without time zone else to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone end);\n" +
            "       i\\:=i+1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    //todo: get rid of infinity
    public static final String COUNT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME = "SELECT count(*)\n" +
            "FROM ${versionTable} v\n" +
            " WHERE " +
            "    ( coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'), coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity') ) " +
            "           OVERLAPS\n" +
            "    ( v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\")\n" +
            "      AND NOT exists(SELECT 1\n" +
            "                     FROM ${draftTable} d\n" +
            "                     WHERE d.\"SYS_HASH\" = v.\"SYS_HASH\");";

    //todo: get rid of infinity
    public static final String INSERT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            " row record;\n" +
            " i int;\n" +
            "BEGIN\n" +
            "    OPEN tbl_cursor FOR SELECT \"SYS_RECORDID\", ${columns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" FROM ${versionTable} v\n" +
            " WHERE " +
            "    ( coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '-infinity'), coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, 'infinity') ) " +
            "           OVERLAPS\n" +
            "    ( v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\")\n" +
            "      AND NOT exists(SELECT 1\n" +
            "                     FROM ${draftTable} d\n" +
            "                     WHERE d.\"SYS_HASH\" = v.\"SYS_HASH\") " +
            " order by v.\"SYS_RECORDID\";" +
            "    MOVE FORWARD ${offset} FROM tbl_cursor;\t\n" +
            "    i\\:=0;\t\n" +
            "    while i<${transactionSize} loop\n" +
            "       FETCH FROM tbl_cursor INTO row;\t\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "       INSERT INTO ${tableToInsert}(\"SYS_RECORDID\", ${columns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\")\n" +
            "           SELECT \"SYS_RECORDID\", ${columns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" FROM data.closed_now_records('\"SYS_RECORDID\", ${columns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\"'\\:\\:text, row.\"SYS_RECORDID\", '${versionTable}'\\:\\:text, to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp without time zone, '${sequenceName}'\\:\\:text) t(\"SYS_RECORDID\" bigint, ${columnsWithType}, \"FTS\" tsvector, \"SYS_HASH\" character(32), \"SYS_PUBLISHTIME\" timestamp without time zone, \"SYS_CLOSETIME\" timestamp without time zone);\n" +
            "       i\\:=i+1;\n" +
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

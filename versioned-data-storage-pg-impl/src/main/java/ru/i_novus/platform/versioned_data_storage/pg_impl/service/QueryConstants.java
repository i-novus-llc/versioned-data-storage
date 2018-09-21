package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author lgalimova
 * @since 22.03.2018
 */
public class QueryConstants {

    Pattern dataRegexp = Pattern.compile("([0-9]{2})\\.([0-9]{2})\\.([0-9]{4})");

    protected static final int TRANSACTION_SIZE = 1000;

    public static final String DATE_FORMAT_FOR_INSERT_ROW = "yyyy-MM-dd";
    public static final String DATE_FORMAT_FOR_USING_CONVERTING = "DD.MM.YYYY";
    public static final String TIMESTAMP_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String SYS_PUBLISHTIME = "SYS_PUBLISHTIME";
    public static final String SYS_CLOSETIME = "SYS_CLOSETIME";
    public static final String SYS_HASH = "SYS_HASH";
    public static final String DATE_BEGIN = "DATEBEG";
    public static final String DATE_END = "DATEEND";
    public static final String DATA_PRIMARY_COLUMN = "SYS_RECORDID";
    public static final String HAS_CHILD_BRANCH = "SYS_HAS_CHILD_BRANCH";
    public static final String FULL_TEXT_SEARCH = "FTS";

    public static final List<String> SYS_RECORDS = Arrays.asList(DATA_PRIMARY_COLUMN, SYS_PUBLISHTIME, SYS_CLOSETIME, SYS_HASH, "SYS_PATH", FULL_TEXT_SEARCH);

    public static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE data.%s (\"SYS_RECORDID\" bigserial NOT NULL, " +
            "%s, " +
            "\"FTS\" tsvector, " +
            "\"SYS_HASH\" char(32), " +
            "\"SYS_PUBLISHTIME\" timestamp with time zone, " +
            "\"SYS_CLOSETIME\" timestamp with time zone, "
            + "CONSTRAINT \"%s_pkey\" PRIMARY KEY (\"SYS_RECORDID\"));";

    public static final String CREATE_EMPTY_DRAFT_TABLE_TEMPLATE = "CREATE TABLE data.%s (\"SYS_RECORDID\" bigserial NOT NULL, " +
            "\"FTS\" tsvector, " +
            "\"SYS_HASH\" char(32) UNIQUE, " +
            "CONSTRAINT \"%s_pkey\" PRIMARY KEY (\"SYS_RECORDID\"));";

    public static final String CREATE_DRAFT_TABLE_TEMPLATE = "CREATE TABLE data.%s (\"SYS_RECORDID\" bigserial NOT NULL, " +
            "%s, " +
            "\"FTS\" tsvector, " +
            "\"SYS_HASH\" char(32) UNIQUE, " +
            "CONSTRAINT \"%s_pkey\" PRIMARY KEY (\"SYS_RECORDID\"));";

    public static final String COPY_TABLE_TEMPLATE = "create table data.%s as select * from data.%s with no data;";

    public static final String DROP_HASH_TRIGGER = "DROP TRIGGER IF EXISTS hash_tg ON data.%s;";

    public static final String CREATE_HASH_TRIGGER = "CREATE OR REPLACE FUNCTION data.\"%s_hash_tf\"()\n" +
            "  RETURNS trigger AS\n" +
            "$BODY$\n" +
            "  BEGIN\n" +
            "\tNEW.\"SYS_HASH\" = md5(ROW(%s)||'');\t\t\n" +
            "\tRETURN NEW;\n" +
            "  END;\n" +
            "  $BODY$\n" +
            "  LANGUAGE plpgsql;\n" +
            "\n" +
            "  CREATE TRIGGER hash_tg\n" +
            "  BEFORE INSERT OR UPDATE OF %s\n" +
            "  ON data.%s\n" +
            "  FOR EACH ROW\n" +
            "  EXECUTE PROCEDURE data.\"%s_hash_tf\"();";

    public static final String UPDATE_HASH = "UPDATE data.%s  SET \"SYS_HASH\" =  md5(ROW(%s)||'');";

    public static final String DROP_FTS_TRIGGER = "DROP TRIGGER IF EXISTS fts_vector_tg ON data.%s;";

    public static final String CREATE_FTS_TRIGGER = "CREATE OR REPLACE FUNCTION data.\"%s_fts_vector_tf\"()\n" +
            "  RETURNS trigger AS\n" +
            "$BODY$\n" +
            "  BEGIN\n" +
            "\tNEW.\"FTS\" = %s;\n" +
            "\tRETURN NEW;\n" +
            "  END;\n" +
            "  $BODY$\n" +
            "  LANGUAGE plpgsql;\n" +
            "\n" +
            "  CREATE TRIGGER fts_vector_tg\n" +
            "  BEFORE INSERT OR UPDATE OF %s\n" +
            "  ON data.%s\n" +
            "  FOR EACH ROW\n" +
            "  EXECUTE PROCEDURE data.\"%s_fts_vector_tf\"();";
    public static final String UPDATE_FTS = "UPDATE data.%s  SET \"FTS\" =  %s;";


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
    public static final String UPDATE_QUERY_TEMPLATE = "UPDATE data.%s SET %s WHERE \"SYS_RECORDID\" IN (%s);";
    public static final String IS_VERSION_NOT_EMPTY = "SELECT exists(SELECT * FROM data.%s);";
    public static final String IS_FIELD_NOT_EMPTY = "SELECT exists(SELECT * FROM data.%s WHERE %s.%s IS NOT NULL);";
    public static final String IS_FIELD_CONTAIN_EMPTY_VALUES = "SELECT exists(SELECT * FROM data.%s WHERE %s.%s IS NULL);";
    public static final String IS_RELATED_VALUE_EXIST = "SELECT exists(SELECT * FROM data.%s where %s.%s = %s)";
    public static final String SELECT_FIELD_NAMES = "SELECT column_name FROM \"information_schema\".\"columns\" WHERE table_name = '%s' AND column_name NOT IN ('SYS_RECORDID', 'FTS', 'SYS_HASH', 'SYS_PUBLISHTIME', 'SYS_CLOSETIME')";
    public static final String SELECT_FIELD_TYPE = "SELECT data_type FROM \"information_schema\".\"columns\" WHERE table_name = '%s' AND column_name='%s'";

    public static final String INSERT_QUERY_FROM_DRAFT_TEMPLATE = "INSERT INTO data.%s SELECT %s FROM data.%s WHERE \"SYS_CLOSETIME\" IS NULL;";
    public static final String SELECT_COUNT_QUERY_TEMPLATE = "SELECT count(*) FROM data.%s;";
    public static final String TRUNCATE_QUERY_TEMPLATE = "TRUNCATE TABLE data.%s;";
    public static final String CREATE_TABLE_HASH_INDEX = "CREATE INDEX %s ON data.%s(\"SYS_HASH\");";
    public static final String CREATE_TABLE_INDEX = "CREATE INDEX %s ON data.%s(%s);";
    public static final String DROP_TABLE_INDEX = "DROP INDEX IF EXISTS data.%s;";
    public static final String CREATE_FTS_INDEX = "CREATE INDEX %s ON data.%s USING gin (%s);";
    public static final String CREATE_LTREE_INDEX = "CREATE INDEX %s ON data.%s USING gist (%s);";
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

    public static final String DROP_TABLE = "DROP TABLE IF EXISTS data.%s";

    //todo: get rid of infinity
    public static final String INSERT_FROM_DRAFT_TEMPLATE_WITH_CLOSE_TIME = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            " row data.%1$s%%rowtype;\n" +
            " i int;\n" +
            "BEGIN\n" +
            "    OPEN tbl_cursor FOR select * from data.%1$s;\n" +
            "    MOVE FORWARD %2$s FROM tbl_cursor;\n" +
            "    i\\:=0;\n" +
            "    while i<%3$s loop\n" +
            "       FETCH FROM tbl_cursor INTO row;\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "       row.\"SYS_RECORDID\"\\:=nextval('data.%7$s');\n" +
            "       insert into data.%4$s(\"SYS_RECORDID\", %8$s, \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\") values(row.\"SYS_RECORDID\", %9$s, to_timestamp('%5$s', 'YYYY-MM-DD HH24:MI:SS'), coalesce(to_timestamp('%6$s', 'YYYY-MM-DD HH24:MI:SS'), 'infinity'));\n" +
            "       i\\:=i+1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    public static final String SELECT_ROWS_FROM_DATA = " select %s from data.%s d where %s";

    public static final String SELECT_ROWS_FROM_DATA_BY_FIELD = " select %s from data.%s where %s=? ";

    public static final String SELECT_RELATION_ROW_FROM_DATA = " select %s from data.%s where %s=? limit 1;\n";

    public static final String COUNT_OLD_VAL_FROM_VERSION_WITH_CLOSE_TIME = "SELECT count(*)\n" +
            "FROM data.%1$s v\n" +
            "WHERE NOT (\n" +
            "        v.\"SYS_CLOSETIME\" IS NULL AND (to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS') >= v.\"SYS_PUBLISHTIME\" OR\n" +
            "                                           to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS') < v.\"SYS_PUBLISHTIME\" AND\n" +
            "                                           ('%4$s' = 'null' OR to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS') >= v.\"SYS_PUBLISHTIME\"))\n" +
            "        OR\n" +
            "        v.\"SYS_CLOSETIME\" IS NOT NULL AND (v.\"SYS_PUBLISHTIME\" = to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS') OR\n" +
            "                                               v.\"SYS_CLOSETIME\" = to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS') OR\n" +
            "                                               '%4$s' = 'null' AND to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS') < v.\"SYS_CLOSETIME\" OR\n" +
            "                                               (to_timestamp('%3$s', 'YYYY-MM-DD HH24:MI:SS'), to_timestamp('%4$s', 'YYYY-MM-DD HH24:MI:SS')) OVERLAPS\n" +
            "                                               (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\"))\n" +
            "      );";

    //todo: get rid of infinity
    public static final String INSERT_OLD_VAL_FROM_VERSION_WITH_CLOSE_DATE = "DO $$" +
            "DECLARE tbl_cursor refcursor;\n" +
            " row record;\n" +
            " i int;\n" +
            "BEGIN\n" +
            "    OPEN tbl_cursor FOR SELECT \"SYS_RECORDID\", %7$s, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" FROM data.%2$s v\n" +
            "   WHERE NOT (\n" +
            "        v.\"SYS_CLOSETIME\" IS NULL AND (to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS') >= v.\"SYS_PUBLISHTIME\" OR\n" +
            "                                           to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS') < v.\"SYS_PUBLISHTIME\" AND\n" +
            "                                           ('%10$s' = 'null' OR to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS') >= v.\"SYS_PUBLISHTIME\"))\n" +
            "        OR\n" +
            "        v.\"SYS_CLOSETIME\" IS NOT NULL AND (v.\"SYS_PUBLISHTIME\" = to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS') OR\n" +
            "                                               v.\"SYS_CLOSETIME\" = to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS') OR\n" +
            "                                               '%10$s' = 'null' AND to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS') < v.\"SYS_CLOSETIME\" OR\n" +
            "                                               (to_timestamp('%9$s', 'YYYY-MM-DD HH24:MI:SS'), to_timestamp('%10$s', 'YYYY-MM-DD HH24:MI:SS')) OVERLAPS\n" +
            "                                               (v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\"))\n" +
            "      );\n" +
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
            "BEGIN\n" +
            "    OPEN tbl_cursor FOR select * from data.%2$s ;\n" +
            "    MOVE FORWARD %3$s FROM tbl_cursor;\t\n" +
            "    i\\:=0;\t\n" +
            "    while i<%4$s loop\n" +
            "       FETCH FROM tbl_cursor INTO row;\t\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "       row.\"SYS_RECORDID\"\\:=nextval('data.%5$s');" +
            "       insert into data.%1$s values( row.*);   \n" +
            "       i\\:=i+1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    //todo: get rid of infinity
    public static final String COUNT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = "SELECT count(*)  FROM ${draftTable} d\n" +
            "     WHERE exists(SELECT 1\n" +
            "                      FROM ${versionTable} v\n" +
            "                      WHERE v.\"SYS_HASH\" = d.\"SYS_HASH\" \n" +
            "                           AND (( coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), '-infinity'), coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS'), 'infinity') ) " +
            "                                   OVERLAPS " +
            "                           (coalesce(v.\"SYS_PUBLISHTIME\", '-infinity') ,  coalesce(v.\"SYS_CLOSETIME\", 'infinity')) " +
            "                           OR (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), '-infinity') = (coalesce(v.\"SYS_CLOSETIME\", 'infinity'))))" +
            "    )";

    //todo: get rid of infinity
    public static final String INSERT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME = "DO $$\n" +
            "DECLARE tbl_cursor refcursor;\n" +
            " row record;\n" +
            " i int;\n" +
            "BEGIN\n" +
            "    OPEN tbl_cursor FOR select d.\"SYS_RECORDID\", ${dColumns}, d.\"FTS\", d.\"SYS_HASH\" FROM ${draftTable} d\n" +
            "     WHERE exists(SELECT 1\n" +
            "                      FROM ${versionTable} v\n" +
            "                      WHERE v.\"SYS_HASH\" = d.\"SYS_HASH\" \n" +
            "                           AND (( coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), '-infinity'), coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS'), 'infinity') ) \n" +
            "                                 OVERLAPS \n" +
            "                               (coalesce( v.\"SYS_PUBLISHTIME\", '-infinity') ,  coalesce(v.\"SYS_CLOSETIME\", 'infinity'))" +
            "                           OR (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), '-infinity') = (coalesce(v.\"SYS_CLOSETIME\", 'infinity'))))" +
            "    );"+
            "    MOVE FORWARD ${offset} FROM tbl_cursor;\t\n" +
            "    i\\:=0;\t\n" +
            "    while i<${transactionSize} loop\n" +
            "       FETCH FROM tbl_cursor INTO row;\t\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "       row.\"SYS_RECORDID\" \\:= nextval('${newTableSeqName}');\n" +
            "       insert into ${tableToInsert} (\"SYS_RECORDID\", ${columns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\") " +
            "           SELECT \"SYS_RECORDID\", ${columns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" " +
            "           FROM data.merged_actual_rows('${columns}'\\:\\:text, row.\"SYS_HASH\", '${versionTable}'\\:\\:text, to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS'), 'infinity'),  row.\"SYS_RECORDID\") " +
            "           t(\"SYS_RECORDID\" bigint, ${columnsWithType}, \"FTS\" tsvector, \"SYS_HASH\" character(32) , \"SYS_PUBLISHTIME\" timestamp with time zone, \"SYS_CLOSETIME\" timestamp with time zone);" +
            "       i\\:=i+1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    //todo: get rid of infinity
    public static final String COUNT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME = "SELECT count(*)  FROM ${draftTable} d\n" +
            "     WHERE NOT exists(SELECT 1\n" +
            "                      FROM ${versionTable} v\n" +
            "                      WHERE v.\"SYS_HASH\" = d.\"SYS_HASH\" \n" +
            "                           AND (( coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), '-infinity'), coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS'), 'infinity') )" +
            "                                   OVERLAPS " +
            "                               (coalesce( v.\"SYS_PUBLISHTIME\", '-infinity') ,  coalesce(v.\"SYS_CLOSETIME\", 'infinity')) " +
            "                           OR (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), '-infinity') = (coalesce(v.\"SYS_CLOSETIME\", 'infinity'))))" +
            "    )";


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
            "                      WHERE v.\"SYS_HASH\" = d.\"SYS_HASH\" \n" +
            "                           AND (( coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), '-infinity'), coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS'), 'infinity') )" +
            "                                   OVERLAPS " +
            "                               (coalesce( v.\"SYS_PUBLISHTIME\", '-infinity') ,  coalesce(v.\"SYS_CLOSETIME\", 'infinity')) " +
            "                           OR (coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), '-infinity') = (coalesce(v.\"SYS_CLOSETIME\", 'infinity'))))" +
            "    ); "+
            "    MOVE FORWARD ${offset} FROM tbl_cursor;\t\n" +
            "    i\\:=0;\t\n" +
            "    while i<${transactionSize} loop\n" +
            "       FETCH FROM tbl_cursor INTO row;\t\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "       row.\"SYS_RECORDID\" \\:= nextval('${sequenceName}');\n" +
            "       INSERT INTO ${tableToInsert} (\"SYS_RECORDID\", ${fields}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\") VALUES (row.\"SYS_RECORDID\", ${rowFields}, row.\"FTS\", row.\"SYS_HASH\", to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), case when '${closeTime}' = 'null' then null\\:\\:timestamp with time zone else to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp with time zone end);\n" +
            "       i\\:=i+1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    //todo: get rid of infinity
    public static final String COUNT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME = "SELECT count(*)\n" +
            "FROM ${versionTable} v\n" +
            " WHERE " +
            "    ( coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), '-infinity'), coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS'), 'infinity') ) " +
            "           OVERLAPS\n" +
            "    ( v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\") \n" +
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
            "    ( coalesce(to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS'), '-infinity'), coalesce(to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS'), 'infinity') ) " +
            "           OVERLAPS\n" +
            "    ( v.\"SYS_PUBLISHTIME\", v.\"SYS_CLOSETIME\") \n" +
            "      AND NOT exists(SELECT 1\n" +
            "                     FROM ${draftTable} d\n" +
            "                     WHERE d.\"SYS_HASH\" = v.\"SYS_HASH\");" +
            "    MOVE FORWARD ${offset} FROM tbl_cursor;\t\n" +
            "    i\\:=0;\t\n" +
            "    while i<${transactionSize} loop\n" +
            "       FETCH FROM tbl_cursor INTO row;\t\n" +
            "       EXIT WHEN NOT FOUND;\n" +
            "       INSERT INTO ${tableToInsert}(\"SYS_RECORDID\", ${columns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\")\n" +
            "           SELECT \"SYS_RECORDID\", ${columns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\" FROM data.closed_now_records('\"SYS_RECORDID\", ${columns}, \"FTS\", \"SYS_HASH\", \"SYS_PUBLISHTIME\", \"SYS_CLOSETIME\"'\\:\\:text, row.\"SYS_RECORDID\", '${versionTable}'\\:\\:text, to_timestamp('${publishTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp with time zone, to_timestamp('${closeTime}', 'YYYY-MM-DD HH24:MI:SS')\\:\\:timestamp with time zone, '${sequenceName}'\\:\\:text) t(\"SYS_RECORDID\" bigint, ${columnsWithType}, \"FTS\" tsvector, \"SYS_HASH\" character(32), \"SYS_PUBLISHTIME\" timestamp with time zone, \"SYS_CLOSETIME\" timestamp with time zone);\n" +
            "       i\\:=i+1;\n" +
            "    end loop;\n" +
            "    CLOSE tbl_cursor;\n" +
            "END$$;";

    public static final String INSERT_FROM_SELECT_ACTUAL_DATA = "INSERT INTO data.%1$s(%2$s) SELECT %2$s FROM data.%3$s WHERE \"SYS_CLOSETIME\" IS NULL;";
}

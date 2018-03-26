package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import cz.atria.common.lang.Util;
import ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil;
import ru.i_novus.platform.datastorage.temporal.model.*;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static ru.i_novus.platform.versioned_data_storage.pg_impl.service.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.*;

public class DataDao {

    private EntityManager entityManager;

    public DataDao(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public BigInteger countData(String tableName) {
        return (BigInteger) entityManager.createNativeQuery(String.format(SELECT_COUNT_QUERY_TEMPLATE, addEscapeCharacters(tableName))).getSingleResult();
    }

    public void createDraftTable(String tableName, List<Field> fields) {
        if (Util.isEmpty(fields)) {
            entityManager.createNativeQuery(String.format(CREATE_EMPTY_DRAFT_TABLE_TEMPLATE, addEscapeCharacters(tableName), tableName)).executeUpdate();
        } else {
            String fieldsString = fields.stream().map(f -> addEscapeCharacters(f.getName()) + " " + f.getType()).collect(Collectors.joining(", "));
            entityManager.createNativeQuery(String.format(CREATE_DRAFT_TABLE_TEMPLATE, addEscapeCharacters(tableName), fieldsString, tableName)).executeUpdate();
        }
    }

    public void createVersionTable(String tableName, List<Field> fields) {
        String fieldsString = fields.stream().map(f -> addEscapeCharacters(f.getName()) + " " + f.getType()).collect(Collectors.joining(", "));
        entityManager.createNativeQuery(String.format(CREATE_TABLE_TEMPLATE, addEscapeCharacters(tableName),
                fieldsString, tableName)).executeUpdate();
    }

    public void addColumnToTable(String tableName, Field field) {
        entityManager.createNativeQuery(String.format(ADD_NEW_COLUMN, tableName, field.getName(), field.getType())).executeUpdate();
    }

    public void deleteColumnFromTable(String tableName, String field) {
        entityManager.createNativeQuery(String.format(DELETE_COLUMN, tableName, field)).executeUpdate();
    }

    public void insertData(String tableName, String keys, String values, List<FieldValue> data) {
        Query query = entityManager.createNativeQuery(String.format(INSERT_QUERY_TEMPLATE, addEscapeCharacters(tableName), keys, values));
        int i = 1;
        for (Object fieldValue : data) {
            query.setParameter(i++, ((FieldValue) fieldValue).getValue());
        }
        query.executeUpdate();
    }

    public void updateData(String tableName, String systemId, String keys, List<FieldValue> data, Map<String, String> types) {
        Query query = entityManager.createNativeQuery(String.format(UPDATE_QUERY_TEMPLATE, addEscapeCharacters(tableName), keys, "?"));
        int i = 1;
        for (FieldValue fieldValue : data) {
            query.setParameter(i++, fieldValue.getValue());
        }
        query.setParameter(i, systemId);
        query.executeUpdate();
    }

    public void deleteData(String tableName) {
        Query query = entityManager.createNativeQuery(String.format(DELETE_ALL_RECORDS_FROM_TABLE_QUERY_TEMPLATE, addEscapeCharacters(tableName)));
        query.executeUpdate();
    }

    public void deleteData(String tableName, List<String> systemIds) {
        String ids = systemIds.stream().map(id -> "?").collect(Collectors.joining(","));
        Query query = entityManager.createNativeQuery(String.format(DELETE_QUERY_TEMPLATE, addEscapeCharacters(tableName), ids));
        int i = 1;
        for (String systemId : systemIds) {
            query.setParameter(i++, systemId);
        }
        query.executeUpdate();
    }

    public void createTrigger(String tableName) {
        createTrigger(tableName, getFieldNames(tableName));
    }

    public void createTrigger(String tableName, List<String> fields) {
        String escapedTableName = addEscapeCharacters(tableName);
        entityManager.createNativeQuery(String.format(CREATE_HASH_TRIGGER,
                tableName,
                fields.stream().map(field -> "NEW." + field).collect(Collectors.joining(", ")),
                fields.stream().collect(Collectors.joining(", ")),
                escapedTableName,
                tableName)).executeUpdate();
        entityManager.createNativeQuery(String.format(CREATE_FTS_TRIGGER,
                tableName,
                fields.stream().map(field -> "coalesce( to_tsvector('ru', NEW." + field + "\\:\\:text),'')")
                        .collect(Collectors.joining(" || ' ' || ")),
                fields.stream().collect(Collectors.joining(", ")),
                escapedTableName,
                tableName)).executeUpdate();
    }

    public void dropTrigger(String tableName) {
        String escapedTableName = addEscapeCharacters(tableName);
        entityManager.createNativeQuery(String.format(DROP_HASH_TRIGGER, escapedTableName)).executeUpdate();
        entityManager.createNativeQuery(String.format(DROP_FTS_TRIGGER, escapedTableName)).executeUpdate();
    }

    public void createIndex(String tableName, String field) {
        entityManager.createNativeQuery(String.format(CREATE_TABLE_INDEX, addEscapeCharacters(tableName + "_" + field.toLowerCase() + "_idx"),
                addEscapeCharacters(tableName), addEscapeCharacters(field))).executeUpdate();
    }

    public void createFullTextSearchIndex(String tableName) {
        entityManager.createNativeQuery(String.format(CREATE_FTS_INDEX, addEscapeCharacters(tableName + "_fts_idx"),
                addEscapeCharacters(tableName),
                addEscapeCharacters(FULL_TEXT_SEARCH))).executeUpdate();
    }

    public void createHashIndex(String tableName) {
        entityManager.createNativeQuery(String.format(CREATE_TABLE_HASH_INDEX, addEscapeCharacters(tableName + "_sys_hash_ix"),
                addEscapeCharacters(tableName))).executeUpdate();
    }

    public List<String> getFieldNames(String tableName) {
        List<String> results = entityManager.createNativeQuery(String.format(SELECT_FIELD_NAMES, tableName)).getResultList();
        return results.stream().map(QueryUtil::addEscapeCharacters).collect(Collectors.toList());
    }

    public List getRowsByField(String tableName, String field, Object uniqueValue, boolean existDateColumns, Date begin, Date end, String id) {
        String query = SELECT_ROWS_FROM_DATA_BY_FIELD;
        String rows = addEscapeCharacters(field);
        if (existDateColumns) {
            SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy");
            rows += "," + addEscapeCharacters(DATE_BEGIN) + "," + addEscapeCharacters(DATE_END);
            query += "and (coalesce(\"DATEBEG\",'-infinity'\\:\\:timestamp),coalesce(\"DATEEND\",'infinity'\\:\\:timestamp)) overlaps ";
            if (begin != null) {
                query += "((to_date('" + sdf.format(begin) + "','dd.MM.yyyy') - integer '1'),";
            } else {
                query += "('-infinity'\\:\\:timestamp,";
            }
            if (end != null) {
                query += "(to_date('" + sdf.format(end) + "','dd.MM.yyyy') + integer '1'))";
            } else {
                query += "'infinity'\\:\\:timestamp)";
            }
        }
        if (id != null) {
            query += " and " + addEscapeCharacters("SYS_RECORDID") + " != " + id;
        }
        Query nativeQuery = entityManager.createNativeQuery(String.format(query, rows, addEscapeCharacters(tableName), addEscapeCharacters(field)));
        nativeQuery.setParameter(1, uniqueValue);
        return nativeQuery.getResultList();
    }
}
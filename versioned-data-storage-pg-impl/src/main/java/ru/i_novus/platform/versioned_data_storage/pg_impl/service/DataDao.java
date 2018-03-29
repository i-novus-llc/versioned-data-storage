package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import cz.atria.common.lang.Util;
import net.n2oapp.criteria.api.Sorting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.FieldSearchCriteria;
import ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil;
import ru.i_novus.platform.datastorage.temporal.model.*;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ru.i_novus.platform.versioned_data_storage.pg_impl.service.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.*;

public class DataDao {

    private static final Logger logger = LoggerFactory.getLogger(DataDao.class);
    private Pattern dataRegexp = Pattern.compile("([0-9]{2})\\.([0-9]{2})\\.([0-9]{4})");
    private EntityManager entityManager;

    public DataDao(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public List<RowValue> getData(DataCriteria criteria) {
        List<String> fieldNames = criteria.getFields().stream().map(Field::getName).collect(Collectors.toList());
        String queryStr = "SELECT " + generateSqlQuery("d", fieldNames) +
                " FROM data." + addEscapeCharacters(criteria.getTableName()) + " d ";

        queryStr += getDataWhereClause(criteria.getBdate(), criteria.getEdate(), criteria.getCommonFilter(), criteria.getFieldFilter());
        String orderBy = getDictionaryDataOrderBy((!Util.isEmpty(criteria.getSortings()) ? criteria.getSortings().get(0) : null), "d");
        Query query = entityManager
                .createNativeQuery(queryStr + orderBy);
        if (criteria.getPage() > 0 && criteria.getSize() > 0)
            query.setFirstResult((criteria.getPage() - 1) * criteria.getSize())
                    .setMaxResults(criteria.getSize());
        if (criteria.getBdate() != null) {
            query.setParameter("bdate", criteria.getBdate());
        }
        if (criteria.getEdate() != null) {
            query.setParameter("edate", criteria.getEdate());
        }
        if (!Util.isEmpty(criteria.getCommonFilter())) {
            String search = criteria.getCommonFilter().trim();
            if (dataRegexp.matcher(search).matches()) {
                String[] dateArr = search.split("\\.");
                String reverseSearch = dateArr[2] + "-" + dateArr[1] + "-" + dateArr[0];
                query.setParameter("search", criteria.getCommonFilter().trim());
                query.setParameter("reverseSearch", reverseSearch);
            } else {
                search = search.toLowerCase().replaceAll(":", "\\\\:").replaceAll("/", "\\\\/").replace(" ", "+") + ":*";
                query.setParameter("formattedSearch", search);
                query.setParameter("search", criteria.getCommonFilter());
            }
        }
        List<Object[]> resultList = query.getResultList();
        return convertToRowValue(criteria.getFields(), resultList);
    }

    public RowValue getRowData(String tableName, List<Field> fields, String systemId) {
        String keys = fields.stream().map(field -> addEscapeCharacters(field.getName())).collect(Collectors.joining(","));
        List<Object[]> list = entityManager.createNativeQuery(String.format(SELECT_ROWS_FROM_DATA_BY_FIELD, keys,
                addEscapeCharacters(tableName), addEscapeCharacters(DATA_PRIMARY_COLUMN)))
                .setParameter(1, systemId).getResultList();
        if (list.isEmpty())
            return null;
        return convertToRowValue(fields, list).get(0);
    }

    private String getDictionaryDataOrderBy(Sorting sorting, String alias) {
        String spaceAliasPoint = " " + alias + ".";
        String orderBy = " order by ";
        if (sorting != null && sorting.getField() != null) {
            orderBy = orderBy + formatFieldForQuery(sorting.getField(), alias) + " " + sorting.getDirection().toString() + ", ";
        }
        return orderBy + spaceAliasPoint + addEscapeCharacters(DATA_PRIMARY_COLUMN);
    }

    public BigInteger getDataCount(DataCriteria criteria) {
        String queryStr = "SELECT count(*)" +
                " FROM data." + addEscapeCharacters(criteria.getTableName()) + " d ";
        queryStr += getDataWhereClause(criteria.getBdate(), criteria.getEdate(), criteria.getCommonFilter(), criteria.getFieldFilter());
        Query query = entityManager.createNativeQuery(queryStr);
        if (criteria.getBdate() != null) {
            query.setParameter("bdate", criteria.getBdate());
        }
        if (criteria.getEdate() != null) {
            query.setParameter("edate", criteria.getEdate());
        }
        if (!Util.isEmpty(criteria.getCommonFilter())) {
            String search = criteria.getCommonFilter().trim();
            if (dataRegexp.matcher(search).matches()) {
                String[] dateArr = search.split("\\.");
                String reverseSearch = dateArr[2] + "-" + dateArr[1] + "-" + dateArr[0];
                query.setParameter("search", criteria.getCommonFilter().trim());
                query.setParameter("reverseSearch", reverseSearch);
            } else {
                search = search.toLowerCase().replaceAll(":", "\\\\:").replaceAll("/", "\\\\/").replace(" ", "+") + ":*";
                query.setParameter("formattedSearch", search);
                query.setParameter("search", criteria.getCommonFilter());
            }
        }
        return (BigInteger) query.getSingleResult();
    }

    private String getDataWhereClause(Date publishDate, Date closeDate, String search, List<FieldSearchCriteria> filter) {
        String result = " WHERE 1=1 ";
        if (publishDate != null) {
            result += " and (d.\"SYS_PUBLISHTIME\" is null or date_trunc('second', d.\"SYS_PUBLISHTIME\") <= to_timestamp(:bdate,'YYYY-MM-DD HH24:MI:SS') ) " +
                    "and (date_trunc('second', d.\"SYS_CLOSETIME\") > to_timestamp (:bdate, 'YYYY-MM-DD HH24:MI:SS') or d.\"SYS_CLOSETIME\" is null)";
        }
        if (closeDate != null) {
            result += " and (date_trunc('second', d.\"SYS_CLOSETIME\") >= to_timestamp (:edate, 'YYYY-MM-DD HH24:MI:SS') or d.\"SYS_CLOSETIME\" is null)";
        }
        result += getDictionaryFilterQuery(search, filter);
        return result;
    }

    private String getDictionaryFilterQuery(String search, List<FieldSearchCriteria> filter) {
        String queryStr = "";
        if (!Util.isEmpty(search)) {
            //full text search
            String original = new String(search);
            search = search.trim();
            String escapedFtsColumn = addEscapeCharacters(FULL_TEXT_SEARCH);
            if (dataRegexp.matcher(search).matches()) {
                queryStr += " and (" + escapedFtsColumn + " @@ to_tsquery(:search) or " + escapedFtsColumn + " @@ to_tsquery(:reverseSearch) ) ";
            } else {
                queryStr += " and (" + escapedFtsColumn + " @@ to_tsquery(:formattedSearch) or " + escapedFtsColumn + " @@ to_tsquery('ru', :formattedSearch) or " + escapedFtsColumn + " @@ to_tsquery('ru', ''':search'':*')) ";
            }
        } else if (!Util.isEmpty(filter)) {
            for (FieldSearchCriteria searchCriteria : filter) {
                //todo
//                String values = searchCriteria.getValues().stream().map(e -> "'" + e + "'").collect(Collectors.joining(","));
//                FieldValue fieldValue = searchCriteria.getValues().get(0);
//                String fieldName = fieldValue.getField().getName();
//                if (fieldValue.getField() instanceof IntegerField || fieldValue.getField() instanceof FloatField ||
//                        fieldValue.getField() instanceof DateField) {
//                    queryStr += " and " + addEscapeCharacters(fieldName) + " in (" + values + ")";
//                } else if (fieldValue.getField() instanceof ReferenceField) {
//                    queryStr += " and " + addEscapeCharacters(fieldName) + "->> 'value' in (" + values + ")";
//                    break;
//                } else if (fieldValue.getField() instanceof BooleanField) {
//                    if (searchCriteria.getValues().size() == 1 || searchCriteria.getValues().stream().map(Boolean::valueOf).reduce(Boolean::equals).orElse(false)) {
//                        queryStr += " and " + addEscapeCharacters(fieldName) +
//                                ((Boolean) (fieldValue.getValue()) ? " IS TRUE " : " IS NOT TRUE");
//                    }
//                    break;
//                } else if (fieldValue.getField() instanceof StringField) {
//                    if (SearchTypeEnum.LIKE.equals(searchCriteria.getType()) && searchCriteria.getValues().size() == 1)
//                        queryStr += " and " + addEscapeCharacters(fieldName) + " like '" + searchCriteria.getFormattedValue() + "'";
//                    else {
//                        queryStr += " and " + addEscapeCharacters(fieldName) + " in (" + values + ")";
//                    }
//                    break;
//                }
            }
        }
        return queryStr;
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

    public void insertData(String tableName, String keys, List<String> values, List<RowValue> data) {
        String stringValues = values.stream().collect(Collectors.joining("),("));
        Query query = entityManager.createNativeQuery(String.format(INSERT_QUERY_TEMPLATE, addEscapeCharacters(tableName), keys, stringValues));
        int i = 1;
        for (RowValue rowValue : data) {
            for (Object fieldValue : rowValue.getFieldValues()) {
                if (((FieldValue) fieldValue).getValue() != null)
                    query.setParameter(i++, ((FieldValue) fieldValue).getValue());
            }
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

    public BigInteger countActualDataFromVersion(String versionTable, String draftTable) {
        return (BigInteger) entityManager.createNativeQuery(
                String.format(COUNT_ACTUAL_VAL_FROM_VERSION,
                        addEscapeCharacters(versionTable),
                        addEscapeCharacters(draftTable)
                ))
                .getSingleResult();
    }

    public void insertActualDataFromVersion(String tableToInsert, String versionTableFromInsert, String draftTable, int offset, int transactionSize) {
        String query = String.format(INSERT_ACTUAL_VAL_FROM_VERSION,
                addEscapeCharacters(tableToInsert),
                addEscapeCharacters(versionTableFromInsert),
                addEscapeCharacters(draftTable),
                offset,
                transactionSize,
                getSequenceName(tableToInsert));
        if (logger.isDebugEnabled()) {
            logger.debug("insertActualDataFromVersion method query: " + query);
        }
        entityManager.createNativeQuery(
                query)
                .executeUpdate();
    }

    public BigInteger countOldDataFromVersion(String versionTable) {
        return (BigInteger) entityManager.createNativeQuery(
                String.format(COUNT_OLD_VAL_FROM_VERSION,
                        addEscapeCharacters(versionTable))).getSingleResult();
    }

    public void insertOldDataFromVersion(String tableToInsert, String tableFromInsert, int offset, int transactionSize) {
        String query = String.format(INSERT_OLD_VAL_FROM_VERSION,
                addEscapeCharacters(tableToInsert),
                addEscapeCharacters(tableFromInsert),
                offset,
                transactionSize,
                getSequenceName(tableToInsert));
        if (logger.isDebugEnabled()) {
            logger.debug("insertOldDataFromVersion method query: " + query);
        }
        entityManager.createNativeQuery(
                query).executeUpdate();
    }

    public BigInteger countClosedNowDataFromVersion(String versionTable, String draftTable) {
        return (BigInteger) entityManager.createNativeQuery(String.format(COUNT_CLOSED_NOW_VAL_FROM_VERSION,
                addEscapeCharacters(versionTable),
                addEscapeCharacters(draftTable)))
                .getSingleResult();
    }

    public void insertClosedNowDataFromVersion(String tableToInsert, String versionTable, String draftTable, int offset, int transactionSize, Date publishTime) {
        String query = String.format(INSERT_CLOSED_NOW_VAL_FROM_VERSION,
                addEscapeCharacters(tableToInsert),
                addEscapeCharacters(versionTable),
                addEscapeCharacters(draftTable),
                offset,
                transactionSize,
                new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(publishTime),
                getSequenceName(tableToInsert));
        if (logger.isDebugEnabled()) {
            logger.debug("insertClosedNowDataFromVersion method query: " + query);
        }
        entityManager.createNativeQuery(query)
                .executeUpdate();
    }

    public BigInteger countNewValFromDraft(String draftTable, String versionTable) {
        return (BigInteger) entityManager.createNativeQuery(
                String.format(COUNT_NEW_VAL_FROM_DRAFT,
                        addEscapeCharacters(draftTable),
                        addEscapeCharacters(versionTable)))
                .getSingleResult();

    }

    public void insertNewDataFromDraft(String tableToInsert, String versionTable, String draftTable, int offset, int transactionSize, Date publishTime) {
        String query = String.format(INSERT_NEW_VAL_FROM_DRAFT,
                addEscapeCharacters(draftTable),
                addEscapeCharacters(versionTable),
                offset,
                transactionSize,
                addEscapeCharacters(tableToInsert),
                new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(publishTime),
                getSequenceName(tableToInsert));
        if (logger.isDebugEnabled()) {
            logger.debug("insertNewDataFromDraft method query: " + query);
        }
        entityManager.createNativeQuery(
                query)
                .executeUpdate();
    }

    public void insertDataFromDraft(String draftTable, int offset, String targetTable, int transactionSize, Date publishTime, List<String> columns) {
        String columnsWithPrefix = columns.stream().map(s -> "row.\"" + s + "\"").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsStr = columns.stream().map(s -> "\"" + s + "\"").reduce((s1, s2) -> s1 + ", " + s2).get();
        String query = String.format(INSERT_FROM_DRAFT_TEMPLATE,
                addEscapeCharacters(draftTable),
                offset,
                transactionSize,
                addEscapeCharacters(targetTable),
                new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(publishTime),
                getSequenceName(targetTable),
                columnsStr,
                columnsWithPrefix);
        if (logger.isDebugEnabled()) {
            logger.debug("insertDataFromDraft method query: " + query);
        }
        entityManager.createNativeQuery(
                query)
                .executeUpdate();
    }

}
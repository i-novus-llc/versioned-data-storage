package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import cz.atria.common.lang.Util;
import net.n2oapp.criteria.api.CollectionPage;
import net.n2oapp.criteria.api.Sorting;
import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.platform.datastorage.temporal.model.criteria.CompareDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.FieldSearchCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.SearchTypeEnum;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil;
import ru.i_novus.platform.datastorage.temporal.model.*;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
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
                " FROM data." + addEscapeCharacters(criteria.getTableName()) + " d WHERE ";

        queryStr += getDataWhereClause(criteria.getBdate(), criteria.getEdate(), criteria.getCommonFilter(), criteria.getFieldFilter());
        String orderBy = getDictionaryDataOrderBy((!Util.isEmpty(criteria.getSortings()) ? criteria.getSortings().get(0) : null), "d");
        Query query = entityManager
                .createNativeQuery(queryStr + orderBy);
        if (criteria.getPage() > 0 && criteria.getSize() > 0)
            query.setFirstResult(getOffset(criteria))
                    .setMaxResults(criteria.getSize());
        setDataParameters(criteria, query);
        List<Object[]> resultList = query.getResultList();
        return convertToRowValue(criteria.getFields(), resultList);
    }

    public RowValue getRowData(String tableName, List<String> fieldNames, Object systemId) {
        String keys = fieldNames.stream().map(field -> addEscapeCharacters(field)).collect(Collectors.joining(","));
        List<Object[]> list = entityManager.createNativeQuery(String.format(SELECT_ROWS_FROM_DATA_BY_FIELD, keys,
                addEscapeCharacters(tableName), addEscapeCharacters(DATA_PRIMARY_COLUMN)))
                .setParameter(1, systemId).getResultList();
        if (list.isEmpty())
            return null;
        List<Object[]> dataTypes = entityManager.createNativeQuery("select column_name, data_type from information_schema.columns " +
                "where table_schema='data' and table_name=:table")
                .setParameter("table", tableName)
                .getResultList();
        List<Field> fields = new ArrayList<>(fieldNames.size());
        FieldFactory fieldFactory = new FieldFactory();
        for (Object[] dataType : dataTypes) {
            String fieldName = (String) dataType[0];
            if (fieldNames.contains(fieldName)) {
                fields.add(fieldFactory.getField(fieldName, (String) dataType[1]));
            }
        }
        RowValue row = convertToRowValue(fields, list).get(0);
        row.setSystemId(systemId);
        return row;
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
                " FROM data." + addEscapeCharacters(criteria.getTableName()) + " d WHERE ";
        queryStr += getDataWhereClause(criteria.getBdate(), criteria.getEdate(), criteria.getCommonFilter(), criteria.getFieldFilter());
        Query query = entityManager.createNativeQuery(queryStr);
        setDataParameters(criteria, query);
        return (BigInteger) query.getSingleResult();
    }

    private String getDataWhereClause(Date publishDate, Date closeDate, String search, List<FieldSearchCriteria> filter) {
        String result = " 1=1 ";
        if (publishDate != null) {
            result += " and date_trunc('second', d.\"SYS_PUBLISHTIME\") <= :bdate and (date_trunc('second', d.\"SYS_CLOSETIME\") > :bdate or d.\"SYS_CLOSETIME\" is null)";
        }
        if (closeDate != null) {
            result += " and (date_trunc('second', d.\"SYS_CLOSETIME\") >= :edate or d.\"SYS_CLOSETIME\" is null)";
        }
        result += getDictionaryFilterQuery(search, filter);
        return result;
    }

    private void setDataParameters(DataCriteria criteria, Query query) {
        if (criteria.getBdate() != null) {
            query.setParameter("bdate", truncateDateTo(criteria.getBdate(), ChronoUnit.SECONDS));
        }
        if (criteria.getEdate() != null) {
            query.setParameter("edate", truncateDateTo(criteria.getEdate(), ChronoUnit.SECONDS));
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
        if (!Util.isEmpty(criteria.getFieldFilter())) {
            for (FieldSearchCriteria searchCriteria : criteria.getFieldFilter()) {
                FieldValue fieldValue = searchCriteria.getValues().get(0);
                Field field = fieldValue.getField();
                if (field instanceof StringField && SearchTypeEnum.LIKE.equals(searchCriteria.getType()) && searchCriteria.getValues().size() == 1) {
                    query.setParameter(field.getName(), "%" + fieldValue.getValue().toString().trim() + "%");
                } else if (!(field instanceof BooleanField)) {
                    query.setParameter(field.getName(), searchCriteria.getValues().stream().map(f -> f.getValue()).collect(Collectors.toList()));
                }
            }
        }
    }

    private String getDictionaryFilterQuery(String search, List<FieldSearchCriteria> filter) {
        String queryStr = "";
        if (!Util.isEmpty(search)) {
            //full text search
            search = search.trim();
            String escapedFtsColumn = addEscapeCharacters(FULL_TEXT_SEARCH);
            if (dataRegexp.matcher(search).matches()) {
                queryStr += " and (" + escapedFtsColumn + " @@ to_tsquery(:search) or " + escapedFtsColumn + " @@ to_tsquery(:reverseSearch) ) ";
            } else {
                queryStr += " and (" + escapedFtsColumn + " @@ to_tsquery(:formattedSearch) or " + escapedFtsColumn + " @@ to_tsquery('ru', :formattedSearch) or " + escapedFtsColumn + " @@ to_tsquery('ru', ''':search'':*')) ";
            }
        } else if (!Util.isEmpty(filter)) {
            for (FieldSearchCriteria searchCriteria : filter) {
                FieldValue fieldValue = searchCriteria.getValues().get(0);
                String fieldName = fieldValue.getField().getName();
                if (fieldValue.getField() instanceof IntegerField || fieldValue.getField() instanceof FloatField ||
                        fieldValue.getField() instanceof DateField) {
                    queryStr += " and " + addEscapeCharacters(fieldName) + " in (:" + fieldName + ")";
                } else if (fieldValue.getField() instanceof ReferenceField) {
                    queryStr += " and " + addEscapeCharacters(fieldName) + "->> 'value' in (:" + fieldName + ")";
                    break;
                } else if (fieldValue.getField() instanceof BooleanField) {
                    if (searchCriteria.getValues().size() == 1) {
                        queryStr += " and " + addEscapeCharacters(fieldName) +
                                ((Boolean) (fieldValue.getValue()) ? " IS TRUE " : " IS NOT TRUE");
                    }
                    break;
                } else if (fieldValue.getField() instanceof StringField) {
                    if (SearchTypeEnum.LIKE.equals(searchCriteria.getType()) && searchCriteria.getValues().size() == 1)
                        queryStr += " and " + addEscapeCharacters(fieldName) + " like :" + fieldName + "";
                    else {
                        queryStr += " and " + addEscapeCharacters(fieldName) + " in (:" + fieldName + ")";
                    }
                    break;
                }
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

    public void copyTable(String newTableName, String sourceTableName) {
        entityManager.createNativeQuery(String.format(COPY_TABLE_TEMPLATE, addEscapeCharacters(newTableName),
                addEscapeCharacters(sourceTableName))).executeUpdate();
        entityManager.createNativeQuery(String.format("CREATE SEQUENCE data.\"%s_SYS_RECORDID_seq\" start 1", newTableName)).executeUpdate();
        List<String> indexes = entityManager.createNativeQuery("select indexdef from pg_indexes where tablename=?;")
                .setParameter(1, sourceTableName)
                .getResultList();
        for (String index : indexes) {
            entityManager.createNativeQuery(index.replaceAll(sourceTableName, newTableName)).executeUpdate();
        }
        entityManager.createNativeQuery(String.format("ALTER TABLE data.%s ADD PRIMARY KEY (\"SYS_RECORDID\")", addEscapeCharacters(newTableName))).executeUpdate();
        entityManager.createNativeQuery(String.format("ALTER TABLE data.%s ALTER COLUMN \"SYS_RECORDID\" SET DEFAULT nextval('data.\"%s_SYS_RECORDID_seq\"');", addEscapeCharacters(newTableName), newTableName)).executeUpdate();
    }

    public void dropTable(String tableName) {
        entityManager.createNativeQuery(String.format(DROP_TABLE, addEscapeCharacters(tableName))).executeUpdate();
    }

    public void addColumnToTable(String tableName, String name, String type) {
        entityManager.createNativeQuery(String.format(ADD_NEW_COLUMN, tableName, name, type)).executeUpdate();
    }

    public void deleteColumnFromTable(String tableName, String field) {
        entityManager.createNativeQuery(String.format(DELETE_COLUMN, tableName, field)).executeUpdate();
    }

    public List<Object> insertDataWithId(String tableName, String keys, List<String> values, List<RowValue> data) {
        String stringValues = values.stream().collect(Collectors.joining("),("));
        Query query = entityManager.createNativeQuery(String.format(INSERT_QUERY_TEMPLATE_WITH_ID, addEscapeCharacters(tableName), keys, stringValues));
        int i = 1;
        for (RowValue rowValue : data) {
            for (Object fieldValue : rowValue.getFieldValues()) {
                if (((FieldValue) fieldValue).getValue() != null)
                    query.setParameter(i++, ((FieldValue) fieldValue).getValue());
            }
        }
        return query.getResultList();
    }

    public void insertData(String tableName, String keys, List<String> values, List<RowValue> data) {
        int i = 1;
        int batchSize = 500;
        for (int firstIndex = 0, maxIndex = batchSize; firstIndex < values.size(); firstIndex = maxIndex, maxIndex = firstIndex + batchSize) {
            if (maxIndex > values.size())
                maxIndex = values.size();
            List<String> subValues = values.subList(firstIndex, maxIndex);
            List<RowValue> subData = data.subList(firstIndex, maxIndex);
            String stringValues = subValues.stream().collect(Collectors.joining("),("));
            Query query = entityManager.createNativeQuery(String.format(INSERT_QUERY_TEMPLATE, addEscapeCharacters(tableName), keys, stringValues));
            for (RowValue rowValue : subData) {
                for (Object fieldValue : rowValue.getFieldValues()) {
                    if (((FieldValue) fieldValue).getValue() != null)
                        query.setParameter(i++, ((FieldValue) fieldValue).getValue());
                }
            }
            query.executeUpdate();
            i = 1;
        }
    }

    public void loadData(String draftCode, String sourceStorageCode, List<String> fields, Date onDate) {
        String keys = fields.stream().collect(Collectors.joining(","));
        String where = getDataWhereClause(onDate, null, null, null);
        entityManager.createNativeQuery(String.format(COPY_QUERY_TEMPLATE, addEscapeCharacters(draftCode), keys, keys,
                addEscapeCharacters(sourceStorageCode), where))
                .setParameter("bdate", truncateDateTo(onDate, ChronoUnit.SECONDS))
                .executeUpdate();

    }

    public void updateData(String tableName, String keys, RowValue rowValue) {
        Query query = entityManager.createNativeQuery(String.format(UPDATE_QUERY_TEMPLATE, addEscapeCharacters(tableName), keys, "?"));
        int i = 1;
        for (Object obj : rowValue.getFieldValues()) {
            FieldValue fieldValue = (FieldValue) obj;
            if (fieldValue.getValue() != null)
                query.setParameter(i++, fieldValue.getValue());
        }
        query.setParameter(i, rowValue.getSystemId());
        query.executeUpdate();
    }

    public void deleteData(String tableName) {
        Query query = entityManager.createNativeQuery(String.format(DELETE_ALL_RECORDS_FROM_TABLE_QUERY_TEMPLATE, addEscapeCharacters(tableName)));
        query.executeUpdate();
    }

    public void deleteData(String tableName, List<Object> systemIds) {
        String ids = systemIds.stream().map(id -> "?").collect(Collectors.joining(","));
        Query query = entityManager.createNativeQuery(String.format(DELETE_QUERY_TEMPLATE, addEscapeCharacters(tableName), ids));
        int i = 1;
        for (Object systemId : systemIds) {
            query.setParameter(i++, systemId);
        }
        query.executeUpdate();
    }

    public void updateSequence(String tableName) {
        entityManager.createNativeQuery(String.format("SELECT setval('data.%s', (SELECT max(\"SYS_RECORDID\") FROM data.%s))",
                getSequenceName(tableName), addEscapeCharacters(tableName))).getSingleResult();
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

    public List getRowsByField(String tableName, String field, Object uniqueValue, boolean existDateColumns, Date begin, Date end, Object id) {
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
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(publishTime),
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
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(publishTime),
                getSequenceName(tableToInsert));
        if (logger.isDebugEnabled()) {
            logger.debug("insertNewDataFromDraft method query: " + query);
        }
        entityManager.createNativeQuery(
                query)
                .executeUpdate();
    }

    public void insertDataFromDraft(String draftTable, int offset, String targetTable, int transactionSize, Date publishTime, List<String> columns) {
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String query = String.format(INSERT_FROM_DRAFT_TEMPLATE,
                addEscapeCharacters(draftTable),
                offset,
                transactionSize,
                addEscapeCharacters(targetTable),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(publishTime),
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

    public DataDifference getDataDifference(CompareDataCriteria criteria) {
        DataDifference dataDifference;
        List<String> fields = criteria.getFields().stream().map(Field::getName).collect(Collectors.toList());
        Map<String, Field> fieldMap = new HashMap<>();
        for (Field field : criteria.getFields()) {
            fieldMap.put(field.getName(), field);
        }
        String countSelect = "SELECT count(*)";
        String orderBy = " order by " + criteria.getPrimaryFields().stream().map(f -> formatFieldForQuery(f, "t1")).collect(Collectors.joining(",")) + "," +
                criteria.getPrimaryFields().stream().map(f -> formatFieldForQuery(f, "t2")).collect(Collectors.joining(","));
        String dataSelect = "select t1." + addEscapeCharacters(DATA_PRIMARY_COLUMN) + " as sysId1," + generateSqlQuery("t1", fields) + ", "
                + "t2." + addEscapeCharacters(DATA_PRIMARY_COLUMN) + " as sysId2, " + generateSqlQuery("t2", fields);
        String primaryEquality = criteria.getPrimaryFields().stream().map(f -> formatFieldForQuery(f, "t1") + "=" + formatFieldForQuery(f, "t2")).collect(Collectors.joining(","));
        String basePrimaryIsNull = criteria.getPrimaryFields().stream().map(f -> formatFieldForQuery(f, "t1") + " is null ").collect(Collectors.joining(" and "));
        String targetPrimaryIsNull = criteria.getPrimaryFields().stream().map(f -> formatFieldForQuery(f, "t2") + " is null ").collect(Collectors.joining(" and "));
        String baseFilter = "date_trunc('second', t1.\"SYS_PUBLISHTIME\") <= :baseDate and (date_trunc('second', t1.\"SYS_CLOSETIME\") > :baseDate or t1.\"SYS_CLOSETIME\" is null) ";
        String targetFilter = "date_trunc('second', t2.\"SYS_PUBLISHTIME\") <= :targetDate and (date_trunc('second', t2.\"SYS_CLOSETIME\") > :targetDate or t2.\"SYS_CLOSETIME\" is null) ";
        String query = " from data." + addEscapeCharacters(criteria.getStorageCode()) + " t1 " +
                " full join data." + addEscapeCharacters(criteria.getStorageCode()) + " t2 on " + primaryEquality +
                " and " + baseFilter +
                " and " + targetFilter +
                " where ";
        if (criteria.getStatus() == null)
            query += basePrimaryIsNull + " and " + targetFilter +
                    " or " + targetPrimaryIsNull + " and " + baseFilter +
                    " or (" + primaryEquality + " and t1.\"SYS_HASH\"<>t2.\"SYS_HASH\") ";
        Query countQuery = entityManager.createNativeQuery(countSelect + query);
        countQuery.setParameter("baseDate", truncateDateTo(criteria.getBaseDataDate(), ChronoUnit.SECONDS));
        countQuery.setParameter("targetDate", truncateDateTo(criteria.getTargetDataDate(), ChronoUnit.SECONDS));
        BigInteger count = (BigInteger) countQuery.getSingleResult();
        if (BooleanUtils.toBoolean(criteria.getCountOnly())) {
            dataDifference = new DataDifference(new CollectionPage<>(count.intValue(), null, criteria));
        } else {
            Query dataQuery = entityManager.createNativeQuery(dataSelect + query + orderBy)
                    .setFirstResult(getOffset(criteria))
                    .setMaxResults(criteria.getSize());
            dataQuery.setParameter("baseDate", truncateDateTo(criteria.getBaseDataDate(), ChronoUnit.SECONDS));
            dataQuery.setParameter("targetDate", truncateDateTo(criteria.getTargetDataDate(), ChronoUnit.SECONDS));
            List<Object[]> resultList = dataQuery.getResultList();
            List<DiffRowValue> rowValues = new ArrayList<>();
            if (!resultList.isEmpty()) {
                for (Object[] row : resultList) {
                    List<DiffFieldValue> fieldValues = new ArrayList<>();
                    //get old/new versions data exclude sys_recordid
                    int i = 1;
                    List<String> primaryFields = criteria.getPrimaryFields();
                    DiffStatusEnum rowStatus = null;
                    for (String field : fields) {
                        DiffFieldValue fieldValue = new DiffFieldValue();
                        fieldValue.setField(fieldMap.get(field));
                        Object oldValue = row[i];
                        Object newValue = row[row.length / 2 + i];
                        fieldValue.setOldValue(oldValue);
                        fieldValue.setNewValue(newValue);
                        if (primaryFields.contains(field)) {
                            if (oldValue == null) {
                                rowStatus = DiffStatusEnum.INSERTED;
                            } else if (newValue == null) {
                                rowStatus = DiffStatusEnum.DELETED;
                            } else if (oldValue.equals(newValue)) {
                                rowStatus = DiffStatusEnum.UPDATED;
                            }
                        }
                        fieldValues.add(fieldValue);
                        i++;
                    }
                    for (DiffFieldValue fieldValue : fieldValues) {
                        if (DiffStatusEnum.INSERTED.equals(rowStatus))
                            fieldValue.setStatus(DiffStatusEnum.INSERTED);
                        else if (DiffStatusEnum.DELETED.equals(rowStatus))
                            fieldValue.setStatus(DiffStatusEnum.DELETED);
                        else {
                            Object oldValue = fieldValue.getOldValue();
                            Object newValue = fieldValue.getNewValue();
                            if (oldValue == null && newValue == null)
                                continue;
                            if (oldValue == null || newValue == null || !oldValue.equals(newValue)) {
                                fieldValue.setStatus(DiffStatusEnum.UPDATED);
                            } else {
                                //if value is not changed store only new value
                                fieldValue.setOldValue(null);
                            }
                        }
                    }
                    rowValues.add(new DiffRowValue(fieldValues, rowStatus));

                }
            }
            dataDifference = new DataDifference(new CollectionPage<>(count.intValue(), rowValues, criteria));
        }

        return dataDifference;
    }

}
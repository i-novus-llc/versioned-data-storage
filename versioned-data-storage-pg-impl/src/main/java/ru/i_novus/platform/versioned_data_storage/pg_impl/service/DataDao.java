package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import cz.atria.common.lang.Util;
import net.n2oapp.criteria.api.CollectionPage;
import net.n2oapp.criteria.api.Sorting;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.model.*;
import ru.i_novus.platform.datastorage.temporal.model.criteria.CompareDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.FieldSearchCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.SearchTypeEnum;
import ru.i_novus.platform.datastorage.temporal.model.value.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil;

import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.transaction.Transactional;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static org.springframework.util.CollectionUtils.isEmpty;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.service.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.*;

public class DataDao {

    public static final Date PG_MAX_TIMESTAMP = Date.from(LocalDateTime.of(294276, 12, 31, 23, 59).atZone(ZoneId.systemDefault()).toInstant());

    private static final Logger logger = LoggerFactory.getLogger(DataDao.class);
    private Pattern dataRegexp = Pattern.compile("([0-9]{2})\\.([0-9]{2})\\.([0-9]{4})");
    private EntityManager entityManager;

    public DataDao(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public List<RowValue> getData(DataCriteria criteria) {
        List<Field> fields = new ArrayList<>(criteria.getFields());
        fields.add(0, new IntegerField(DATA_PRIMARY_COLUMN));
        if (fields.stream().noneMatch(field -> SYS_HASH.equals(field.getName())))
            fields.add(1, new StringField(SYS_HASH));
        QueryWithParams queryWithParams = new QueryWithParams("SELECT " + generateSqlQuery("d", fields, true) +
                " FROM data." + addDoubleQuotes(criteria.getTableName()) + " d ", null);
        QueryWithParams dataWhereClause;
        if (isEmpty(criteria.getHashList())) {
            dataWhereClause = getDataWhereClause(criteria.getBdate(), criteria.getEdate(), criteria.getCommonFilter(), criteria.getFieldFilter());
        } else {
            dataWhereClause = getDataWhereClause(criteria.getBdate(), criteria.getEdate(), criteria.getCommonFilter(),
                    singleton(singletonList(new FieldSearchCriteria(new StringField(SYS_HASH), SearchTypeEnum.EXACT, criteria.getHashList()))));
        }
        queryWithParams.concat(dataWhereClause);
        queryWithParams.concat(new QueryWithParams(
                getDictionaryDataOrderBy((!Util.isEmpty(criteria.getSortings()) ? criteria.getSortings().get(0) : null), "d"),
                null
        ));
        Query query = queryWithParams.createQuery(entityManager);
        if (criteria.getPage() > 0 && criteria.getSize() > 0)
            query.setFirstResult(getOffset(criteria))
                    .setMaxResults(criteria.getSize());
        List<Object[]> resultList = query.getResultList();
        return convertToRowValue(fields, resultList);
    }

    public List<String> getNotExists(String tableName, Date bdate, Date edate, List<String> hashList) {
        Map<String, Object> params = new HashMap<>();
        String sqlHashArray = "array[" + hashList.stream().map(hash -> {
            String hashPlaceHolder = "hash" + params.size();
            params.put(hashPlaceHolder, hash);
            return ":" + hashPlaceHolder;
        }).collect(joining(",")) + "]";

        QueryWithParams dataWhereClause = getDataWhereClause(bdate, edate, null, null);
        String query = "SELECT hash FROM (" +
                "SELECT unnest(" + sqlHashArray + ") hash) hashes WHERE hash NOT IN (" +
                "SELECT " + addDoubleQuotes(SYS_HASH) + " FROM data." + addDoubleQuotes(tableName) + " d " +
                dataWhereClause.getQuery() + ")";
        params.putAll(dataWhereClause.params);
        QueryWithParams queryWithParams = new QueryWithParams(query, params);
        List<String> resultList = queryWithParams.createQuery(entityManager).getResultList();
        return resultList;
    }

    public RowValue getRowData(String tableName, List<String> fieldNames, Object systemId) {
        Map<String, String> dataTypes = getColumnDataTypes(tableName);
        List<Field> fields = new ArrayList<>(fieldNames.size());

        for (Map.Entry<String, String> entry : dataTypes.entrySet()) {
            String fieldName = entry.getKey();
            if (fieldNames.contains(fieldName)) {
                fields.add(getField(fieldName, entry.getValue()));
            }
        }
        fields.add(0, new IntegerField(DATA_PRIMARY_COLUMN));
        String keys = generateSqlQuery(null, fields, true);
        List<Object[]> list = entityManager.createNativeQuery(String.format(SELECT_ROWS_FROM_DATA_BY_FIELD, keys,
                addDoubleQuotes(tableName), addDoubleQuotes(DATA_PRIMARY_COLUMN)))
                .setParameter(1, systemId).getResultList();
        if (list.isEmpty())
            return null;


        RowValue row = convertToRowValue(fields, list).get(0);
        row.setSystemId(systemId);
        return row;
    }

    public boolean tableStructureEquals(String tableName1, String tableName2) {
        Map<String, String> dataTypes1 = getColumnDataTypes(tableName1);
        Map<String, String> dataTypes2 = getColumnDataTypes(tableName2);
        return dataTypes1.equals(dataTypes2);
    }

    public Map<String, String> getColumnDataTypes(String tableName) {
        List<Object[]> dataTypes = entityManager.createNativeQuery("SELECT column_name, data_type FROM information_schema.columns " +
                "WHERE table_schema='data' AND table_name=:table")
                .setParameter("table", tableName)
                .getResultList();
        Map<String, String> map = new HashMap<>();
        for (Object[] dataType : dataTypes) {
            String fieldName = (String) dataType[0];
            if (!SYS_RECORDS.contains(fieldName))
                map.put(fieldName, (String) dataType[1]);
        }
        return map;
    }

    private String getDictionaryDataOrderBy(Sorting sorting, String alias) {
        String spaceAliasPoint = " " + alias + ".";
        String orderBy = " order by ";
        if (sorting != null && sorting.getField() != null) {
            orderBy = orderBy + formatFieldForQuery(sorting.getField(), alias) + " " + sorting.getDirection().toString() + ", ";
        }
        return orderBy + spaceAliasPoint + addDoubleQuotes(DATA_PRIMARY_COLUMN);
    }

    public BigInteger getDataCount(DataCriteria criteria) {
        QueryWithParams queryWithParams = new QueryWithParams("SELECT count(*)" +
                " FROM data." + addDoubleQuotes(criteria.getTableName()) + " d ", null);
        queryWithParams.concat(getDataWhereClause(criteria.getBdate(), criteria.getEdate(), criteria.getCommonFilter(), criteria.getFieldFilter()));
        return (BigInteger) queryWithParams.createQuery(entityManager).getSingleResult();
    }

    @Deprecated//todo о избавиться
    public String getDataWhereClauseStr(Date publishDate, Date closeDate, String search, Set<List<FieldSearchCriteria>> filter) {
        String result = " 1=1 ";
        if (publishDate != null) {
            result += " and date_trunc('second', d.\"SYS_PUBLISHTIME\") <= :bdate and (date_trunc('second', d.\"SYS_CLOSETIME\") > :bdate or d.\"SYS_CLOSETIME\" is null)";
        }
        if (closeDate != null) {
            result += " and (date_trunc('second', d.\"SYS_CLOSETIME\") >= :edate or d.\"SYS_CLOSETIME\" is null)";
        }
        result += getDictionaryFilterQuery(search, filter, null).getQuery();
        return result;
    }

    private QueryWithParams getDataWhereClause(Date publishDate, Date closeDate, String search, Set<List<FieldSearchCriteria>> filters) {
        closeDate = closeDate == null ? PG_MAX_TIMESTAMP : closeDate;
        Map<String, Object> params = new HashMap<>();
        String result = " WHERE 1=1 ";
        if (publishDate != null) {
            result += " and date_trunc('second', d.\"SYS_PUBLISHTIME\") <= :bdate and (date_trunc('second', d.\"SYS_CLOSETIME\") > :bdate or d.\"SYS_CLOSETIME\" is null)";
            params.put("bdate", truncateDateTo(publishDate, ChronoUnit.SECONDS));
            result += " and (date_trunc('second', d.\"SYS_CLOSETIME\") >= :edate or d.\"SYS_CLOSETIME\" is null)";
            params.put("edate", truncateDateTo(closeDate, ChronoUnit.SECONDS));
        }
        QueryWithParams queryWithParams = new QueryWithParams(result, params);
        queryWithParams.concat(getDictionaryFilterQuery(search, filters, null));
        return queryWithParams;
    }

    private QueryWithParams getDictionaryFilterQuery(String search, Set<List<FieldSearchCriteria>> filters, String alias) {
        Map<String, Object> params = new HashMap<>();
        String queryStr = "";
        if (!Util.isEmpty(search)) {
            //full text search
            search = search.trim();
            String escapedFtsColumn = addDoubleQuotes(FULL_TEXT_SEARCH);
            if (dataRegexp.matcher(search).matches()) {
                queryStr += " and (" + escapedFtsColumn + " @@ to_tsquery(:search) or " + escapedFtsColumn + " @@ to_tsquery(:reverseSearch) ) ";
                String[] dateArr = search.split("\\.");
                String reverseSearch = dateArr[2] + "-" + dateArr[1] + "-" + dateArr[0];
                params.put("search", search.trim());
                params.put("reverseSearch", reverseSearch);
            } else {
                String formattedSearch = search.toLowerCase().replaceAll(":", "\\\\:").replaceAll("/", "\\\\/").replace(" ", "+");
                queryStr += " and (" + escapedFtsColumn + " @@ to_tsquery(:formattedSearch||':*') or " + escapedFtsColumn + " @@ to_tsquery('ru', :formattedSearch||':*') or " + escapedFtsColumn + " @@ to_tsquery('ru', :original||':*')) ";
                params.put("formattedSearch", "'" + formattedSearch + "'");
                params.put("original", "'''" + search + "'''");
            }
        } else if (!Util.isEmpty(filters)) {
            final int[] i = {-1};
            queryStr += filters.stream().map(listOfFilters -> {
                if (isEmpty(listOfFilters))
                    return "";
                String filter = "1 = 1";
                for (FieldSearchCriteria searchCriteria : listOfFilters) {
                    i[0]++;
                    Field field = searchCriteria.getField();
                    String fieldName = searchCriteria.getField().getName();
                    String escapedFieldName = addDoubleQuotes(fieldName);
                    if (alias != null && !"".equals(alias))
                        escapedFieldName = alias + "." + escapedFieldName;
                    if (searchCriteria.getValues() == null || searchCriteria.getValues().get(0) == null) {
                        filter += " and " + escapedFieldName + " is null";
                    } else if (field instanceof IntegerField || field instanceof FloatField || field instanceof DateField) {
                        filter += " and " + escapedFieldName + " in (:" + fieldName + i[0] + ")";
                        params.put(fieldName + i[0], searchCriteria.getValues());
                    } else if (field instanceof ReferenceField) {
                        filter += " and " + escapedFieldName + "->> 'value' in (:" + fieldName + i[0] + ")";
                        params.put(fieldName + i[0], searchCriteria.getValues().stream().map(Object::toString).collect(Collectors.toList()));
                    } else if (field instanceof TreeField) {
                        if (SearchTypeEnum.LESS.equals(searchCriteria.getType())) {
                            filter += " and " + escapedFieldName + "@> (cast(:" + fieldName + i[0] + " AS ltree[]))";
                            String v = searchCriteria.getValues().stream().map(Object::toString).collect(Collectors.joining(",", "{", "}"));
                            params.put(fieldName + i[0], v);
                        }
                    } else if (field instanceof BooleanField) {
                        if (searchCriteria.getValues().size() == 1) {
                            filter += " and " + escapedFieldName +
                                    ((Boolean) (searchCriteria.getValues().get(0)) ? " IS TRUE " : " IS NOT TRUE");
                        }
                    } else if (field instanceof StringField) {
                        if (SearchTypeEnum.LIKE.equals(searchCriteria.getType()) && searchCriteria.getValues().size() == 1) {
                            filter += " and lower(" + escapedFieldName + ") like :" + fieldName + i[0] + "";
                            params.put(fieldName + i[0], "%" + searchCriteria.getValues().get(0).toString().trim().toLowerCase() + "%");
                        } else {
                            filter += " and " + escapedFieldName + " in (:" + fieldName + i[0] + ")";
                            params.put(fieldName + i[0], searchCriteria.getValues());
                        }
                    } else {
                        params.put(fieldName + i[0], searchCriteria.getValues());
                    }
                }
                return filter;
            }).collect(Collectors.joining(" or "));
            if (!queryStr.equals(""))
                queryStr = " and (" + queryStr + ")";
        }
        return new QueryWithParams(queryStr, params);
    }

    public BigInteger countData(String tableName) {
        return (BigInteger) entityManager.createNativeQuery(String.format(SELECT_COUNT_QUERY_TEMPLATE, addDoubleQuotes(tableName))).getSingleResult();
    }

    @Transactional
    public void createDraftTable(String tableName, List<Field> fields) {
        if (Util.isEmpty(fields)) {
            entityManager.createNativeQuery(String.format(CREATE_EMPTY_DRAFT_TABLE_TEMPLATE, addDoubleQuotes(tableName), tableName)).executeUpdate();
        } else {
            String fieldsString = fields.stream().map(f -> addDoubleQuotes(f.getName()) + " " + f.getType()).collect(Collectors.joining(", "));
            entityManager.createNativeQuery(String.format(CREATE_DRAFT_TABLE_TEMPLATE, addDoubleQuotes(tableName), fieldsString, tableName)).executeUpdate();
        }
    }

    @Transactional
    public void copyTable(String newTableName, String sourceTableName) {
        entityManager.createNativeQuery(String.format(COPY_TABLE_TEMPLATE, addDoubleQuotes(newTableName),
                addDoubleQuotes(sourceTableName))).executeUpdate();
        entityManager.createNativeQuery(String.format("CREATE SEQUENCE data.\"%s_SYS_RECORDID_seq\" start 1", newTableName)).executeUpdate();
        List<String> indexes = entityManager.createNativeQuery("SELECT indexdef FROM pg_indexes WHERE tablename=? AND NOT indexdef LIKE '%\"SYS_HASH\"%';")
                .setParameter(1, sourceTableName)
                .getResultList();
        for (String index : indexes) {
            entityManager.createNativeQuery(index.replaceAll(sourceTableName, newTableName)).executeUpdate();
        }
        createHashIndex(newTableName);
        entityManager.createNativeQuery(String.format("ALTER TABLE data.%s ADD PRIMARY KEY (\"SYS_RECORDID\")", addDoubleQuotes(newTableName))).executeUpdate();
        entityManager.createNativeQuery(String.format("ALTER TABLE data.%s ALTER COLUMN \"SYS_RECORDID\" SET DEFAULT nextval('data.\"%s_SYS_RECORDID_seq\"');", addDoubleQuotes(newTableName), newTableName)).executeUpdate();
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void dropTable(String tableName) {
        entityManager.createNativeQuery(String.format(DROP_TABLE, addDoubleQuotes(tableName))).executeUpdate();
    }

    @Transactional
    public void addColumnToTable(String tableName, String name, String type, String defaultValue) {
        if (defaultValue != null)
            entityManager.createNativeQuery(String.format(ADD_NEW_COLUMN_WITH_DEFAULT, tableName, name, type, defaultValue)).executeUpdate();
        else
            entityManager.createNativeQuery(String.format(ADD_NEW_COLUMN, tableName, name, type)).executeUpdate();
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void deleteColumnFromTable(String tableName, String field) {
        entityManager.createNativeQuery(String.format(DELETE_COLUMN, tableName, field)).executeUpdate();
    }

    @Transactional
    public void insertData(String tableName, List<RowValue> data) {
        if (isEmpty(data))
            return;
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat(TIMESTAMP_DATE_FORMAT);
        for (Object fieldValue : data.iterator().next().getFieldValues()) {
            keys.add(addDoubleQuotes(((FieldValue) fieldValue).getField()));
        }
        for (RowValue rowValue : data) {
            List<String> rowValues = new ArrayList<>(rowValue.getFieldValues().size());
            for (Object fieldValueObj : rowValue.getFieldValues()) {
                FieldValue fieldValue = (FieldValue) fieldValueObj;
                if (fieldValue.getValue() == null) {
                    rowValues.add("null");
                } else if (fieldValue instanceof ReferenceFieldValue) {
                    Reference refValue = ((ReferenceFieldValue) fieldValue).getValue();
                    if (refValue.getValue() == null)
                        rowValues.add("null");
                    else {
                        if (ofNullable(refValue.getDisplayExpression()).map(DisplayExpression::getValue).isPresent()) {
                            rowValues.add(String.format("(select jsonb_build_object('value', d.%s , 'displayValue', %s, 'hash', d.\"SYS_HASH\") from data.%s d where d.%s=?\\:\\:" + getFieldType(refValue.getStorageCode(), refValue.getKeyField()) + " and %s)",
                                    addDoubleQuotes(refValue.getKeyField()),
                                    sqlDisplayExpression(refValue.getDisplayExpression(), "d"),
                                    addDoubleQuotes(refValue.getStorageCode()), addDoubleQuotes(refValue.getKeyField()),
                                    getDataWhereClauseStr(refValue.getDate(), null, null, null).replace(":bdate", addSingleQuotes(sdf.format(refValue.getDate())))));
                        } else if (refValue.getDisplayField() != null)
                            rowValues.add(String.format("(select jsonb_build_object('value', d.%s , 'displayValue', d.%s, 'hash', d.\"SYS_HASH\") from data.%s d where d.%s=?\\:\\:" + getFieldType(refValue.getStorageCode(), refValue.getKeyField()) + " and %s)",
                                    addDoubleQuotes(refValue.getKeyField()),
                                    addDoubleQuotes(refValue.getDisplayField()),
                                    addDoubleQuotes(refValue.getStorageCode()), addDoubleQuotes(refValue.getKeyField()),
                                    getDataWhereClauseStr(refValue.getDate(), null, null, null).replace(":bdate", addSingleQuotes(sdf.format(refValue.getDate())))));
                        else
                            rowValues.add("(select jsonb_build_object('value', ?))");
                    }
                } else if (fieldValue instanceof TreeFieldValue) {
                    rowValues.add("?\\:\\:ltree");
                } else {
                    rowValues.add("?");
                }
            }
            values.add(String.join(",", rowValues));
        }
        int i = 1;
        int batchSize = 500;
        for (int firstIndex = 0, maxIndex = batchSize; firstIndex < values.size(); firstIndex = maxIndex, maxIndex = firstIndex + batchSize) {
            if (maxIndex > values.size())
                maxIndex = values.size();
            List<String> subValues = values.subList(firstIndex, maxIndex);
            List<RowValue> subData = data.subList(firstIndex, maxIndex);
            String stringValues = subValues.stream().collect(Collectors.joining("),("));
            Query query = entityManager.createNativeQuery(String.format(INSERT_QUERY_TEMPLATE, addDoubleQuotes(tableName), String.join(",", keys), stringValues));
            for (RowValue rowValue : subData) {
                for (Object value : rowValue.getFieldValues()) {
                    FieldValue fieldValue = (FieldValue) value;
                    if (fieldValue.getValue() != null) {
                        if (fieldValue instanceof ReferenceFieldValue) {
                            Reference refValue = ((ReferenceFieldValue) fieldValue).getValue();
                            if (refValue.getValue() != null)
                                query.setParameter(i++, ((ReferenceFieldValue) fieldValue).getValue().getValue());

                        } else
                            query.setParameter(i++, fieldValue.getValue());
                    }
                }
            }
            query.executeUpdate();
            i = 1;
        }
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void loadData(String draftCode, String sourceStorageCode, List<String> fields, Date fromDate, Date toDate ) {
        String keys = fields.stream().collect(Collectors.joining(","));
        String values = fields.stream().map(f -> "d." + f).collect(Collectors.joining(","));
        QueryWithParams queryWithParams = new QueryWithParams(String.format(COPY_QUERY_TEMPLATE, addDoubleQuotes(draftCode), keys, values,
                addDoubleQuotes(sourceStorageCode)), null);
        queryWithParams.concat(getDataWhereClause(fromDate, toDate, null, null));
        queryWithParams.createQuery(entityManager).executeUpdate();
    }

    @Transactional
    public void updateData(String tableName, RowValue rowValue) {
        List<String> keyList = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat(TIMESTAMP_DATE_FORMAT);
        for (Object objectValue : rowValue.getFieldValues()) {
            FieldValue fieldValue = (FieldValue) objectValue;
            String fieldName = fieldValue.getField();
            if (fieldValue.getValue() == null || fieldValue.getValue().equals("null")) {
                keyList.add(addDoubleQuotes(fieldName) + " = NULL");
            } else if (fieldValue instanceof ReferenceFieldValue) {
                Reference refValue = ((ReferenceFieldValue) fieldValue).getValue();
                if (refValue.getValue() == null)
                    keyList.add(addDoubleQuotes(fieldName) + " = NULL");
                else {
                    if (ofNullable(refValue.getDisplayExpression()).map(DisplayExpression::getValue).isPresent()) {
                        keyList.add(addDoubleQuotes(fieldName) + String.format("=(select jsonb_build_object('value', d.%s , 'displayValue', %s, 'hash', d.\"SYS_HASH\") from data.%s d where d.%s=?\\:\\:" + getFieldType(refValue.getStorageCode(), refValue.getKeyField()) + " and %s)",
                                addDoubleQuotes(refValue.getKeyField()),
                                sqlDisplayExpression(refValue.getDisplayExpression(), "d"),
                                addDoubleQuotes(refValue.getStorageCode()),
                                addDoubleQuotes(refValue.getKeyField()),
                                getDataWhereClauseStr(refValue.getDate(), null, null, null).replace(":bdate", addSingleQuotes(sdf.format(refValue.getDate())))));
                    } else if (refValue.getDisplayField() != null)
                        keyList.add(addDoubleQuotes(fieldName) + String.format("=(select jsonb_build_object('value', d.%s , 'displayValue', d.%s, 'hash', d.\"SYS_HASH\") from data.%s d where d.%s=?\\:\\:" + getFieldType(refValue.getStorageCode(), refValue.getKeyField()) + " and %s)",
                                addDoubleQuotes(refValue.getKeyField()),
                                addDoubleQuotes(refValue.getDisplayField()),
                                addDoubleQuotes(refValue.getStorageCode()),
                                addDoubleQuotes(refValue.getKeyField()),
                                getDataWhereClauseStr(refValue.getDate(), null, null, null).replace(":bdate", addSingleQuotes(sdf.format(refValue.getDate())))));
                    else
                        keyList.add(addDoubleQuotes(fieldName) + "=(select jsonb_build_object('value', ?))");
                }
            } else if (fieldValue instanceof TreeFieldValue) {
                keyList.add(addDoubleQuotes(fieldName) + " = ?\\:\\:ltree");
            } else {
                keyList.add(addDoubleQuotes(fieldName) + " = ?");
            }
        }

        String keys = String.join(",", keyList);
        Query query = entityManager.createNativeQuery(String.format(UPDATE_QUERY_TEMPLATE, addDoubleQuotes(tableName), keys, "?"));
        int i = 1;
        for (Object obj : rowValue.getFieldValues()) {
            FieldValue fieldValue = (FieldValue) obj;
            if (fieldValue.getValue() != null)
                if (fieldValue instanceof ReferenceFieldValue) {
                    if (((ReferenceFieldValue) fieldValue).getValue().getValue() != null)
                        query.setParameter(i++, ((ReferenceFieldValue) fieldValue).getValue().getValue());
                } else
                    query.setParameter(i++, fieldValue.getValue());
        }
        query.setParameter(i, rowValue.getSystemId());
        query.executeUpdate();
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void deleteData(String tableName) {
        Query query = entityManager.createNativeQuery(String.format(DELETE_ALL_RECORDS_FROM_TABLE_QUERY_TEMPLATE, addDoubleQuotes(tableName)));
        query.executeUpdate();
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void deleteData(String tableName, List<Object> systemIds) {
        String ids = systemIds.stream().map(id -> "?").collect(Collectors.joining(","));
        Query query = entityManager.createNativeQuery(String.format(DELETE_QUERY_TEMPLATE, addDoubleQuotes(tableName), ids));
        int i = 1;
        for (Object systemId : systemIds) {
            query.setParameter(i++, systemId);
        }
        query.executeUpdate();
    }

    @Transactional
    public void deleteEmptyRows(String draftCode) {
        List<String> fieldNames = getFieldNames(draftCode);
        if (isEmpty(fieldNames)) {
            deleteData(draftCode);
        } else {
            String allFieldsNullWhere = fieldNames.stream().map(s -> s + " IS NULL").collect(joining(" AND "));
            Query query = entityManager.createNativeQuery(String.format(DELETE_EMPTY_RECORDS_FROM_TABLE_QUERY_TEMPLATE, addDoubleQuotes(draftCode), allFieldsNullWhere));
            query.executeUpdate();
        }
    }

    public boolean isUnique(String storageCode, List<String> fieldNames, Date publishTime) {
        String fields = fieldNames.stream().map(fieldName -> addDoubleQuotes(fieldName) + "\\:\\:text")
                .collect(Collectors.joining(","));
        String groupBy = Stream.iterate(1, n -> n + 1).limit(fieldNames.size()).map(String::valueOf)
                .collect(Collectors.joining(","));

        Query query = entityManager.createNativeQuery(
                "SELECT " + fields + ", COUNT(*)" +
                        " FROM data." + addDoubleQuotes(storageCode) + " d" +
                        " WHERE " + getDataWhereClauseStr(publishTime, null, null, null) +
                        " GROUP BY " + groupBy +
                        " HAVING COUNT(*) > 1"
        );
        if (publishTime != null)
            query.setParameter("bdate", publishTime);
        return query.getResultList().isEmpty();
    }


    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void updateSequence(String tableName) {
        entityManager.createNativeQuery(String.format("SELECT setval('data.%s', (SELECT max(\"SYS_RECORDID\") FROM data.%s))",
                getSequenceName(tableName), addDoubleQuotes(tableName))).getSingleResult();
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createTrigger(String tableName) {
        createTrigger(tableName, getFieldNames(tableName));
    }

    @Transactional
    public void createTrigger(String tableName, List<String> fields) {
        String escapedTableName = addDoubleQuotes(tableName);
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

    @Transactional
    public void updateHashRows(String tableName) {
        List<String> fieldNames = getFieldNames(tableName);
        entityManager.createNativeQuery(String.format(UPDATE_HASH,
                addDoubleQuotes(tableName),
                fieldNames.stream().collect(Collectors.joining(", ")))).executeUpdate();
    }

    @Transactional
    public void updateFtsRows(String tableName) {
        List<String> fieldNames = getFieldNames(tableName);
        entityManager.createNativeQuery(String.format(UPDATE_FTS,
                addDoubleQuotes(tableName),
                fieldNames.stream().map(field -> "coalesce( to_tsvector('ru', " + field + "\\:\\:text),'')")
                        .collect(Collectors.joining(" || ' ' || ")))).executeUpdate();
    }


    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void dropTrigger(String tableName) {
        String escapedTableName = addDoubleQuotes(tableName);
        entityManager.createNativeQuery(String.format(DROP_HASH_TRIGGER, escapedTableName)).executeUpdate();
        entityManager.createNativeQuery(String.format(DROP_FTS_TRIGGER, escapedTableName)).executeUpdate();
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createIndex(String tableName, String field) {
        entityManager.createNativeQuery(String.format(CREATE_TABLE_INDEX, addDoubleQuotes(tableName + "_" + field.toLowerCase() + "_idx"),
                addDoubleQuotes(tableName), addDoubleQuotes(field))).executeUpdate();
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createFullTextSearchIndex(String tableName) {
        entityManager.createNativeQuery(String.format(CREATE_FTS_INDEX, addDoubleQuotes(tableName + "_fts_idx"),
                addDoubleQuotes(tableName),
                addDoubleQuotes(FULL_TEXT_SEARCH))).executeUpdate();
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createLtreeIndex(String tableName, String field) {
        entityManager.createNativeQuery(String.format(CREATE_LTREE_INDEX, addDoubleQuotes(tableName + "_" + field.toLowerCase() + "_idx"),
                addDoubleQuotes(tableName),
                addDoubleQuotes(field))).executeUpdate();
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createHashIndex(String tableName) {
        entityManager.createNativeQuery(String.format(CREATE_TABLE_HASH_INDEX, addDoubleQuotes(tableName + "_sys_hash_ix"),
                addDoubleQuotes(tableName))).executeUpdate();
    }

    public List<String> getFieldNames(String tableName) {
        List<String> results = entityManager.createNativeQuery(String.format(SELECT_FIELD_NAMES, tableName)).getResultList();
        Collections.sort(results);
        return results.stream().map(QueryUtil::addDoubleQuotes).collect(Collectors.toList());
    }

    public String getFieldType(String tableName, String field) {
        return entityManager.createNativeQuery(String.format(SELECT_FIELD_TYPE, tableName, field)).getSingleResult().toString();
    }

    public void alterDataType(String tableName, String field, String oldType, String newType) {
        String escapedField = addDoubleQuotes(field);
        String using = "";
        if (DateField.TYPE.equals(oldType) && (StringField.TYPE.equals(newType) || IntegerStringField.TYPE.equals(newType))) {
            using = "to_char(" + escapedField + ", '" + DATE_FORMAT_FOR_USING_CONVERTING + "')";
        } else if (DateField.TYPE.equals(newType) && StringField.TYPE.equals(oldType)) {
            using = "to_date(" + escapedField + ", '" + DATE_FORMAT_FOR_USING_CONVERTING + "')";
        } else if (ReferenceField.TYPE.equals(oldType)) {
            using = "(" + escapedField + "->>'value')" + "\\:\\:varchar\\:\\:" + newType;
        } else if (ReferenceField.TYPE.equals(newType)) {
            using = "nullif(jsonb_build_object('value'," + escapedField + "),jsonb_build_object('value',null))";
        } else if (StringField.TYPE.equals(oldType) || IntegerStringField.TYPE.equals(oldType)
                || StringField.TYPE.equals(newType) || IntegerStringField.TYPE.equals(newType)) {
            using = escapedField + "\\:\\:" + newType;
        } else {
            using = escapedField + "\\:\\:varchar\\:\\:" + newType;
        }
        entityManager.createNativeQuery(String.format(ALTER_COLUMN_WITH_USING, addDoubleQuotes(tableName),
                escapedField, newType, using)).executeUpdate();
    }

    public List getRowsByField(String tableName, String field, Object uniqueValue, boolean existDateColumns, Date begin, Date end, Object id) {
        String query = SELECT_ROWS_FROM_DATA_BY_FIELD;
        String rows = addDoubleQuotes(field);
        if (existDateColumns) {
            SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy");
            rows += "," + addDoubleQuotes(DATE_BEGIN) + "," + addDoubleQuotes(DATE_END);
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
            query += " and " + addDoubleQuotes("SYS_RECORDID") + " != " + id;
        }
        Query nativeQuery = entityManager.createNativeQuery(String.format(query, rows, addDoubleQuotes(tableName), addDoubleQuotes(field)));
        nativeQuery.setParameter(1, uniqueValue);
        return nativeQuery.getResultList();
    }

    public boolean isFieldNotEmpty(String tableName, String fieldName) {
        return (boolean) entityManager
                .createNativeQuery(String.format(IS_FIELD_NOT_EMPTY, addDoubleQuotes(tableName),
                        addDoubleQuotes(tableName),
                        addDoubleQuotes(fieldName)))
                .getSingleResult();
    }

    public boolean isFieldContainEmptyValues(String tableName, String fieldName) {
        return (boolean) entityManager
                .createNativeQuery(String.format(IS_FIELD_CONTAIN_EMPTY_VALUES, addDoubleQuotes(tableName),
                        addDoubleQuotes(tableName),
                        addDoubleQuotes(fieldName)))
                .getSingleResult();
    }

    public BigInteger countActualDataFromVersion(String versionTable, String draftTable, Date publishTime, Date closeTime) {

        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        final SimpleDateFormat df = new SimpleDateFormat(TIMESTAMP_DATE_FORMAT);
        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("draftTable", "data." + addDoubleQuotes(draftTable));
        placeholderValues.put("versionTable", "data." + addDoubleQuotes(versionTable));
        placeholderValues.put("publishTime", df.format(publishTime));
        placeholderValues.put("closeTime", df.format(closeTime));
        String query = StrSubstitutor.replace(COUNT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);
        return (BigInteger) entityManager.createNativeQuery(query).getSingleResult();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertActualDataFromVersion(String tableToInsert, String versionTable, String draftTable,
                                            Map<String, String> columns, int offset, int transactionSize, Date publishTime, Date closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        String columnsStr = columns.keySet().stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithType = columns.keySet().stream().map(s -> s + " " + columns.get(s)).reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefixValue = columns.keySet().stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefixD = columns.keySet().stream().map(s -> "d." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        final SimpleDateFormat dateFormat = new SimpleDateFormat(TIMESTAMP_DATE_FORMAT);

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("dColumns", columnsWithPrefixD);
        placeholderValues.put("vValues", columnsWithPrefixValue);
        placeholderValues.put("versionTable", "data." + addDoubleQuotes(versionTable));
        placeholderValues.put("draftTable", "data." + addDoubleQuotes(draftTable));
        placeholderValues.put("publishTime", dateFormat.format(publishTime));
        placeholderValues.put("closeTime", dateFormat.format(closeTime));
        placeholderValues.put("offset", "" + offset);
        placeholderValues.put("transactionSize", "" + transactionSize);
        placeholderValues.put("newTableSeqName", "data." + getSequenceName(tableToInsert));
        placeholderValues.put("tableToInsert", "data." + addDoubleQuotes(tableToInsert));
        placeholderValues.put("columns", columnsStr);
        placeholderValues.put("columnsWithType", columnsWithType);
        String query = StrSubstitutor.replace(INSERT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);

        if (logger.isDebugEnabled()) {
            logger.debug("insertActualDataFromVersion with closeTime method query: " + query);
        }
        entityManager.createNativeQuery(
                query)
                .executeUpdate();
    }

    public BigInteger countOldDataFromVersion(String versionTable, String draftTable, Date publishTime, Date closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        return (BigInteger) entityManager.createNativeQuery(
                String.format(COUNT_OLD_VAL_FROM_VERSION_WITH_CLOSE_TIME,
                        addDoubleQuotes(versionTable),
                        addDoubleQuotes(draftTable),
                        new SimpleDateFormat(TIMESTAMP_DATE_FORMAT).format(publishTime),
                        new SimpleDateFormat(TIMESTAMP_DATE_FORMAT).format(closeTime)
                )).getSingleResult();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertOldDataFromVersion(String tableToInsert, String tableFromInsert, String draftTable, List<String> columns,
                                         int offset, int transactionSize, Date publishTime, Date closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        final SimpleDateFormat sdf = new SimpleDateFormat(TIMESTAMP_DATE_FORMAT);
        String query = String.format(INSERT_OLD_VAL_FROM_VERSION_WITH_CLOSE_DATE,
                addDoubleQuotes(tableToInsert),
                addDoubleQuotes(tableFromInsert),
                addDoubleQuotes(draftTable),
                offset,
                transactionSize,
                getSequenceName(tableToInsert),
                columnsStr,
                columnsWithPrefix,
                sdf.format(publishTime),
                sdf.format(closeTime)
        );
        if (logger.isDebugEnabled()) {
            logger.debug("insertOldDataFromVersion with closeTime method query: " + query);
        }
        entityManager.createNativeQuery(
                query).executeUpdate();
    }

    public BigInteger countClosedNowDataFromVersion(String versionTable, String draftTable, Date publishTime, Date closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        final SimpleDateFormat df = new SimpleDateFormat(TIMESTAMP_DATE_FORMAT);
        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("versionTable", "data." + addDoubleQuotes(versionTable));
        placeholderValues.put("draftTable", "data." + addDoubleQuotes(draftTable));
        placeholderValues.put("publishTime", df.format(publishTime));
        placeholderValues.put("closeTime", df.format(closeTime));
        String query = StrSubstitutor.replace(COUNT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);
        return (BigInteger) entityManager.createNativeQuery(query).getSingleResult();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertClosedNowDataFromVersion(String tableToInsert, String versionTable, String draftTable,
                                               Map<String, String> columns, int offset, int transactionSize, Date publishTime, Date closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        String columnsStr = columns.keySet().stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithType = columns.keySet().stream().map(s -> s + " " + columns.get(s)).reduce((s1, s2) -> s1 + ", " + s2).get();
        final SimpleDateFormat df = new SimpleDateFormat(TIMESTAMP_DATE_FORMAT);
        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("tableToInsert", "data." + addDoubleQuotes(tableToInsert));
        placeholderValues.put("draftTable", "data." + addDoubleQuotes(draftTable));
        placeholderValues.put("versionTable", "data." + addDoubleQuotes(versionTable));
        placeholderValues.put("publishTime", df.format(publishTime));
        placeholderValues.put("closeTime", df.format(closeTime));
        placeholderValues.put("columns", columnsStr);
        placeholderValues.put("offset", "" + offset);
        placeholderValues.put("transactionSize", "" + transactionSize);
        placeholderValues.put("columnsWithType", columnsWithType);
        placeholderValues.put("sequenceName", "data." + getSequenceName(tableToInsert));
        String query = StrSubstitutor.replace(INSERT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);
        if (logger.isDebugEnabled()) {
            logger.debug("insertClosedNowDataFromVersion with closeTime method query: " + query);
        }
        entityManager.createNativeQuery(query)
                .executeUpdate();
    }

    public BigInteger countNewValFromDraft(String draftTable, String versionTable, Date publishTime, Date closeTime) {

        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        final SimpleDateFormat df = new SimpleDateFormat(TIMESTAMP_DATE_FORMAT);
        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("draftTable", "data." + addDoubleQuotes(draftTable));
        placeholderValues.put("versionTable", "data." + addDoubleQuotes(versionTable));
        placeholderValues.put("publishTime", df.format(publishTime));
        placeholderValues.put("closeTime", df.format(closeTime));
        String query = StrSubstitutor.replace(COUNT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME, placeholderValues);
        return (BigInteger) entityManager.createNativeQuery(query).getSingleResult();

    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertNewDataFromDraft(String tableToInsert, String versionTable, String draftTable,
                                       List<String> columns, int offset, int transactionSize, Date publishTime, Date closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        final SimpleDateFormat df = new SimpleDateFormat(TIMESTAMP_DATE_FORMAT);

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("fields", columnsStr);
        placeholderValues.put("draftTable", "data." + addDoubleQuotes(draftTable));
        placeholderValues.put("versionTable", "data." + addDoubleQuotes(versionTable));
        placeholderValues.put("publishTime", df.format(publishTime));
        placeholderValues.put("closeTime", df.format(closeTime));
        placeholderValues.put("transactionSize", "" + transactionSize);
        placeholderValues.put("offset", "" + offset);
        placeholderValues.put("sequenceName", "data." + getSequenceName(tableToInsert));
        placeholderValues.put("tableToInsert", "data." + addDoubleQuotes(tableToInsert));
        placeholderValues.put("rowFields", columnsWithPrefix);
        String query = StrSubstitutor.replace(INSERT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME, placeholderValues);

        if (logger.isDebugEnabled()) {
            logger.debug("insertNewDataFromDraft with closeTime method query: " + query);
        }

        entityManager.createNativeQuery(
                query)
                .executeUpdate();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertDataFromDraft(String draftTable, int offset, String targetTable, int transactionSize, Date publishTime, Date closeTime, List<String> columns) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        final SimpleDateFormat sdf = new SimpleDateFormat(TIMESTAMP_DATE_FORMAT);
        String query = String.format(INSERT_FROM_DRAFT_TEMPLATE_WITH_CLOSE_TIME,
                addDoubleQuotes(draftTable),
                offset,
                transactionSize,
                addDoubleQuotes(targetTable),
                sdf.format(publishTime),
                sdf.format(closeTime),
                getSequenceName(targetTable),
                columnsStr,
                columnsWithPrefix);
        if (logger.isDebugEnabled()) {
            logger.debug("insertDataFromDraft with closeTime method query: " + query);
        }
        entityManager.createNativeQuery(
                query)
                .executeUpdate();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void deletePointRows(String targetTable) {
        String query = String.format(DELETE_POINT_ROWS_QUERY_TEMPLATE,
                addDoubleQuotes(targetTable));
        if (logger.isDebugEnabled()) {
            logger.debug("deletePointRows method query: " + query);
        }
        entityManager.createNativeQuery(
                query)
                .executeUpdate();
    }

    public DataDifference getDataDifference(CompareDataCriteria criteria) {
        DataDifference dataDifference;
        List<String> fields = new ArrayList<>();
        List<String> nonPrimaryFields = new ArrayList<>();
        Map<String, Field> fieldMap = new HashMap<>();
        Map<String, Object> params = new HashMap<>();
        criteria.getFields().forEach(field -> {
            fields.add(field.getName());
            if (!criteria.getPrimaryFields().contains(field.getName()))
                nonPrimaryFields.add(field.getName());
            fieldMap.put(field.getName(), field);
        });
        String oldStorage = criteria.getStorageCode();
        String newStorage = criteria.getNewStorageCode() != null ? criteria.getNewStorageCode() : criteria.getStorageCode();
        String countSelect = "SELECT count(*)";
        String dataSelect = "SELECT t1." + addDoubleQuotes(DATA_PRIMARY_COLUMN) + " as sysId1, " + generateSqlQuery("t1", criteria.getFields(), false) + ", "
                + "t2." + addDoubleQuotes(DATA_PRIMARY_COLUMN) + " as sysId2, " + generateSqlQuery("t2", criteria.getFields(), false);
        String primaryEquality = criteria.getPrimaryFields()
                .stream()
                .map(f -> formatFieldForQuery(f, "t1") + " = " + formatFieldForQuery(f, "t2"))
                .collect(Collectors.joining(" and "));
        String oldPrimaryValuesFilter = getFieldValuesFilter("t1", params, criteria.getPrimaryFieldsFilters());
        String newPrimaryValuesFilter = getFieldValuesFilter("t2", params, criteria.getPrimaryFieldsFilters());
        String nonPrimaryFieldsInequality = isEmpty(nonPrimaryFields) ? " and false " : " and (" + nonPrimaryFields
                .stream()
                .map(field -> formatFieldForQuery(field, "t1") + " != " + formatFieldForQuery(field, "t2"))
                .collect(Collectors.joining(" or ")) + ") ";
        String oldPrimaryIsNull = criteria.getPrimaryFields()
                .stream()
                .map(f -> formatFieldForQuery(f, "t1") + " is null ")
                .collect(Collectors.joining(" and "));
        String newPrimaryIsNull = criteria.getPrimaryFields()
                .stream()
                .map(f -> formatFieldForQuery(f, "t2") + " is null ")
                .collect(Collectors.joining(" and "));
        String oldVersionDateFilter = "";
        if (criteria.getOldPublishDate() != null || criteria.getOldCloseDate() != null) {
            oldVersionDateFilter = " and date_trunc('second', t1.\"SYS_PUBLISHTIME\") <= :oldPublishDate\\:\\:timestamp and date_trunc('second', t1.\"SYS_CLOSETIME\") >= :oldCloseDate\\:\\:timestamp ";
            params.put("oldPublishDate", criteria.getOldPublishDate() != null
                    ? truncateDateTo(criteria.getOldPublishDate(), ChronoUnit.SECONDS)
                    : "'-infinity'");
            params.put("oldCloseDate", criteria.getOldCloseDate() != null
                    ? truncateDateTo(criteria.getOldCloseDate(), ChronoUnit.SECONDS)
                    : PG_MAX_TIMESTAMP);
        }
        String newVersionDateFilter = "";
        if (criteria.getNewPublishDate() != null || criteria.getNewCloseDate() != null) {
            newVersionDateFilter = " and date_trunc('second', t2.\"SYS_PUBLISHTIME\") <= :newPublishDate\\:\\:timestamp and date_trunc('second', t2.\"SYS_CLOSETIME\") >= :newCloseDate\\:\\:timestamp ";
            params.put("newPublishDate", criteria.getNewPublishDate() != null
                    ? truncateDateTo(criteria.getNewPublishDate(), ChronoUnit.SECONDS)
                    : "'-infinity'");
            params.put("newCloseDate", criteria.getNewCloseDate() != null
                    ? truncateDateTo(criteria.getNewCloseDate(), ChronoUnit.SECONDS)
                    : PG_MAX_TIMESTAMP);
        }
        String joinType;
        switch (criteria.getReturnType()) {
            case NEW:
                joinType = "right";
                break;
            case OLD:
                joinType = "left";
                break;
            default:
                joinType = "full";
        }
        String query = " from data." + addDoubleQuotes(oldStorage) + " t1 " + joinType +
                " join data." + addDoubleQuotes(newStorage) + " t2 on " + primaryEquality +
                " and (true" + oldPrimaryValuesFilter + " or true" + newPrimaryValuesFilter + ")" +
                oldVersionDateFilter +
                newVersionDateFilter +
                " where ";
        if (criteria.getStatus() == null)
            query += oldPrimaryIsNull + newVersionDateFilter +
                    " or " + newPrimaryIsNull + oldVersionDateFilter +
                    " or (" + primaryEquality + nonPrimaryFieldsInequality + ") ";
        else if (DiffStatusEnum.UPDATED.equals(criteria.getStatus())) {
            query += primaryEquality + nonPrimaryFieldsInequality;
        } else if (DiffStatusEnum.INSERTED.equals(criteria.getStatus())) {
            query += oldPrimaryIsNull + newVersionDateFilter;
        } else if (DiffStatusEnum.DELETED.equals(criteria.getStatus())) {
            query += newPrimaryIsNull + oldVersionDateFilter;
        }
        QueryWithParams countQueryWithParams = new QueryWithParams(countSelect + query, params);
        Query countQuery = countQueryWithParams.createQuery(entityManager);
        BigInteger count = (BigInteger) countQuery.getSingleResult();
        if (BooleanUtils.toBoolean(criteria.getCountOnly())) {
            dataDifference = new DataDifference(new CollectionPage<>(count.intValue(), null, criteria));
        } else {
            String orderBy = " order by " +
                    criteria.getPrimaryFields()
                            .stream()
                            .map(f -> formatFieldForQuery(f, "t2"))
                            .collect(Collectors.joining(",")) + "," +
                    criteria.getPrimaryFields()
                            .stream()
                            .map(f -> formatFieldForQuery(f, "t1"))
                            .collect(Collectors.joining(","));

            QueryWithParams dataQueryWithParams = new QueryWithParams(dataSelect + query + orderBy, params);
            Query dataQuery = dataQueryWithParams.createQuery(entityManager)
                    .setFirstResult(getOffset(criteria))
                    .setMaxResults(criteria.getSize());
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

    private String getFieldValuesFilter(String alias, Map<String, Object> params, Set<List<FieldSearchCriteria>> fieldValuesFilters) {
        QueryWithParams queryWithParams = getDictionaryFilterQuery(null, fieldValuesFilters, alias);
        params.putAll(queryWithParams.getParams());

        return queryWithParams.getQuery();
    }

    private class QueryWithParams {

        private String query;
        private Map<String, Object> params;

        public QueryWithParams(String query, Map<String, Object> params) {
            this.query = query;
            this.params = params;
        }

        public void concat(QueryWithParams queryWithParams) {
            this.query = this.query + " " + queryWithParams.getQuery();
            if (this.params == null) {
                this.params = queryWithParams.getParams();
            } else if (queryWithParams.getParams() != null) {
                params.putAll(queryWithParams.getParams());
            }
        }

        public Query createQuery(EntityManager entityManager) {
            Query query = entityManager.createNativeQuery(getQuery());
            if (getParams() != null) {
                for (Map.Entry<String, Object> paramEntry : getParams().entrySet()) {
                    query = query.setParameter(paramEntry.getKey(), paramEntry.getValue());
                }
            }
            return query;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public void setParams(Map<String, Object> params) {
            this.params = params;
        }
    }

}
package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import net.n2oapp.criteria.api.CollectionPage;
import net.n2oapp.criteria.api.Sorting;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.model.DataDifference;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.Reference;
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
        List<Field> fields = new ArrayList<>(criteria.getFields());
        fields.add(0, new IntegerField(DATA_PRIMARY_COLUMN));
        QueryWithParams queryWithParams = new QueryWithParams("SELECT " + generateSqlQuery("d", fields, true) +
                " FROM data." + addDoubleQuotes(criteria.getTableName()) + " d ", null);

        queryWithParams.concat(getDataWhereClause(criteria.getBdate(), criteria.getEdate(), criteria.getCommonFilter(), criteria.getFieldFilter()));
        queryWithParams.concat(new QueryWithParams(getDictionaryDataOrderBy((!CollectionUtils.isEmpty(criteria.getSortings()) ? criteria.getSortings().get(0) : null), "d"), null));
        Query query = queryWithParams.createQuery(entityManager);
        if (criteria.getPage() > 0 && criteria.getSize() > 0)
            query.setFirstResult(getOffset(criteria))
                    .setMaxResults(criteria.getSize());
        List<Object[]> resultList = query.getResultList();
        return convertToRowValue(fields, resultList);
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

    private Map<String, String> getColumnDataTypes(String tableName) {
        List<Object[]> dataTypes = entityManager.createNativeQuery("select column_name, data_type from information_schema.columns " +
                "where table_schema='data' and table_name=:table")
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

    @Deprecated//todo избавиться
    public String getDataWhereClauseStr(Date publishDate, Date closeDate, String search, List<FieldSearchCriteria> filter) {
        String result = " 1=1 ";
        if (publishDate != null) {
            result += " and d.\"SYS_PUBLISHTIME\" <= :bdate and (d.\"SYS_CLOSETIME\" > :bdate or d.\"SYS_CLOSETIME\" is null)";
        }
        if (closeDate != null) {
            result += " and (d.\"SYS_CLOSETIME\" >= :edate or d.\"SYS_CLOSETIME\" is null)";
        }
        result += getDictionaryFilterQuery(search, filter).getQuery();
        return result;
    }

    private QueryWithParams getDataWhereClause(Date publishDate, Date closeDate, String search, List<FieldSearchCriteria> filter) {
        Map<String, Object> params = new HashMap<>();
        String result = " WHERE 1=1 ";
        if (publishDate != null) {
            result += " and d.\"SYS_PUBLISHTIME\" <= :bdate and (d.\"SYS_CLOSETIME\" > :bdate or d.\"SYS_CLOSETIME\" is null)";
            params.put("bdate", truncateDateTo(publishDate, ChronoUnit.SECONDS));
        }
        if (closeDate != null) {
            result += " and (d.\"SYS_CLOSETIME\" >= :edate or d.\"SYS_CLOSETIME\" is null)";
            params.put("edate", truncateDateTo(closeDate, ChronoUnit.SECONDS));
        }
        QueryWithParams queryWithParams = new QueryWithParams(result, params);
        queryWithParams.concat(getDictionaryFilterQuery(search, filter));
        return queryWithParams;
    }

    private QueryWithParams getDictionaryFilterQuery(String search, List<FieldSearchCriteria> filter) {
        Map<String, Object> params = new HashMap<>();
        String queryStr = "";
        if (!StringUtils.isEmpty(search)) {
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
                String formattedSearch = search.toLowerCase().replaceAll(":", "\\\\:").replaceAll("/", "\\\\/").replace(" ", "+") + "\\\\:*";
                queryStr += " and (" + escapedFtsColumn + " @@ to_tsquery(:formattedSearch) or " + escapedFtsColumn + " @@ to_tsquery('ru', :formattedSearch) or " + escapedFtsColumn + " @@ to_tsquery('ru', :original)) ";
                params.put("formattedSearch", "'" + formattedSearch + "'");
                params.put("original", "'''" + search + "''\\\\:*'");
            }
        } else if (!CollectionUtils.isEmpty(filter)) {
            for (FieldSearchCriteria searchCriteria : filter) {
                Field field = searchCriteria.getField();
                String fieldName = searchCriteria.getField().getName();
                String escapedFieldName = addDoubleQuotes(fieldName);
                if (searchCriteria.getValues() == null || searchCriteria.getValues().get(0) == null) {
                    queryStr += " and " + escapedFieldName + " is null";
                } else if (field instanceof IntegerField || field instanceof FloatField || field instanceof DateField) {
                    queryStr += " and " + escapedFieldName + " in (:" + fieldName + ")";
                    params.put(field.getName(), searchCriteria.getValues());
                } else if (field instanceof ReferenceField) {
                    queryStr += " and " + escapedFieldName + "->> 'value' in (:" + fieldName + ")";
                    params.put(field.getName(), searchCriteria.getValues().stream().map(Object::toString).collect(Collectors.toList()));
                } else if (field instanceof TreeField) {
                    if (SearchTypeEnum.LESS.equals(searchCriteria.getType())) {
                        queryStr += " and " + escapedFieldName + "@> (cast(:" + fieldName + " AS ltree[]))";
                        String v = searchCriteria.getValues().stream().map(Object::toString).collect(Collectors.joining(",", "{", "}"));
                        params.put(field.getName(), v);
                    }
                } else if (field instanceof BooleanField) {
                    if (searchCriteria.getValues().size() == 1) {
                        queryStr += " and " + escapedFieldName +
                                ((Boolean) (searchCriteria.getValues().get(0)) ? " IS TRUE " : " IS NOT TRUE");
                    }
                } else if (field instanceof StringField) {
                    if (SearchTypeEnum.LIKE.equals(searchCriteria.getType()) && searchCriteria.getValues().size() == 1) {
                        queryStr += " and lower(" + escapedFieldName + ") like :" + fieldName + "";
                        params.put(field.getName(), "%" + searchCriteria.getValues().get(0).toString().trim().toLowerCase() + "%");
                    } else {
                        queryStr += " and " + escapedFieldName + " in (:" + fieldName + ")";
                        params.put(field.getName(), searchCriteria.getValues());
                    }
                } else {
                    params.put(field.getName(), searchCriteria.getValues());

                }
            }
        }
        return new QueryWithParams(queryStr, params);
    }


    public BigInteger countData(String tableName) {
        return (BigInteger) entityManager.createNativeQuery(String.format(SELECT_COUNT_QUERY_TEMPLATE, addDoubleQuotes(tableName))).getSingleResult();
    }

    @Transactional
    public void createDraftTable(String tableName, List<Field> fields) {
        if (CollectionUtils.isEmpty(fields)) {
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
        List<String> indexes = entityManager.createNativeQuery("select indexdef from pg_indexes where tablename=? and not indexdef like '%\"SYS_HASH\"%';")
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
        if (CollectionUtils.isEmpty(data))
            return;
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
                        if (refValue.getDisplayField() != null)
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
    public void loadData(String draftCode, String sourceStorageCode, List<String> fields, Date onDate) {
        String keys = fields.stream().collect(Collectors.joining(","));
        String values = fields.stream().map(f -> "d." + f).collect(Collectors.joining(","));
        QueryWithParams queryWithParams = new QueryWithParams(String.format(COPY_QUERY_TEMPLATE, addDoubleQuotes(draftCode), keys, values,
                addDoubleQuotes(sourceStorageCode)), null);
        queryWithParams.concat(getDataWhereClause(onDate, null, null, null));
        queryWithParams.createQuery(entityManager).executeUpdate();
    }

    @Transactional
    public void updateData(String tableName, RowValue rowValue) {
        List<String> keyList = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
                    if (refValue.getDisplayField() != null) {
                        Date refDate = refValue.getDate();
                        keyList.add(addDoubleQuotes(fieldName) + String.format(
                                "=(select jsonb_build_object('value', d.%s , 'displayValue', d.%s, 'hash', d.\"SYS_HASH\") from data.%s d where d.%s=?\\:\\:" +
                                        getFieldType(refValue.getStorageCode(), refValue.getKeyField()) + ((refDate != null) ? " and %s)" : ")"),
                                addDoubleQuotes(refValue.getKeyField()),
                                addDoubleQuotes(refValue.getDisplayField()),
                                addDoubleQuotes(refValue.getStorageCode()),
                                addDoubleQuotes(refValue.getKeyField()),
                                ((refDate != null) ? getDataWhereClauseStr(refDate, null, null, null).replace(":bdate", addSingleQuotes(sdf.format(refDate))) : "")));
                    } else
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

    public boolean isFieldUnique(String storageCode, String fieldName, Date publishTime) {
        Query query = entityManager.createNativeQuery(
                "SELECT " + addDoubleQuotes(fieldName) + "\\:\\:text, COUNT(*)" +
                        " FROM data." + addDoubleQuotes(storageCode) + " d  WHERE " + getDataWhereClauseStr(publishTime, null, null, null) +
                        " GROUP BY 1" +
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
        fields.sort(String.CASE_INSENSITIVE_ORDER);
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

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void dropTrigger(String tableName) {
        String escapedTableName = addDoubleQuotes(tableName);
        entityManager.createNativeQuery(String.format(DROP_HASH_TRIGGER, escapedTableName)).executeUpdate();
        entityManager.createNativeQuery(String.format(DROP_FTS_TRIGGER, escapedTableName)).executeUpdate();
    }

    @Transactional
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createIndex(String tableName, String name, List<String> fields) {
        fields.stream().map(QueryUtil::addDoubleQuotes).collect(Collectors.joining(","));
        entityManager.createNativeQuery(String.format(CREATE_TABLE_INDEX, name,
                addDoubleQuotes(tableName),
                fields.stream().map(QueryUtil::addDoubleQuotes).collect(Collectors.joining(","))))
                .executeUpdate();
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

    public boolean ifFieldIsNotEmpty(String tableName, String fieldName) {
        return (boolean) entityManager
                .createNativeQuery(String.format(IF_FIELD_IS_NOT_EMPTY, addDoubleQuotes(tableName),
                        addDoubleQuotes(tableName),
                        addDoubleQuotes(fieldName)))
                .getSingleResult();
    }

    public BigInteger countActualDataFromVersion(String versionTable, String draftTable) {
        return (BigInteger) entityManager.createNativeQuery(
                String.format(COUNT_ACTUAL_VAL_FROM_VERSION,
                        addDoubleQuotes(versionTable),
                        addDoubleQuotes(draftTable)
                ))
                .getSingleResult();
    }

    public void insertActualDataFromVersion(String tableToInsert, String versionTableFromInsert, String draftTable,
                                            List<String> columns, int offset, int transactionSize) {
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefixValue = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefix = columns.stream().map(s -> "v." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String query = String.format(INSERT_ACTUAL_VAL_FROM_VERSION,
                addDoubleQuotes(tableToInsert),
                addDoubleQuotes(versionTableFromInsert),
                addDoubleQuotes(draftTable),
                offset,
                transactionSize,
                getSequenceName(tableToInsert),
                columnsStr,
                columnsWithPrefixValue,
                columnsWithPrefix
        );
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
                        addDoubleQuotes(versionTable))).getSingleResult();
    }

    public void insertOldDataFromVersion(String tableToInsert, String tableFromInsert, List<String> columns,
                                         int offset, int transactionSize) {
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String query = String.format(INSERT_OLD_VAL_FROM_VERSION,
                addDoubleQuotes(tableToInsert),
                addDoubleQuotes(tableFromInsert),
                offset,
                transactionSize,
                getSequenceName(tableToInsert),
                columnsStr,
                columnsWithPrefix);
        if (logger.isDebugEnabled()) {
            logger.debug("insertOldDataFromVersion method query: " + query);
        }
        entityManager.createNativeQuery(
                query).executeUpdate();
    }

    public BigInteger countClosedNowDataFromVersion(String versionTable, String draftTable) {
        return (BigInteger) entityManager.createNativeQuery(String.format(COUNT_CLOSED_NOW_VAL_FROM_VERSION,
                addDoubleQuotes(versionTable),
                addDoubleQuotes(draftTable)))
                .getSingleResult();
    }

    public void insertClosedNowDataFromVersion(String tableToInsert, String versionTable, String draftTable,
                                               List<String> columns, int offset, int transactionSize, Date publishTime) {
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String query = String.format(INSERT_CLOSED_NOW_VAL_FROM_VERSION,
                addDoubleQuotes(tableToInsert),
                addDoubleQuotes(versionTable),
                addDoubleQuotes(draftTable),
                offset,
                transactionSize,
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(publishTime),
                getSequenceName(tableToInsert),
                columnsStr,
                columnsWithPrefix);
        if (logger.isDebugEnabled()) {
            logger.debug("insertClosedNowDataFromVersion method query: " + query);
        }
        entityManager.createNativeQuery(query)
                .executeUpdate();
    }

    public BigInteger countNewValFromDraft(String draftTable, String versionTable) {
        return (BigInteger) entityManager.createNativeQuery(
                String.format(COUNT_NEW_VAL_FROM_DRAFT,
                        addDoubleQuotes(draftTable),
                        addDoubleQuotes(versionTable)))
                .getSingleResult();

    }

    public void insertNewDataFromDraft(String tableToInsert, String versionTable, String draftTable,
                                       List<String> columns, int offset, int transactionSize, Date publishTime) {
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String query = String.format(INSERT_NEW_VAL_FROM_DRAFT,
                addDoubleQuotes(draftTable),
                addDoubleQuotes(versionTable),
                offset,
                transactionSize,
                addDoubleQuotes(tableToInsert),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(publishTime),
                getSequenceName(tableToInsert),
                columnsStr,
                columnsWithPrefix);
        if (logger.isDebugEnabled()) {
            logger.debug("insertNewDataFromDraft method query: " + query);
        }
        entityManager.createNativeQuery(
                query)
                .executeUpdate();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertDataFromDraft(String draftTable, int offset, String targetTable, int transactionSize, Date publishTime, List<String> columns) {
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String query = String.format(INSERT_FROM_DRAFT_TEMPLATE,
                addDoubleQuotes(draftTable),
                offset,
                transactionSize,
                addDoubleQuotes(targetTable),
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
        List<String> fields;
        Map<String, Field> fieldMap = new HashMap<>();

        String baseStorage = criteria.getStorageCode();
        String targetStorage = criteria.getDraftCode() != null ? criteria.getDraftCode() : criteria.getStorageCode();
        String countSelect = "SELECT count(*)";
        String dataSelect = "select t1." + addDoubleQuotes(DATA_PRIMARY_COLUMN) + " as sysId1," + generateSqlQuery("t1", criteria.getFields(), false) + ", "
                + "t2." + addDoubleQuotes(DATA_PRIMARY_COLUMN) + " as sysId2, " + generateSqlQuery("t2", criteria.getTargetFields(), false);
        String primaryEquality = criteria.getPrimaryFields().stream().map(f -> formatFieldForQuery(f, "t1") + "=" + formatFieldForQuery(f, "t2")).collect(Collectors.joining(","));
        String basePrimaryIsNull = criteria.getPrimaryFields().stream().map(f -> formatFieldForQuery(f, "t1") + " is null ").collect(Collectors.joining(" and "));
        String targetPrimaryIsNull = criteria.getPrimaryFields().stream().map(f -> formatFieldForQuery(f, "t2") + " is null ").collect(Collectors.joining(" and "));
        String baseFilter = " and t1.\"SYS_PUBLISHTIME\" <= :baseDate and (t1.\"SYS_CLOSETIME\" > :baseDate or t1.\"SYS_CLOSETIME\" is null) ";
        String targetFilter = criteria.getTargetDataDate() != null ?
                " and t2.\"SYS_PUBLISHTIME\" <= :targetDate and (t2.\"SYS_CLOSETIME\" > :targetDate or t2.\"SYS_CLOSETIME\" is null) " : "";
        String joinType;
        switch (criteria.getReturnType()) {
            case NEW:
                joinType = "right";
                fields = criteria.getTargetFields().stream().map(Field::getName).collect(Collectors.toList());
                for (Field field : criteria.getTargetFields()) {
                    fieldMap.put(field.getName(), field);
                }
                break;
            case OLD:
                joinType = "left";
                fields = criteria.getFields().stream().map(Field::getName).collect(Collectors.toList());
                for (Field field : criteria.getFields()) {
                    fieldMap.put(field.getName(), field);
                }
                break;
            default:
                joinType = "full";
                fields = criteria.getFields().stream().map(Field::getName).collect(Collectors.toList());
                for (Field field : criteria.getFields()) {
                    fieldMap.put(field.getName(), field);
                }
        }
        String query = " from data." + addDoubleQuotes(baseStorage) + " t1 " + joinType +
                " join data." + addDoubleQuotes(targetStorage) + " t2 on " + primaryEquality +
                baseFilter +
                targetFilter +
                " where ";
        if (criteria.getStatus() == null)
            query += basePrimaryIsNull + targetFilter +
                    " or " + targetPrimaryIsNull + baseFilter +
                    " or (" + primaryEquality + " and t1.\"SYS_HASH\"<>t2.\"SYS_HASH\") ";
        else if (DiffStatusEnum.UPDATED.equals(criteria.getStatus())) {
            query += primaryEquality + " and t1.\"SYS_HASH\"<>t2.\"SYS_HASH\" ";
        } else if (DiffStatusEnum.INSERTED.equals(criteria.getStatus())) {
            query += basePrimaryIsNull + targetFilter;
        } else if (DiffStatusEnum.DELETED.equals(criteria.getStatus())) {
            query += targetPrimaryIsNull + baseFilter;
        }
        Query countQuery = entityManager.createNativeQuery(countSelect + query);
        countQuery.setParameter("baseDate", truncateDateTo(criteria.getBaseDataDate(), ChronoUnit.SECONDS));
        if (criteria.getTargetDataDate() != null)
            countQuery.setParameter("targetDate", truncateDateTo(criteria.getTargetDataDate(), ChronoUnit.SECONDS));
        BigInteger count = (BigInteger) countQuery.getSingleResult();
        if (BooleanUtils.toBoolean(criteria.getCountOnly())) {
            dataDifference = new DataDifference(new CollectionPage<>(count.intValue(), null, criteria));
        } else {
            String orderBy = " order by " + criteria.getPrimaryFields().stream().map(f -> formatFieldForQuery(f, "t1")).collect(Collectors.joining(",")) + "," +
                    criteria.getPrimaryFields().stream().map(f -> formatFieldForQuery(f, "t2")).collect(Collectors.joining(","));
            Query dataQuery = entityManager.createNativeQuery(dataSelect + query + orderBy)
                    .setFirstResult(getOffset(criteria))
                    .setMaxResults(criteria.getSize());
            dataQuery.setParameter("baseDate", truncateDateTo(criteria.getBaseDataDate(), ChronoUnit.SECONDS));
            if (criteria.getTargetDataDate() != null)
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

package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import net.n2oapp.criteria.api.CollectionPage;
import net.n2oapp.criteria.api.Sorting;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.platform.datastorage.temporal.CollectionUtils;
import ru.i_novus.platform.datastorage.temporal.enums.DiffReturnTypeEnum;
import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.enums.ReferenceDisplayType;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.springframework.util.CollectionUtils.isEmpty;
import static ru.i_novus.platform.datastorage.temporal.model.DataConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.*;

public class DataDao {

    private static final LocalDateTime PG_MAX_TIMESTAMP = LocalDateTime.of(294276, 12, 31, 23, 59);
    private static final DateTimeFormatter TIMESTAMP_DATE_FORMATTER = DateTimeFormatter.ofPattern(TIMESTAMP_DATE_FORMAT);

    private static final Logger logger = LoggerFactory.getLogger(DataDao.class);
    private static final Pattern dataRegexp = Pattern.compile("([0-9]{2})\\.([0-9]{2})\\.([0-9]{4})");

    private EntityManager entityManager;

    public DataDao(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    private String formatDateTime(LocalDateTime localDateTime) {
        return (localDateTime != null) ? localDateTime.format(TIMESTAMP_DATE_FORMATTER) : null;
    }

    @SuppressWarnings("unchecked")
    public List<RowValue> getData(DataCriteria criteria) {

        List<Field> fields = new ArrayList<>(criteria.getFields());
        fields.add(0, new IntegerField(SYS_PRIMARY_COLUMN));
        if (fields.stream().noneMatch(field -> SYS_HASH.equals(field.getName())))
            fields.add(1, new StringField(SYS_HASH));

        final String sqlFormat = "SELECT %1$s FROM %2$s as %3$s ";
        String keys = generateSqlQuery(QueryConstants.DEFAULT_TABLE_ALIAS, fields, true);
        String sql = String.format(sqlFormat, keys,
                getSchemeTableName(criteria.getTableName()), QueryConstants.DEFAULT_TABLE_ALIAS);
        QueryWithParams queryWithParams = new QueryWithParams(sql, null);

        QueryWithParams dataWhereClause;
        if (isEmpty(criteria.getHashList())) {
            dataWhereClause = getDataWhereClause(criteria.getBdate(), criteria.getEdate(),
                    criteria.getCommonFilter(), criteria.getFieldFilter(), criteria.getSystemIds());
        } else {
            dataWhereClause = getDataWhereClause(criteria.getBdate(), criteria.getEdate(), criteria.getCommonFilter(),
                    singleton(singletonList(new FieldSearchCriteria(new StringField(SYS_HASH), SearchTypeEnum.EXACT, criteria.getHashList()))));
        }
        queryWithParams.concat(dataWhereClause);

        queryWithParams.concat(new QueryWithParams(
                getDictionaryDataOrderBy((!CollectionUtils.isNullOrEmpty(criteria.getSortings()) ? criteria.getSortings().get(0) : null), "d"),
                null
        ));

        Query query = queryWithParams.createQuery(entityManager);
        if (criteria.getPage() >= DataCriteria.MIN_PAGE
                && criteria.getSize() >= DataCriteria.MIN_SIZE) {
            query.setFirstResult(getOffset(criteria)).setMaxResults(criteria.getSize());
        }

        List<Object[]> resultList = query.getResultList();
        return convertToRowValue(fields, resultList);
    }

    public List<String> getNotExists(String tableName, LocalDateTime bdate, LocalDateTime edate, List<String> hashList) {

        Map<String, Object> params = new HashMap<>();
        String sqlHashArray = "array[" + hashList.stream().map(hash -> {
            String hashPlaceHolder = "hash" + params.size();
            params.put(hashPlaceHolder, hash);
            return ":" + hashPlaceHolder;
        }).collect(joining(",")) + "]";

        QueryWithParams dataWhereClause = getDataWhereClause(bdate, edate, null, null);
        String query = "SELECT hash FROM (" +
                "SELECT unnest(" + sqlHashArray + ") hash) hashes WHERE hash NOT IN (" +
                "SELECT " + addDoubleQuotes(SYS_HASH) + " FROM " + getSchemeTableName(tableName) + " as d " +
                dataWhereClause.getQuery() + ")";
        params.putAll(dataWhereClause.params);

        QueryWithParams queryWithParams = new QueryWithParams(query, params);
        return queryWithParams.createQuery(entityManager).getResultList();
    }

    /** Получение строки данных таблицы по системному идентификатору. */
    public RowValue getRowData(String tableName, List<String> fieldNames, Object systemId) {

        Map<String, String> dataTypes = getColumnDataTypes(tableName);
        List<Field> fields = new ArrayList<>(fieldNames.size());
        fields.add(new IntegerField(SYS_PRIMARY_COLUMN));
        fields.add(new StringField(SYS_HASH));
        for (Map.Entry<String, String> entry : dataTypes.entrySet()) {
            String fieldName = entry.getKey();
            if (fieldNames.contains(fieldName)) {
                fields.add(getField(fieldName, entry.getValue()));
            }
        }

        String keys = generateSqlQuery(null, fields, true);
        String sql = String.format(SELECT_ROWS_FROM_DATA_BY_FIELD, keys,
                addDoubleQuotes(tableName), addDoubleQuotes(SYS_PRIMARY_COLUMN), QUERY_VALUE_SUBST);

        @SuppressWarnings("unchecked")
        List<Object[]> list = entityManager.createNativeQuery(sql)
                .setParameter(1, systemId)
                .getResultList();
        if (list.isEmpty())
            return null;

        RowValue row = convertToRowValue(fields, list).get(0);
        row.setSystemId(systemId); // ??
        return row;
    }

    /** Получение строк данных таблицы по системным идентификаторам. */
    public List<RowValue> getRowData(String tableName, List<String> fieldNames, List<Object> systemIds) {

        Map<String, String> dataTypes = getColumnDataTypes(tableName);
        List<Field> fields = new ArrayList<>(fieldNames.size());
        fields.add(new IntegerField(SYS_PRIMARY_COLUMN));
        fields.add(new StringField(SYS_HASH));
        for (Map.Entry<String, String> entry : dataTypes.entrySet()) {
            String fieldName = entry.getKey();
            if (fieldNames.contains(fieldName)) {
                fields.add(getField(fieldName, entry.getValue()));
            }
        }

        String keys = generateSqlQuery(null, fields, true);
        String sql = String.format(SELECT_ROWS_FROM_DATA_BY_FIELD_ALL, keys,
                addDoubleQuotes(tableName), addDoubleQuotes(SYS_PRIMARY_COLUMN), QUERY_VALUE_SUBST);
        Query query = entityManager.createNativeQuery(sql);

        String ids = systemIds.stream().map(String::valueOf).collect(joining(","));
        query.setParameter(1, "{" + ids + "}");

        @SuppressWarnings("unchecked")
        List<Object[]> list = query.getResultList();
        return !list.isEmpty() ? convertToRowValue(fields, list) : emptyList();
    }

    public boolean tableStructureEquals(String tableName1, String tableName2) {
        Map<String, String> dataTypes1 = getColumnDataTypes(tableName1);
        Map<String, String> dataTypes2 = getColumnDataTypes(tableName2);
        return dataTypes1.equals(dataTypes2);
    }

    public Map<String, String> getColumnDataTypes(String tableName) {

        @SuppressWarnings("unchecked")
        List<Object[]> dataTypes = entityManager.createNativeQuery(SELECT_FIELD_NAMES_AND_TYPES)
                .setParameter("table", tableName)
                .getResultList();

        Map<String, String> map = new HashMap<>();
        for (Object[] dataType : dataTypes) {
            String fieldName = (String) dataType[0];
            if (!systemFieldList().contains(fieldName))
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
        return orderBy + spaceAliasPoint + addDoubleQuotes(SYS_PRIMARY_COLUMN);
    }

    public BigInteger getDataCount(DataCriteria criteria) {
        QueryWithParams queryWithParams = new QueryWithParams(SELECT_COUNT_ONLY +
                "  FROM " + getSchemeTableName(criteria.getTableName()) + " as d ", null);
        queryWithParams.concat(getDataWhereClause(criteria.getBdate(), criteria.getEdate(),
                criteria.getCommonFilter(), criteria.getFieldFilter(), criteria.getSystemIds()));
        return (BigInteger) queryWithParams.createQuery(entityManager).getSingleResult();
    }

    /**
     * @deprecated
     */
    @Deprecated
    public String getDataWhereClauseStr(LocalDateTime publishDate, LocalDateTime closeDate, String search, Set<List<FieldSearchCriteria>> filter) {
        String result = " 1=1 ";
        if (publishDate != null) {
            result += " and date_trunc('second', d.\"SYS_PUBLISHTIME\") <= :bdate and (date_trunc('second', d.\"SYS_CLOSETIME\") > :bdate or d.\"SYS_CLOSETIME\" is null)";
        }
        if (closeDate != null) {
            result += " and (date_trunc('second', d.\"SYS_CLOSETIME\") >= :edate or d.\"SYS_CLOSETIME\" is null)";
        }
        result += getDictionaryFilterQuery(search, filter, emptyList(), null).getQuery();
        return result;
    }

    private QueryWithParams getDataWhereClause(LocalDateTime publishDate, LocalDateTime closeDate, String search, Set<List<FieldSearchCriteria>> filters) {
        return getDataWhereClause(publishDate, closeDate, search, filters, emptyList());
    }

    private QueryWithParams getDataWhereClause(LocalDateTime publishDate, LocalDateTime closeDate, String search,
                                               Set<List<FieldSearchCriteria>> filters, List<Long> rowSystemIds) {

        closeDate = closeDate == null ? PG_MAX_TIMESTAMP : closeDate;
        Map<String, Object> params = new HashMap<>();
        String result = " WHERE 1=1 ";
        if (publishDate != null) {
            result += " and date_trunc('second', d.\"SYS_PUBLISHTIME\") <= :bdate and (date_trunc('second', d.\"SYS_CLOSETIME\") > :bdate or d.\"SYS_CLOSETIME\" is null)";
            params.put("bdate", publishDate.truncatedTo(ChronoUnit.SECONDS));
            result += " and (date_trunc('second', d.\"SYS_CLOSETIME\") >= :edate or d.\"SYS_CLOSETIME\" is null)";
            params.put("edate", closeDate.truncatedTo(ChronoUnit.SECONDS));
        }

        QueryWithParams queryWithParams = new QueryWithParams(result, params);
        queryWithParams.concat(getDictionaryFilterQuery(search, filters, rowSystemIds, null));
        return queryWithParams;
    }

    private QueryWithParams getDictionaryFilterQuery(String search, Set<List<FieldSearchCriteria>> filters, List<Long> systemIds, String alias) {

        Map<String, Object> params = new HashMap<>();
        String queryStr = "";
        if (!StringUtils.isEmpty(search)) {
            //full text search
            search = search.trim();
            String escapedFtsColumn = addDoubleQuotes(SYS_FULL_TEXT_SEARCH);
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
        } else if (!CollectionUtils.isNullOrEmpty(filters)) {
            filters = preprocessFilters(filters);
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
                            String v = searchCriteria.getValues().stream()
                                    .map(Object::toString)
                                    .collect(joining(",", "{", "}"));
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

            }).collect(joining(" or "));

            if (!"".equals(queryStr))
                queryStr = " and (" + queryStr + ")";
        }

        if (!isEmpty(systemIds)) {
            queryStr += " and (\"" + SYS_PRIMARY_COLUMN + "\" in (:systemIds))";
            params.put("systemIds", systemIds);
        }

        return new QueryWithParams(queryStr, params);
    }

    private Set<List<FieldSearchCriteria>> preprocessFilters(Set<List<FieldSearchCriteria>> filters) {
        Set<List<FieldSearchCriteria>> set = new HashSet<>();
        for (List<FieldSearchCriteria> list : filters) {
            set.add(groupByFieldAndSearchTypeEnum(list));
        }
        return set;
    }

    private List<FieldSearchCriteria> groupByFieldAndSearchTypeEnum(List<FieldSearchCriteria> list) {
        EnumMap<SearchTypeEnum, Map<String, FieldSearchCriteria>> group = new EnumMap<>(SearchTypeEnum.class);
        for (FieldSearchCriteria criteria : list) {
            Map<String, FieldSearchCriteria> map = group.computeIfAbsent(criteria.getType(), k -> new HashMap<>());
            FieldSearchCriteria c = map.get(criteria.getField().getName());
            if (c == null) {
                criteria.setValues(new ArrayList<>(criteria.getValues()));
                map.put(criteria.getField().getName(), criteria);
            } else {
                List<Object> unchecked = (List<Object>) c.getValues();
                unchecked.addAll(criteria.getValues());
            }
        }
        return group.values().stream().map(Map::values).flatMap(Collection::stream).collect(toList());
    }

    public BigInteger countData(String tableName) {
        return (BigInteger) entityManager.createNativeQuery(String.format(SELECT_COUNT_QUERY_TEMPLATE, addDoubleQuotes(tableName))).getSingleResult();
    }

    @Transactional
    public void createDraftTable(String tableName, List<Field> fields) {
        if (CollectionUtils.isNullOrEmpty(fields)) {
            entityManager.createNativeQuery(String.format(CREATE_EMPTY_DRAFT_TABLE_TEMPLATE, addDoubleQuotes(tableName), tableName)).executeUpdate();
        } else {
            String fieldsString = fields.stream().map(f -> addDoubleQuotes(f.getName()) + " " + f.getType()).collect(joining(", "));
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
        for (Object fieldValue : data.iterator().next().getFieldValues()) {
            keys.add(addDoubleQuotes(((FieldValue) fieldValue).getField()));
        }

        List<String> values = new ArrayList<>();
        for (RowValue rowValue : data) {
            List<String> rowValues = new ArrayList<>(rowValue.getFieldValues().size());
            for (Object fieldValueObj : rowValue.getFieldValues()) {
                FieldValue fieldValue = (FieldValue) fieldValueObj;
                if (fieldValue.getValue() == null) {
                    rowValues.add(QUERY_NULL_VALUE);
                } else if (fieldValue instanceof ReferenceFieldValue) {
                    rowValues.add(getReferenceValuationSelect((ReferenceFieldValue) fieldValue, QUERY_VALUE_SUBST));
                } else if (fieldValue instanceof TreeFieldValue) {
                    rowValues.add(QUERY_LTREE_SUBST);
                } else {
                    rowValues.add(QUERY_VALUE_SUBST);
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
            String stringValues = String.join("),(", subValues);
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
    public void loadData(String draftCode, String sourceStorageCode, List<String> fields, LocalDateTime fromDate, LocalDateTime toDate ) {
        String keys = String.join(",", fields);
        String values = fields.stream().map(f -> "d." + f).collect(joining(","));

        QueryWithParams queryWithParams = new QueryWithParams(String.format(COPY_QUERY_TEMPLATE, addDoubleQuotes(draftCode), keys, values,
                addDoubleQuotes(sourceStorageCode)), null);
        queryWithParams.concat(getDataWhereClause(fromDate, toDate, null, null));
        queryWithParams.createQuery(entityManager).executeUpdate();
    }

    /**
     * Формирование текста запроса для получения значения ссылочного поля.
     *
     * Используется при вставке или обновлении записи в таблицу,
     * а также при обновлении значения ссылочного поля.
     *
     * @param fieldValue значение ссылочного поля
     * @return Текст запроса или {@value QueryConstants#QUERY_NULL_VALUE}
     */
    private String getReferenceValuationSelect(ReferenceFieldValue fieldValue, String valueSubst) {
        Reference refValue = fieldValue.getValue();
        if (refValue.getValue() == null && QUERY_NULL_VALUE.equals(valueSubst))
            return QUERY_NULL_VALUE;

        ReferenceDisplayType displayType = getReferenceDisplayType(refValue);
        if (displayType == null)
            return "(" + REFERENCE_VALUATION_SELECT_SUBST + ")";

        // NB: Replace getDataWhereClauseStr by simplified one.
        String sqlExpression;
        switch (displayType) {
            case DISPLAY_EXPRESSION:
                sqlExpression = sqlDisplayExpression(refValue.getDisplayExpression(), REFERENCE_VALUATION_SELECT_TABLE);
                break;

            case DISPLAY_FIELD:
                sqlExpression = sqlFieldExpression(refValue.getDisplayField(), REFERENCE_VALUATION_SELECT_TABLE);
                break;

            default:
                throw new UnsupportedOperationException("unknown.reference.dipslay.type");
        }

        String valueSelect = String.format(REFERENCE_VALUATION_SELECT_EXPRESSION,
                addDoubleQuotes(refValue.getKeyField()),
                sqlExpression,
                addDoubleQuotes(refValue.getStorageCode()),
                valueSubst,
                getFieldType(refValue.getStorageCode(), refValue.getKeyField()),
                getDataWhereClauseStr(refValue.getDate(), null, null, null)
                        .replace(":bdate", addSingleQuotes(formatDateTime(refValue.getDate())))
        );

        return "(" + valueSelect + ")";
    }

    @Transactional
    public void updateData(String tableName, RowValue rowValue) {
        List<String> keyList = new ArrayList<>();
        for (Object objectValue : rowValue.getFieldValues()) {
            FieldValue fieldValue = (FieldValue) objectValue;
            String quotedFieldName = addDoubleQuotes(fieldValue.getField());
            if (isFieldValueNull(fieldValue)) {
                keyList.add(quotedFieldName + " = " + QUERY_NULL_VALUE);
            } else if (fieldValue instanceof ReferenceFieldValue) {
                keyList.add(quotedFieldName + " = " +
                        getReferenceValuationSelect((ReferenceFieldValue) fieldValue, QUERY_VALUE_SUBST));
            } else if (fieldValue instanceof TreeFieldValue) {
                keyList.add(quotedFieldName + " = " + QUERY_LTREE_SUBST);
            } else {
                keyList.add(quotedFieldName + " = " + QUERY_VALUE_SUBST);
            }
        }

        String keys = String.join(",", keyList);
        Query query = entityManager.createNativeQuery(String.format(UPDATE_QUERY_TEMPLATE, addDoubleQuotes(tableName), keys, QUERY_VALUE_SUBST));

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
        String ids = systemIds.stream().map(id -> "?").collect(joining(","));
        Query query = entityManager.createNativeQuery(String.format(DELETE_QUERY_TEMPLATE, addDoubleQuotes(tableName), ids));
        int i = 1;
        for (Object systemId : systemIds) {
            query.setParameter(i++, systemId);
        }
        query.executeUpdate();
    }

    @Transactional
    public void updateReferenceInRows(String tableName, ReferenceFieldValue fieldValue, List<Object> systemIds) {

        if (getReferenceDisplayType(fieldValue.getValue()) == null)
            return;

        String quotedFieldName = addDoubleQuotes(fieldValue.getField());
        String oldFieldExpression = sqlFieldExpression(fieldValue.getField(), REFERENCE_VALUATION_UPDATE_TABLE);
        String oldFieldValue = String.format(REFERENCE_VALUATION_OLD_VALUE, oldFieldExpression);
        String key = quotedFieldName + " = " + getReferenceValuationSelect(fieldValue, oldFieldValue);

        Query query = entityManager.createNativeQuery(String.format(UPDATE_REFERENCE_QUERY_TEMPLATE, addDoubleQuotes(tableName), key, QUERY_VALUE_SUBST));

        String ids = systemIds.stream().map(String::valueOf).collect(joining(","));
        query.setParameter(1, "{" + ids + "}");

        query.executeUpdate();
    }

    public BigInteger countReferenceInRefRows(String tableName, ReferenceFieldValue fieldValue) {

        if (getReferenceDisplayType(fieldValue.getValue()) == null)
            return BigInteger.ZERO;

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("versionTable", getSchemeTableName(tableName));
        placeholderValues.put("refFieldName", addDoubleQuotes(fieldValue.getField()));

        String query = StrSubstitutor.replace(COUNT_REFERENCE_IN_REF_ROWS, placeholderValues);
        BigInteger count = (BigInteger) entityManager.createNativeQuery(query).getSingleResult();

        if (logger.isDebugEnabled()) {
            logger.debug("countReferenceInRefRows method count: {}, query: {}", count, query);
        }

        return count;
    }

    @Transactional
    public void updateReferenceInRefRows(String tableName, ReferenceFieldValue fieldValue, int offset, int limit) {

        String quotedFieldName = addDoubleQuotes(fieldValue.getField());
        String oldFieldExpression = sqlFieldExpression(fieldValue.getField(), REFERENCE_VALUATION_UPDATE_TABLE);
        String oldFieldValue = String.format(REFERENCE_VALUATION_OLD_VALUE, oldFieldExpression);
        String key = quotedFieldName + " = " + getReferenceValuationSelect(fieldValue, oldFieldValue);

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("versionTable", getSchemeTableName(tableName));
        placeholderValues.put("refFieldName", addDoubleQuotes(fieldValue.getField()));
        placeholderValues.put("limit", "" + limit);
        placeholderValues.put("offset", "" + offset);

        String where = StrSubstitutor.replace(WHERE_REFERENCE_IN_REF_ROWS, placeholderValues);
        String query = String.format(UPDATE_QUERY_TEMPLATE, addDoubleQuotes(tableName), key, where);

        if (logger.isDebugEnabled()) {
            logger.debug("updateReferenceInRefRows method query: {}", query);
        }

        entityManager.createNativeQuery(query)
                .executeUpdate();
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

    public boolean isUnique(String storageCode, List<String> fieldNames, LocalDateTime publishTime) {
        String fields = fieldNames.stream().map(fieldName -> addDoubleQuotes(fieldName) + "\\:\\:text")
                .collect(joining(","));
        String groupBy = Stream.iterate(1, n -> n + 1).limit(fieldNames.size()).map(String::valueOf)
                .collect(joining(","));

        Query query = entityManager.createNativeQuery(
                "SELECT " + fields + ", COUNT(*)" +
                        " FROM " + getSchemeTableName(storageCode) + " as d" +
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
        String query = String.format("SELECT setval('%1$s.%2$s', (SELECT max(\"SYS_RECORDID\") FROM %1$s.%3$s))",
                DATA_SCHEME_NAME, getSequenceName(tableName), addDoubleQuotes(tableName));
        entityManager.createNativeQuery(query).getSingleResult();
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createTrigger(String tableName) {
        createTrigger(tableName, getHashUsedFieldNames(tableName));
    }

    private String getFieldClearName(String field) {
        return field.substring(0, field.indexOf('"', 1) + 1);
    }

    @Transactional
    public void createTrigger(String tableName, List<String> fields) {
        String escapedTableName = addDoubleQuotes(tableName);
        String tableFields = fields.stream().map(this::getFieldClearName).collect(joining(", "));
        entityManager.createNativeQuery(String.format(CREATE_HASH_TRIGGER,
                tableName,
                fields.stream().map(field -> "NEW." + field).collect(joining(", ")),
                tableFields,
                escapedTableName,
                tableName)).executeUpdate();
        entityManager.createNativeQuery(String.format(CREATE_FTS_TRIGGER,
                tableName,
                fields.stream()
                        .map(field -> "coalesce( to_tsvector('ru', NEW." + field + "\\:\\:text),'')")
                        .collect(joining(" || ' ' || ")),
                tableFields,
                escapedTableName,
                tableName)).executeUpdate();
    }

    @Transactional
    public void updateHashRows(String tableName) {
        List<String> fieldNames = getHashUsedFieldNames(tableName);
        entityManager.createNativeQuery(String.format(UPDATE_HASH,
                addDoubleQuotes(tableName),
                fieldNames.stream().collect(joining(", ")))).executeUpdate();
    }

    @Transactional
    public void updateFtsRows(String tableName) {
        List<String> fieldNames = getHashUsedFieldNames(tableName);
        entityManager.createNativeQuery(String.format(UPDATE_FTS,
                addDoubleQuotes(tableName),
                fieldNames.stream().map(field -> "coalesce( to_tsvector('ru', " + field + "\\:\\:text),'')")
                        .collect(joining(" || ' ' || ")))).executeUpdate();
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
        entityManager.createNativeQuery(String.format(CREATE_TABLE_INDEX, name,
                addDoubleQuotes(tableName),
                fields.stream().map(QueryUtil::addDoubleQuotes).collect(joining(","))))
                .executeUpdate();
    }

    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createFullTextSearchIndex(String tableName) {
        entityManager.createNativeQuery(String.format(CREATE_FTS_INDEX, addDoubleQuotes(tableName + "_fts_idx"),
                addDoubleQuotes(tableName),
                addDoubleQuotes(SYS_FULL_TEXT_SEARCH))).executeUpdate();
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

    public List<String> getFieldNames(String tableName, String sqlFieldNames) {
        List<String> results = entityManager.createNativeQuery(String.format(sqlFieldNames, tableName)).getResultList();
        Collections.sort(results);
        return results;
    }

    public List<String> getFieldNames(String tableName) {
        return getFieldNames(tableName, SELECT_FIELD_NAMES);
    }

    public List<String> getHashUsedFieldNames(String tableName) {
        return getFieldNames(tableName, SELECT_HASH_USED_FIELD_NAMES);
    }

    public String getFieldType(String tableName, String field) {
        return entityManager.createNativeQuery(String.format(SELECT_FIELD_TYPE, tableName, field)).getSingleResult().toString();
    }

    public void alterDataType(String tableName, String field, String oldType, String newType) {
        String escapedField = addDoubleQuotes(field);
        String using = "";
        if (DateField.TYPE.equals(oldType) && isVarcharType(newType)) {
            using = "to_char(" + escapedField + ", '" + DATE_FORMAT_FOR_USING_CONVERTING + "')";
        } else if (DateField.TYPE.equals(newType) && StringField.TYPE.equals(oldType)) {
            using = "to_date(" + escapedField + ", '" + DATE_FORMAT_FOR_USING_CONVERTING + "')";
        } else if (ReferenceField.TYPE.equals(oldType)) {
            using = "(" + escapedField + "->>'value')" + "\\:\\:varchar\\:\\:" + newType;
        } else if (ReferenceField.TYPE.equals(newType)) {
            using = "nullif(jsonb_build_object('value'," + escapedField + "),jsonb_build_object('value',null))";
        } else if (isVarcharType(oldType) || isVarcharType(newType)) {
            using = escapedField + "\\:\\:" + newType;
        } else {
            using = escapedField + "\\:\\:varchar\\:\\:" + newType;
        }

        entityManager.createNativeQuery(String.format(ALTER_COLUMN_WITH_USING, addDoubleQuotes(tableName),
                escapedField, newType, using)).executeUpdate();
    }

    public List getRowsByField(String tableName, String field, Object uniqueValue, boolean existDateColumns, LocalDateTime begin, LocalDateTime end, Object id) {
        String query = SELECT_ROWS_FROM_DATA_BY_FIELD;
        String rows = addDoubleQuotes(field);

        if (existDateColumns) {
            rows += "," + addDoubleQuotes(DATE_BEGIN) + "," + addDoubleQuotes(DATE_END);
            query += "and (coalesce(\"DATEBEG\",'-infinity'\\:\\:timestamp without time zone), coalesce(\"DATEEND\",'infinity'\\:\\:timestamp without time zone)) overlaps ";
            if (begin != null) {
                query += "((to_date('" + formatDateTime(begin) + "','dd.MM.yyyy') - integer '1'),";
            } else {
                query += "('-infinity'\\:\\:timestamp without time zone,";
            }
            if (end != null) {
                query += "(to_date('" + formatDateTime(end) + "','dd.MM.yyyy') + integer '1'))";
            } else {
                query += "'infinity'\\:\\:timestamp without time zone)";
            }
        }
        if (id != null) {
            query += " and " + addDoubleQuotes(SYS_PRIMARY_COLUMN) + " != " + id;
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

    public BigInteger countActualDataFromVersion(String versionTable, String draftTable,
                                                 LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("draftTable", getSchemeTableName(draftTable));
        placeholderValues.put("versionTable", getSchemeTableName(versionTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));

        String query = StrSubstitutor.replace(COUNT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);
        return (BigInteger) entityManager.createNativeQuery(query).getSingleResult();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertActualDataFromVersion(String tableToInsert, String versionTable, String draftTable,
                                            Map<String, String> columns, int offset, int transactionSize,
                                            LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String columnsStr = columns.keySet().stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithType = columns.keySet().stream().map(s -> s + " " + columns.get(s)).reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefixValue = columns.keySet().stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefixD = columns.keySet().stream().map(s -> "d." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("dColumns", columnsWithPrefixD);
        placeholderValues.put("vValues", columnsWithPrefixValue);
        placeholderValues.put("draftTable", getSchemeTableName(draftTable));
        placeholderValues.put("versionTable", getSchemeTableName(versionTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));
        placeholderValues.put("offset", "" + offset);
        placeholderValues.put("transactionSize", "" + transactionSize);
        placeholderValues.put("newTableSeqName", getSchemeSequenceName(tableToInsert));
        placeholderValues.put("tableToInsert", getSchemeTableName(tableToInsert));
        placeholderValues.put("columns", columnsStr);
        placeholderValues.put("columnsWithType", columnsWithType);

        String query = StrSubstitutor.replace(INSERT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);
        if (logger.isDebugEnabled()) {
            logger.debug("insertActualDataFromVersion with closeTime method query: {}", query);
        }
        entityManager.createNativeQuery(query)
                .executeUpdate();
    }

    public BigInteger countOldDataFromVersion(String versionTable, String draftTable, LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        return (BigInteger) entityManager.createNativeQuery(
                String.format(COUNT_OLD_VAL_FROM_VERSION_WITH_CLOSE_TIME,
                        addDoubleQuotes(versionTable),
                        addDoubleQuotes(draftTable),
                        formatDateTime(publishTime),
                        formatDateTime(closeTime)
                )).getSingleResult();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertOldDataFromVersion(String tableToInsert, String tableFromInsert, String draftTable, List<String> columns,
                                         int offset, int transactionSize, LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();

        String query = String.format(INSERT_OLD_VAL_FROM_VERSION_WITH_CLOSE_DATE,
                addDoubleQuotes(tableToInsert),
                addDoubleQuotes(tableFromInsert),
                addDoubleQuotes(draftTable),
                offset,
                transactionSize,
                getSequenceName(tableToInsert),
                columnsStr,
                columnsWithPrefix,
                formatDateTime(publishTime),
                formatDateTime(closeTime)
        );

        if (logger.isDebugEnabled()) {
            logger.debug("insertOldDataFromVersion with closeTime method query: {}", query);
        }
        entityManager.createNativeQuery(query)
                .executeUpdate();
    }

    public BigInteger countClosedNowDataFromVersion(String versionTable, String draftTable, LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("versionTable", getSchemeTableName(versionTable));
        placeholderValues.put("draftTable", getSchemeTableName(draftTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));
        String query = StrSubstitutor.replace(COUNT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);
        return (BigInteger) entityManager.createNativeQuery(query).getSingleResult();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertClosedNowDataFromVersion(String tableToInsert, String versionTable, String draftTable,
                                               Map<String, String> columns, int offset, int transactionSize, LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        String columnsStr = columns.keySet().stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithType = columns.keySet().stream().map(s -> s + " " + columns.get(s)).reduce((s1, s2) -> s1 + ", " + s2).get();

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("tableToInsert", getSchemeTableName(tableToInsert));
        placeholderValues.put("draftTable", getSchemeTableName(draftTable));
        placeholderValues.put("versionTable", getSchemeTableName(versionTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));
        placeholderValues.put("columns", columnsStr);
        placeholderValues.put("offset", "" + offset);
        placeholderValues.put("transactionSize", "" + transactionSize);
        placeholderValues.put("columnsWithType", columnsWithType);
        placeholderValues.put("sequenceName", getSchemeSequenceName(tableToInsert));

        String query = StrSubstitutor.replace(INSERT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);
        if (logger.isDebugEnabled()) {
            logger.debug("insertClosedNowDataFromVersion with closeTime method query: {}", query);
        }
        entityManager.createNativeQuery(query)
                .executeUpdate();
    }

    public BigInteger countNewValFromDraft(String draftTable, String versionTable, LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("draftTable", getSchemeTableName(draftTable));
        placeholderValues.put("versionTable", getSchemeTableName(versionTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));

        String query = StrSubstitutor.replace(COUNT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME, placeholderValues);
        return (BigInteger) entityManager.createNativeQuery(query).getSingleResult();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertNewDataFromDraft(String tableToInsert, String versionTable, String draftTable,
                                       List<String> columns, int offset, int transactionSize, LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("fields", columnsStr);
        placeholderValues.put("draftTable", getSchemeTableName(draftTable));
        placeholderValues.put("versionTable", getSchemeTableName(versionTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));
        placeholderValues.put("transactionSize", "" + transactionSize);
        placeholderValues.put("offset", "" + offset);
        placeholderValues.put("sequenceName", getSchemeSequenceName(tableToInsert));
        placeholderValues.put("tableToInsert", getSchemeTableName(tableToInsert));
        placeholderValues.put("rowFields", columnsWithPrefix);
        String query = StrSubstitutor.replace(INSERT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME, placeholderValues);

        if (logger.isDebugEnabled()) {
            logger.debug("insertNewDataFromDraft with closeTime method query: {}", query);
        }

        entityManager.createNativeQuery(query)
                .executeUpdate();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertDataFromDraft(String draftTable, int offset, String targetTable, int transactionSize, LocalDateTime publishTime, LocalDateTime closeTime, List<String> columns) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String query = String.format(INSERT_FROM_DRAFT_TEMPLATE_WITH_CLOSE_TIME,
                addDoubleQuotes(draftTable),
                offset,
                transactionSize,
                addDoubleQuotes(targetTable),
                formatDateTime(publishTime),
                formatDateTime(closeTime),
                getSequenceName(targetTable),
                columnsStr,
                columnsWithPrefix);

        if (logger.isDebugEnabled()) {
            logger.debug("insertDataFromDraft with closeTime method query: {}", query);
        }

        entityManager.createNativeQuery(query)
                .executeUpdate();
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void deletePointRows(String targetTable) {
        String query = String.format(DELETE_POINT_ROWS_QUERY_TEMPLATE,
                addDoubleQuotes(targetTable));

        if (logger.isDebugEnabled()) {
            logger.debug("deletePointRows method query: {}", query);
        }

        entityManager.createNativeQuery(query)
                .executeUpdate();
    }

    public DataDifference getDataDifference(CompareDataCriteria criteria) {

        List<String> fields = new ArrayList<>();
        Map<String, Field> fieldMap = new HashMap<>();
        List<String> nonPrimaryFields = new ArrayList<>();
        criteria.getFields().forEach(field -> {
            fields.add(field.getName());
            fieldMap.put(field.getName(), field);

            if (!criteria.getPrimaryFields().contains(field.getName()))
                nonPrimaryFields.add(field.getName());
        });

        String oldStorage = criteria.getStorageCode();
        String newStorage = criteria.getNewStorageCode() != null ? criteria.getNewStorageCode() : criteria.getStorageCode();
        String countSelect = "SELECT count(*)";
        String dataSelect = "SELECT t1." + addDoubleQuotes(SYS_PRIMARY_COLUMN) + " as sysId1, " + generateSqlQuery("t1", criteria.getFields(), false) + ", "
                + "t2." + addDoubleQuotes(SYS_PRIMARY_COLUMN) + " as sysId2, " + generateSqlQuery("t2", criteria.getFields(), false);
        String primaryEquality = criteria.getPrimaryFields()
                .stream()
                .map(f -> formatFieldForQuery(f, "t1") + " = " + formatFieldForQuery(f, "t2"))
                .collect(joining(" and "));

        Map<String, Object> params = new HashMap<>();
        String oldPrimaryValuesFilter = getFieldValuesFilter("t1", params, criteria.getPrimaryFieldsFilters());
        String newPrimaryValuesFilter = getFieldValuesFilter("t2", params, criteria.getPrimaryFieldsFilters());

        String nonPrimaryFieldsInequality = isEmpty(nonPrimaryFields)
                ? " and false "
                : " and (" + nonPrimaryFields.stream()
                        .map(field ->
                                formatFieldForQuery(field, "t1") + " is distinct from " + formatFieldForQuery(field, "t2")
                        )
                        .collect(joining(" or ")) +
                        ") ";

        String oldPrimaryIsNull = criteria.getPrimaryFields().stream()
                .map(f -> formatFieldForQuery(f, "t1") + " is null ")
                .collect(joining(" and "));
        String newPrimaryIsNull = criteria.getPrimaryFields().stream()
                .map(f -> formatFieldForQuery(f, "t2") + " is null ")
                .collect(joining(" and "));

        String oldVersionDateFilter = "";
        if (criteria.getOldPublishDate() != null || criteria.getOldCloseDate() != null) {
            oldVersionDateFilter = " and date_trunc('second', t1.\"SYS_PUBLISHTIME\") <= :oldPublishDate\\:\\:timestamp without time zone \n" +
                    " and date_trunc('second', t1.\"SYS_CLOSETIME\") >= :oldCloseDate\\:\\:timestamp without time zone ";
            params.put("oldPublishDate", truncateDateTo(criteria.getOldPublishDate(), ChronoUnit.SECONDS, MIN_DATETIME_VALUE));
            params.put("oldCloseDate", truncateDateTo(criteria.getOldCloseDate(), ChronoUnit.SECONDS, PG_MAX_TIMESTAMP));
        }

        String newVersionDateFilter = "";
        if (criteria.getNewPublishDate() != null || criteria.getNewCloseDate() != null) {
            newVersionDateFilter = " and date_trunc('second', t2.\"SYS_PUBLISHTIME\") <= :newPublishDate\\:\\:timestamp without time zone \n" +
                    " and date_trunc('second', t2.\"SYS_CLOSETIME\") >= :newCloseDate\\:\\:timestamp without time zone ";
            params.put("newPublishDate", truncateDateTo(criteria.getNewPublishDate(), ChronoUnit.SECONDS, MIN_DATETIME_VALUE));
            params.put("newCloseDate", truncateDateTo(criteria.getNewCloseDate(), ChronoUnit.SECONDS, PG_MAX_TIMESTAMP));
        }

        String joinType = diffReturnTypeToJoinType(criteria.getReturnType());

        String query = " from " + getSchemeTableName(oldStorage) + " t1 " + joinType +
                " join " + getSchemeTableName(newStorage) + " t2 on " + primaryEquality +
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
        QueryWithParams countQueryWithParams = new QueryWithParams(SELECT_COUNT_ONLY + query, params);
        Query countQuery = countQueryWithParams.createQuery(entityManager);
        BigInteger count = (BigInteger) countQuery.getSingleResult();

        if (BooleanUtils.toBoolean(criteria.getCountOnly())) {
            return new DataDifference(new CollectionPage<>(count.intValue(), null, criteria));
        }

        String orderBy = " order by " +
                criteria.getPrimaryFields().stream()
                        .map(f -> formatFieldForQuery(f, "t2"))
                        .collect(joining(",")) + "," +
                criteria.getPrimaryFields().stream()
                        .map(f -> formatFieldForQuery(f, "t1"))
                        .collect(joining(","));

        QueryWithParams dataQueryWithParams = new QueryWithParams(dataSelect + query + orderBy, params);
        Query dataQuery = dataQueryWithParams.createQuery(entityManager)
                .setFirstResult(getOffset(criteria))
                .setMaxResults(criteria.getSize());
        List<Object[]> resultList = dataQuery.getResultList();

        List<DiffRowValue> diffRowValues = getDiffRowValues(fields, fieldMap, resultList, criteria);
        return new DataDifference(new CollectionPage<>(count.intValue(), diffRowValues, criteria));
    }

    private String diffReturnTypeToJoinType(DiffReturnTypeEnum typeEnum) {
        switch (typeEnum) {
            case NEW: return "right";
            case OLD: return "left";
            default: return "full";
        }
    }

    private List<DiffRowValue> getDiffRowValues(List<String> fields, Map<String, Field> fieldMap,
                                                List<Object[]> dataList, CompareDataCriteria criteria) {
        List<DiffRowValue> result = new ArrayList<>();
        if (dataList.isEmpty()) {
            return result;
        }

        for (Object[] row : dataList) {
            List<DiffFieldValue> fieldValues = new ArrayList<>();
            int i = 1; // get old/new versions data exclude sys_recordid
            List<String> primaryFields = criteria.getPrimaryFields();
            DiffStatusEnum rowStatus = null;
            for (String field : fields) {
                DiffFieldValue fieldValue = new DiffFieldValue();
                fieldValue.setField(fieldMap.get(field));
                Object oldValue = row[i];
                Object newValue = row[row.length / 2 + i];
                fieldValue.setOldValue(oldValue);
                fieldValue.setNewValue(newValue);

                if (primaryFields.contains(field) && rowStatus == null) {
                    rowStatus = diffFieldValueToStatusEnum(fieldValue);
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

                    if (!Objects.equals(oldValue, newValue)) {
                        fieldValue.setStatus(DiffStatusEnum.UPDATED);
                    } else {
                        //if value is not changed store only new value
                        fieldValue.setOldValue(null);
                    }
                }
            }

            result.add(new DiffRowValue(fieldValues, rowStatus));
        }
        return result;
    }

    private DiffStatusEnum diffFieldValueToStatusEnum(DiffFieldValue value) {

        if (value.getOldValue() == null) {
            return DiffStatusEnum.INSERTED;
        }

        if (value.getNewValue() == null) {
            return DiffStatusEnum.DELETED;
        }

        if (value.getOldValue().equals(value.getNewValue())) {
            return DiffStatusEnum.UPDATED;
        }

        return null;
    }

    private String getFieldValuesFilter(String alias, Map<String, Object> params, Set<List<FieldSearchCriteria>> fieldValuesFilters) {
        QueryWithParams queryWithParams = getDictionaryFilterQuery(null, fieldValuesFilters, emptyList(), alias);
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
package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import net.n2oapp.criteria.api.CollectionPage;
import net.n2oapp.criteria.api.Sorting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.platform.datastorage.temporal.enums.DiffReturnTypeEnum;
import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.enums.ReferenceDisplayType;
import ru.i_novus.platform.datastorage.temporal.model.*;
import ru.i_novus.platform.datastorage.temporal.model.criteria.*;
import ru.i_novus.platform.datastorage.temporal.model.value.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil;
import ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils;

import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.transaction.Transactional;
import java.io.Serializable;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.springframework.util.CollectionUtils.isEmpty;
import static ru.i_novus.platform.datastorage.temporal.util.CollectionUtils.isNullOrEmpty;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.StorageConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.CompareUtil.toDiffRowValues;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StorageUtils.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.*;

@SuppressWarnings("java:S1192")
public class DataDaoImpl implements DataDao {

    private static final Logger logger = LoggerFactory.getLogger(DataDaoImpl.class);

    private static final LocalDateTime PG_MAX_TIMESTAMP = LocalDateTime.of(294276, 12, 31, 23, 59);

    private static final Pattern SEARCH_DATE_PATTERN = Pattern.compile("([0-9]{2})\\.([0-9]{2})\\.([0-9]{4})");

    private EntityManager entityManager;

    public DataDaoImpl(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Override
    public List<RowValue> getData(StorageDataCriteria criteria) {

        final String schemaName = getStorageCodeSchemaName(criteria.getStorageCode());
        final String tableName = toTableName(criteria.getStorageCode());

        List<Field> fields = new ArrayList<>(criteria.getFields());
        fields.add(0, new IntegerField(SYS_PRIMARY_COLUMN));
        if (fields.stream().noneMatch(field -> SYS_HASH.equals(field.getName())))
            fields.add(1, new StringField(SYS_HASH));

        final String sqlFormat = "SELECT %1$s \n  FROM %2$s as %3$s ";
        String sqlFields = getSelectFields(DEFAULT_TABLE_ALIAS, fields, true);
        String sql = String.format(sqlFormat, sqlFields, escapeTableName(schemaName, tableName), DEFAULT_TABLE_ALIAS);

        QueryWithParams queryWithParams = new QueryWithParams(sql);

        QueryWithParams where = getCriteriaWhereClause(criteria, DEFAULT_TABLE_ALIAS);
        if (!StringUtils.isNullOrEmpty(where.getSql())) {
            queryWithParams.concat(" WHERE ");
            queryWithParams.concat(where);
        }

        Sorting sorting = !isNullOrEmpty(criteria.getSortings()) ? criteria.getSortings().get(0) : null;
        queryWithParams.concat(sortingToOrderBy(sorting, DEFAULT_TABLE_ALIAS));

        Query query = queryWithParams.createQuery(entityManager);
        if (criteria.hasPageAndSize()) {
            query.setFirstResult(criteria.getOffset()).setMaxResults(criteria.getSize());
        }

        @SuppressWarnings("unchecked")
        List<Object[]> list = query.getResultList();
        return !isNullOrEmpty(list) ? toRowValues(fields, list) : emptyList();
    }

    @Override
    public BigInteger getDataCount(StorageDataCriteria criteria) {

        final String schemaName = getStorageCodeSchemaName(criteria.getStorageCode());
        final String tableName = toTableName(criteria.getStorageCode());

        final String sqlFormat = "  FROM %s as %s\n";
        String sql = SELECT_COUNT_ONLY +
                String.format(sqlFormat, escapeTableName(schemaName, tableName), DEFAULT_TABLE_ALIAS);

        QueryWithParams queryWithParams = new QueryWithParams(sql);

        QueryWithParams where = getCriteriaWhereClause(criteria, DEFAULT_TABLE_ALIAS);
        if (!StringUtils.isNullOrEmpty(where.getSql())) {
            queryWithParams.concat(" WHERE ");
            queryWithParams.concat(where);
        }

        return (BigInteger) queryWithParams.createQuery(entityManager).getSingleResult();
    }

    @Override
    public boolean hasData(String storageCode) {

        StorageDataCriteria criteria = new StorageDataCriteria(storageCode, null, null,
                emptyList(), emptySet(), null);
        criteria.setCount(1);
        criteria.setPage(BaseDataCriteria.MIN_PAGE);
        criteria.setSize(BaseDataCriteria.MIN_SIZE);

        List<RowValue> data = getData(criteria);
        return !isNullOrEmpty(data);
    }

    @Override
    public RowValue getRowData(String storageCode, List<String> fieldNames, Object systemId) {

        String schemaName = getStorageCodeSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        List<Field> fields = columnDataTypesToFields(getColumnDataTypes(storageCode), fieldNames);

        String sqlFields = getSelectFields(null, fields, true);
        String sql = String.format(SELECT_ROWS_FROM_DATA_BY_FIELD_EQ,
                sqlFields, schemaName, addDoubleQuotes(tableName),
                addDoubleQuotes(SYS_PRIMARY_COLUMN), QUERY_VALUE_SUBST);

        @SuppressWarnings("unchecked")
        List<Object[]> list = entityManager.createNativeQuery(sql)
                .setParameter(1, systemId)
                .getResultList();
        if (isNullOrEmpty(list))
            return null;

        RowValue row = toRowValues(fields, list).get(0);
        row.setSystemId(systemId); // ??
        return row;
    }

    /** Получение строк данных таблицы по системным идентификаторам. */
    @Override
    public List<RowValue> getRowData(String storageCode, List<String> fieldNames, List<Object> systemIds) {

        String schemaName = getStorageCodeSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        List<Field> fields = columnDataTypesToFields(getColumnDataTypes(storageCode), fieldNames);

        String sqlFields = getSelectFields(null, fields, true);
        String sql = String.format(SELECT_ROWS_FROM_DATA_BY_FIELD_EQ,
                sqlFields, schemaName, addDoubleQuotes(tableName),
                addDoubleQuotes(SYS_PRIMARY_COLUMN),
                String.format(TO_ANY_BIGINT, QUERY_VALUE_SUBST));
        Query query = entityManager.createNativeQuery(sql);
        query.setParameter(1, valuesToDbArray(systemIds));

        @SuppressWarnings("unchecked")
        List<Object[]> list = query.getResultList();
        return !isNullOrEmpty(list) ? toRowValues(fields, list) : emptyList();
    }

    /** Преобразование набора наименований полей с типами в список полей. */
    private List<Field> columnDataTypesToFields(Map<String, String> dataTypes, List<String> fieldNames) {

        List<Field> fields = new ArrayList<>(fieldNames.size());
        fields.add(new IntegerField(SYS_PRIMARY_COLUMN));
        fields.add(new StringField(SYS_HASH));

        for (Map.Entry<String, String> entry : dataTypes.entrySet()) {
            String fieldName = entry.getKey();
            if (fieldNames.contains(fieldName)) {
                fields.add(getField(fieldName, entry.getValue()));
            }
        }
        return fields;
    }

    @Override
    public List<String> findExistentHashes(String storageCode, LocalDateTime bdate, LocalDateTime edate,
                                           List<String> hashList) {

        final String sqlFormat = "SELECT %1$s \n  FROM %2$s as %3$s ";
        String sql = String.format(sqlFormat,
                addDoubleQuotes(SYS_HASH),
                escapeStorageTableName(storageCode),
                DEFAULT_TABLE_ALIAS) +
                " WHERE true \n";

        QueryWithParams queryWithParams = new QueryWithParams(sql);
        queryWithParams.concat(getWhereByDates(bdate, edate, DEFAULT_TABLE_ALIAS));
        queryWithParams.concat(getWhereByHashList(hashList, DEFAULT_TABLE_ALIAS));

        return queryWithParams.createQuery(entityManager).getResultList();
    }

    @Override
    public boolean storageStructureEquals(String storageCode1, String storageCode2) {

        Map<String, String> dataTypes1 = getColumnDataTypes(storageCode1);
        Map<String, String> dataTypes2 = getColumnDataTypes(storageCode2);
        return dataTypes1.equals(dataTypes2);
    }

    @Override
    public Map<String, String> getColumnDataTypes(String storageCode) {

        @SuppressWarnings("unchecked")
        List<Object[]> nameTypes = entityManager.createNativeQuery(SELECT_FIELD_NAMES_AND_TYPES)
                .setParameter(BIND_INFO_SCHEMA_NAME, toSchemaName(storageCode))
                .setParameter(BIND_INFO_TABLE_NAME, toTableName(storageCode))
                .getResultList();

        Map<String, String> map = new HashMap<>();
        for (Object[] dataType : nameTypes) {
            String fieldName = (String) dataType[0];
            if (!systemFieldNames().contains(fieldName))
                map.put(fieldName, (String) dataType[1]);
        }
        return map;
    }

    protected String sortingToOrderBy(Sorting sorting, String alias) {

        String orderBy = " ORDER BY ";
        if (sorting != null && sorting.getField() != null) {
            orderBy = orderBy + escapeFieldName(alias, sorting.getField()) +
                    " " + sorting.getDirection().toString() + ", ";
        }

        return orderBy + " " + escapeFieldName(alias, SYS_PRIMARY_COLUMN);
    }

    private QueryWithParams getCriteriaWhereClause(StorageDataCriteria criteria, String alias) {

        StorageDataCriteria whereCriteria = new StorageDataCriteria(criteria);
        if (!isEmpty(criteria.getHashList())) {
            // Поиск только по списку hash:
            whereCriteria.setFieldFilters(null);
            whereCriteria.setSystemIds(null);
        }

        return getWhereClause(whereCriteria, alias);
    }

    private QueryWithParams getWhereClause(StorageDataCriteria criteria, String alias) {

        QueryWithParams query = new QueryWithParams(" true ");
        query.concat(getWhereByDates(criteria.getBdate(), criteria.getEdate(), alias));
        query.concat(getWhereByFts(criteria.getCommonFilter(), alias));
        query.concat(getWhereByFilters(criteria.getFieldFilters(), alias));
        query.concat(getWhereByHashList(criteria.getHashList(), alias));
        query.concat(getWhereBySystemIds(criteria.getSystemIds(), alias));

        return query;
    }

    private QueryWithParams getWhereByDates(LocalDateTime publishDate, LocalDateTime closeDate, String alias) {

        if (publishDate == null)
            return null;

        String sql = "";
        Map<String, Object> params = new HashMap<>();

        String sqlByPublishDate = " and date_trunc('second', %1$s.%2$s) <= :bdate \n" +
                " and (date_trunc('second', %1$s.%3$s) > :bdate or %1$s.%3$s is null) \n";
        sql += String.format(sqlByPublishDate, alias,
                addDoubleQuotes(SYS_PUBLISHTIME),
                addDoubleQuotes(SYS_CLOSETIME));
        params.put("bdate", publishDate.truncatedTo(ChronoUnit.SECONDS));

        String sqlByCloseDate = " and (date_trunc('second', %1$s.%2$s) >= :edate or %1$s.%2$s is null) \n";
        sql += String.format(sqlByCloseDate, alias, addDoubleQuotes(SYS_CLOSETIME));

        closeDate = closeDate == null ? PG_MAX_TIMESTAMP : closeDate;
        params.put("edate", closeDate.truncatedTo(ChronoUnit.SECONDS));

        return new QueryWithParams(sql, params);
    }

    private QueryWithParams getWhereByFts(String search, String alias) {

        search = search != null ? search.trim() : "";
        if (StringUtils.isNullOrEmpty(search))
            return null;

        String sql = "";
        Map<String, Object> params = new HashMap<>();

        // full text search
        String escapedFtsColumn = escapeFieldName(alias, SYS_FTS);
        if (SEARCH_DATE_PATTERN.matcher(search).matches()) {
            sql += " and (" +
                    escapedFtsColumn + " @@ to_tsquery(:search) or " +
                    escapedFtsColumn + " @@ to_tsquery(:reverseSearch) ) ";
            String[] dateArr = search.split("\\.");
            String reverseSearch = dateArr[2] + "-" + dateArr[1] + "-" + dateArr[0];
            params.put("search", search.trim());
            params.put("reverseSearch", reverseSearch);

        } else {
            String formattedSearch = search.toLowerCase()
                    .replace(":", "\\:")
                    .replace("/", "\\/")
                    .replace(" ", "+");
            sql += " and (" + escapedFtsColumn + " @@ to_tsquery(:formattedSearch||':*') or " +
                    escapedFtsColumn + " @@ to_tsquery('ru', :formattedSearch||':*') or " +
                    escapedFtsColumn + " @@ to_tsquery('ru', :original||':*')) ";
            params.put("formattedSearch", "'" + formattedSearch + "'");
            params.put("original", "'''" + search + "'''");

        }

        return new QueryWithParams(sql, params);
    }

    private QueryWithParams getWhereByFilters(Set<List<FieldSearchCriteria>> fieldFilters, String alias) {

        if (isNullOrEmpty(fieldFilters))
            return null;

        String sql = "";
        Map<String, Object> params = new HashMap<>();

        fieldFilters = prepareFilters(fieldFilters);
        AtomicInteger index = new AtomicInteger(0);
        sql += fieldFilters.stream().map(list -> {
            if (isEmpty(list))
                return null;

            List<String> filters = new ArrayList<>();
            list.forEach(searchCriteria ->
                toWhereClauseByFilter(searchCriteria, index.getAndIncrement(), alias, filters, params)
            );

            if (filters.isEmpty())
                return null;

            return " true " + String.join(QUERY_NEW_LINE, filters);
        })
                .filter(Objects::nonNull)
                .collect(joining(" OR "));

        if (!"".equals(sql))
            sql = " and (" + sql + ")";

        return new QueryWithParams(sql, params);
    }

    private void toWhereClauseByFilter(FieldSearchCriteria searchCriteria,
                                       int index, String alias,
                                       List<String> filters, Map<String, Object> params) {

        Field field = searchCriteria.getField();
        List<? extends Serializable> values = searchCriteria.getValues();

        String fieldName = field.getName();
        String escapedFieldName = escapeFieldName(alias, fieldName);

        if (values == null || values.get(0) == null) {
            filters.add(" AND " + escapedFieldName + " IS NULL");
            return;
        }

        String indexedFieldName = fieldName + index;

        if (field instanceof IntegerField || field instanceof FloatField || field instanceof DateField) {
            filters.add(" AND " + escapedFieldName +
                    " IN (:" + indexedFieldName + ")");
            params.put(indexedFieldName, values);

        } else if (field instanceof ReferenceField) {
            // Использовать TO_ANY_BIGINT
            filters.add(" AND " + escapedFieldName +
                    REFERENCE_FIELD_VALUE_OPERATOR + addSingleQuotes(REFERENCE_VALUE_NAME) +
                    " IN (:" + indexedFieldName + ")");
            params.put(indexedFieldName, values.stream().map(Object::toString).collect(toList()));

        } else if (field instanceof TreeField) {
            if (SearchTypeEnum.LESS.equals(searchCriteria.getType())) {
                filters.add(" AND " + escapedFieldName +
                        "@> (CAST(:" + indexedFieldName + " AS ltree[]))");
                params.put(indexedFieldName, valuesToDbArray(values));
            }
        } else if (field instanceof BooleanField) {
            if (values.size() == 1) {
                String isValue = Boolean.TRUE.equals(values.get(0)) ? " IS TRUE " : " IS NOT TRUE";
                filters.add(" AND " + escapedFieldName + isValue);
            }
        } else if (field instanceof StringField) {
            if (SearchTypeEnum.LIKE.equals(searchCriteria.getType()) && values.size() == 1) {
                filters.add(" AND " + "lower(" + escapedFieldName + ") LIKE :" + indexedFieldName + "");
                String value = values.get(0).toString().trim().toLowerCase();
                params.put(indexedFieldName, LIKE_ESCAPE_MANY_CHAR + value + LIKE_ESCAPE_MANY_CHAR);
            } else {
                filters.add(" AND " + escapedFieldName + " IN (:" + indexedFieldName + ")");
                params.put(indexedFieldName, values);
            }
        } else {
            params.put(indexedFieldName, values);
        }
    }

    private Set<List<FieldSearchCriteria>> prepareFilters(Set<List<FieldSearchCriteria>> filters) {

        Set<List<FieldSearchCriteria>> set = new HashSet<>();
        for (List<FieldSearchCriteria> list : filters) {
            set.add(groupBySearchType(list));
        }

        return set;
    }

    private List<FieldSearchCriteria> groupBySearchType(List<FieldSearchCriteria> list) {

        EnumMap<SearchTypeEnum, Map<String, FieldSearchCriteria>> typedGroup = new EnumMap<>(SearchTypeEnum.class);
        for (FieldSearchCriteria criteria : list) {
            Map<String, FieldSearchCriteria> typedMap = typedGroup.computeIfAbsent(criteria.getType(), k -> new HashMap<>());

            FieldSearchCriteria typedCriteria = typedMap.get(criteria.getField().getName());
            if (typedCriteria == null) {
                criteria.setValues(new ArrayList<>(criteria.getValues()));
                typedMap.put(criteria.getField().getName(), criteria);

            } else {
                @SuppressWarnings("unchecked")
                List<Serializable> typedValues = (List<Serializable>) typedCriteria.getValues();
                typedValues.addAll(criteria.getValues());
            }
        }

        return typedGroup.values().stream().map(Map::values).flatMap(Collection::stream).collect(toList());
    }

    private QueryWithParams getWhereByHashList(List<String> hashList, String alias) {

        if (isNullOrEmpty(hashList))
            return null;

        Map<String, Object> params = new HashMap<>();
        String escapedColumn = escapeFieldName(alias, SYS_HASH);

        String sql;
        if (hashList.size() > 1) {
            sql = " and (" + escapedColumn + " = " +
                    String.format(TO_ANY_TEXT, ":hashList") + ")";
            params.put("hashList", stringsToDbArray(hashList));

        } else {
            sql = " and (" + escapedColumn + " = :hashItem)";
            params.put("hashItem", hashList.get(0));
        }

        return new QueryWithParams(sql, params);
    }

    private QueryWithParams getWhereBySystemIds(List<Long> systemIds, String alias) {

        if (isNullOrEmpty(systemIds))
            return null;

        Map<String, Object> params = new HashMap<>();
        String escapedColumn = escapeFieldName(alias, SYS_PRIMARY_COLUMN);

        String sql;
        if (systemIds.size() > 1) {
            sql = " and (" + escapedColumn + " = " +
                    String.format(TO_ANY_BIGINT, ":systemIds") + ")";
            params.put("systemIds", valuesToDbArray(systemIds));

        } else {
            sql = " and (" + escapedColumn + " = :systemId)";
            params.put("systemId", systemIds.get(0));
        }

        return new QueryWithParams(sql, params);
    }

    @Override
    public BigInteger countData(String storageCode) {

        String sql = "SELECT count(*) FROM " + escapeStorageTableName(storageCode);
        return (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public boolean schemaExists(String schemaName) {

        Boolean result = (Boolean) entityManager.createNativeQuery(SELECT_SCHEMA_EXISTS)
                .setParameter(BIND_INFO_SCHEMA_NAME, schemaName)
                .getSingleResult();

        return result != null && result;
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public List<String> findExistentSchemas(List<String> schemaNames) {

        if (isNullOrEmpty(schemaNames))
            return emptyList();

        String condition = String.format(TO_ANY_TEXT, QUERY_BIND_CHAR + BIND_INFO_SCHEMA_NAME);
        String sql = SELECT_EXISTENT_SCHEMA_NAME_LIST_BY + condition;

        Query query = entityManager.createNativeQuery(sql);
        query.setParameter(BIND_INFO_SCHEMA_NAME, stringsToDbArray(schemaNames));

        @SuppressWarnings("unchecked")
        List<String> list = query.getResultList();
        return !isNullOrEmpty(list) ? list : emptyList();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public List<String> findExistentTableSchemas(List<String> schemaNames, String tableName) {

        if (isNullOrEmpty(schemaNames))
            return emptyList();

        String condition = String.format(TO_ANY_TEXT, QUERY_BIND_CHAR + BIND_INFO_SCHEMA_NAME);
        String sql = SELECT_EXISTENT_TABLE_SCHEMA_NAME_LIST_BY + condition;

        Query query = entityManager.createNativeQuery(sql);
        query.setParameter(BIND_INFO_TABLE_NAME, tableName);
        query.setParameter(BIND_INFO_SCHEMA_NAME, stringsToDbArray(schemaNames));

        @SuppressWarnings("unchecked")
        List<String> list = query.getResultList();
        return !isNullOrEmpty(list) ? list : emptyList();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public boolean storageExists(String storageCode) {

        Boolean result = (Boolean) entityManager.createNativeQuery(SELECT_TABLE_EXISTS)
                .setParameter(BIND_INFO_SCHEMA_NAME, toSchemaName(storageCode))
                .setParameter(BIND_INFO_TABLE_NAME, toTableName(storageCode))
                .getSingleResult();

        return result != null && result;
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public boolean storageFieldExists(String storageCode, String fieldName) {

        Boolean result = (Boolean) entityManager.createNativeQuery(SELECT_COLUMN_EXISTS)
                .setParameter(BIND_INFO_SCHEMA_NAME, toSchemaName(storageCode))
                .setParameter(BIND_INFO_TABLE_NAME, toTableName(storageCode))
                .setParameter(BIND_INFO_COLUMN_NAME, fieldName)
                .getSingleResult();

        return result != null && result;
    }

    @Override
    @Transactional
    public void createSchema(String schemaName) {

        if (isDefaultSchema(schemaName))
            return;

        if (!isValidSchemaName(schemaName))
            throw new IllegalArgumentException("schema.name.is.invalid");

        String ddl = String.format(CREATE_SCHEMA, schemaName);
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @Transactional
    public void createDraftTable(String storageCode, List<Field> fields) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);
        
        if (storageExists(storageCode))
            throw new IllegalArgumentException("table.already.exists");

        String tableFields = "";
        if (!isNullOrEmpty(fields)) {
            tableFields = fields.stream()
                    .map(f -> addDoubleQuotes(f.getName()) + " " + f.getType())
                    .collect(joining(", \n")) + ", \n";
        }

        String ddl = String.format(CREATE_DRAFT_TABLE,
                schemaName, addDoubleQuotes(tableName), tableFields, tableName);

        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void dropTable(String storageCode) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        if (StringUtils.isNullOrEmpty(tableName)) {
            logger.error("Dropping table name is empty");
            return;
        }

        dropTriggers(storageCode);
        dropTableFunctions(storageCode);

        String ddl = String.format(DROP_TABLE, schemaName, addDoubleQuotes(tableName));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    protected void createTableSequence(String storageCode) {

        String ddl = String.format(CREATE_TABLE_SEQUENCE, escapeStorageSequenceName(storageCode));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void updateTableSequence(String storageCode) {

        String sqlSelect = String.format(SELECT_PRIMARY_MAX,
                toSchemaName(storageCode),
                addDoubleQuotes(toTableName(storageCode)),
                addDoubleQuotes(SYS_PRIMARY_COLUMN));
        String sql = String.format(UPDATE_TABLE_SEQUENCE, escapeStorageSequenceName(storageCode), sqlSelect);
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    protected void dropTableSequence(String storageCode) {

        String ddl = String.format(DROP_TABLE_SEQUENCE, escapeStorageSequenceName(storageCode));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @Transactional
    public void createTriggers(String storageCode, List<String> fieldNames) {

        createHashTrigger(storageCode, fieldNames);
        createFtsTrigger(storageCode, fieldNames);
    }

    protected void createHashTrigger(String storageCode, List<String> fieldNames) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        final String alias = TRIGGER_NEW_ALIAS + NAME_SEPARATOR;
        String tableFields = fieldNames.stream().map(QueryUtil::getClearedFieldName).collect(joining(", "));

        String expression = String.format(HASH_EXPRESSION,
                fieldNames.stream().map(field -> alias + field).collect(joining(", ")));
        String triggerBody = alias + addDoubleQuotes(SYS_HASH) + " = " + expression;
        String ddl = String.format(CREATE_TRIGGER, schemaName, tableName,
                escapeTableFunctionName(tableName, HASH_FUNCTION_NAME),
                HASH_TRIGGER_NAME, tableFields, triggerBody + ";");
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    protected void createFtsTrigger(String storageCode, List<String> fieldNames) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        final String alias = TRIGGER_NEW_ALIAS + NAME_SEPARATOR;
        String tableFields = fieldNames.stream().map(QueryUtil::getClearedFieldName).collect(joining(", "));

        String expression = fieldNames.stream()
                .map(field -> "coalesce( to_tsvector('ru', " + alias + field + "\\:\\:text),'')")
                .collect(joining(" || ' ' || "));
        String triggerBody = alias + addDoubleQuotes(SYS_FTS) + " = " + expression;
        String ddl = String.format(CREATE_TRIGGER, schemaName, tableName,
                escapeTableFunctionName(tableName, FTS_FUNCTION_NAME),
                FTS_TRIGGER_NAME, tableFields, triggerBody + ";");
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void dropTriggers(String storageCode) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        String escapedTableName = addDoubleQuotes(tableName);

        String dropHashTrigger = String.format(DROP_TRIGGER, HASH_TRIGGER_NAME, schemaName, escapedTableName);
        entityManager.createNativeQuery(dropHashTrigger).executeUpdate();

        String dropFtsTrigger = String.format(DROP_TRIGGER, FTS_TRIGGER_NAME, schemaName, escapedTableName);
        entityManager.createNativeQuery(dropFtsTrigger).executeUpdate();
    }

    @Override
    @Transactional
    public void enableTriggers(String storageCode) {

        String ddl = String.format(SWITCH_TRIGGERS,escapeStorageTableName(storageCode), "ENABLE");
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @Transactional
    public void disableTriggers(String storageCode) {

        String ddl = String.format(SWITCH_TRIGGERS, escapeStorageTableName(storageCode), "DISABLE");
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void dropTableFunctions(String storageCode) {

        dropTableFunction(storageCode, HASH_FUNCTION_NAME);
        dropTableFunction(storageCode, FTS_FUNCTION_NAME);
    }

    protected void dropTableFunction(String storageCode, String functionName) {

        String ddl = String.format(DROP_FUNCTION,
                toSchemaName(storageCode),
                escapeTableFunctionName(toTableName(storageCode), functionName));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @Transactional
    public void updateHashRows(String storageCode, List<String> fieldNames) {

        String expression = String.format(HASH_EXPRESSION, String.join(", ", fieldNames));
        String ddlAssign = addDoubleQuotes(SYS_HASH) + " = " + expression;

        String ddl = String.format(UPDATE_FIELD,
                toSchemaName(storageCode), addDoubleQuotes(toTableName(storageCode)), ddlAssign);
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @Transactional
    public void updateFtsRows(String storageCode, List<String> fieldNames) {

        String expression = fieldNames.stream()
                .map(field -> "coalesce( to_tsvector('ru', " + field + "\\:\\:text),'')")
                .collect(joining(" || ' ' || "));
        String ddlAssign = addDoubleQuotes(SYS_FTS) + " = " + expression;

        String ddl = String.format(UPDATE_FIELD,
                toSchemaName(storageCode), addDoubleQuotes(toTableName(storageCode)), ddlAssign);
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @Transactional
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createIndex(String storageCode, String name, List<String> fieldNames) {

        String expression = fieldNames.stream().map(StringUtils::addDoubleQuotes).collect(joining(","));
        String ddl = String.format(CREATE_TABLE_INDEX,
                name,
                toSchemaName(storageCode),
                addDoubleQuotes(toTableName(storageCode)),
                "",
                expression);
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createHashIndex(String storageCode) {

        String tableName = toTableName(storageCode);

        // Неуникальный индекс, т.к. используется только для создания хранилища версии.
        // В версии могут быть идентичные строки с отличающимся периодом действия.
        String ddl = String.format(CREATE_TABLE_INDEX,
                addDoubleQuotes(tableName + NAME_CONNECTOR + TABLE_INDEX_SYSHASH_NAME),
                toSchemaName(storageCode),
                addDoubleQuotes(tableName),
                "",
                addDoubleQuotes(SYS_HASH));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createFtsIndex(String storageCode) {

        String tableName = toTableName(storageCode);

        String ddl = String.format(CREATE_TABLE_INDEX,
                escapeTableIndexName(tableName, TABLE_INDEX_FTS_NAME),
                toSchemaName(storageCode),
                addDoubleQuotes(tableName),
                TABLE_INDEX_FTS_USING,
                addDoubleQuotes(SYS_FTS));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createLtreeIndex(String storageCode, String fieldName) {

        String tableName = toTableName(storageCode);

        String ddl = String.format(CREATE_TABLE_INDEX,
                escapeTableIndexName(tableName, fieldName.toLowerCase()),
                toSchemaName(storageCode),
                addDoubleQuotes(tableName),
                TABLE_INDEX_LTREE_USING,
                addDoubleQuotes(fieldName));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @Transactional
    public void copyTable(String sourceCode, String targetCode) {

        String sourceSchema = toSchemaName(sourceCode);
        String sourceTable = toTableName(sourceCode);
        if (StringUtils.isNullOrEmpty(sourceTable))
            throw new IllegalArgumentException("source.table.name.is.empty");

        String targetSchema = toSchemaName(targetCode);
        String targetTable = toTableName(targetCode);
        if (StringUtils.isNullOrEmpty(targetTable))
            throw new IllegalArgumentException("target.table.name.is.empty");

        String ddlCopyTable = String.format(CREATE_TABLE_COPY,
                targetSchema, addDoubleQuotes(targetTable),
                sourceSchema, addDoubleQuotes(sourceTable));
        entityManager.createNativeQuery(ddlCopyTable).executeUpdate();

        copyIndexes(sourceCode, targetCode);

        createHashIndex(targetCode);

        addPrimaryKey(targetCode);
        addTableSequence(targetCode);
    }

    /**
     * Копирование всех индексов
     * (включая индекс для FTS и исключая индекс для SYS_HASH).
     */
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    private void copyIndexes(String sourceCode, String targetCode) {

        String sourceSchema = toSchemaName(sourceCode);
        String sourceTable = toTableName(sourceCode);

        final String notLikeIndexes = LIKE_ESCAPE_MANY_CHAR + addDoubleQuotes(SYS_HASH) + LIKE_ESCAPE_MANY_CHAR;
        String sql = SELECT_DDL_INDEXES + AND_DDL_INDEX_NOT_LIKE;
        List<String> ddlIndexes = entityManager.createNativeQuery(sql)
                .setParameter(BIND_INFO_SCHEMA_NAME, sourceSchema)
                .setParameter(BIND_INFO_TABLE_NAME, sourceTable)
                .setParameter("notLikeIndexes", notLikeIndexes)
                .getResultList();

        String targetSchema = toSchemaName(targetCode);
        String targetTable = toTableName(targetCode);

        for (String ddlIndex : ddlIndexes) {
            String ddl = ddlIndex
                    .replace(sourceSchema + NAME_SEPARATOR + sourceTable,
                            targetSchema + NAME_SEPARATOR + targetTable)
                    .replace(escapeTableName(sourceSchema, sourceTable),
                            escapeTableName(targetSchema, targetTable))
                    .replace(sourceTable, targetTable);
            entityManager.createNativeQuery(ddl).executeUpdate();
        }
    }

    /** Добавление SYS_PRIMARY_COLUMN. */
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    private void addPrimaryKey(String storageCode) {

        String ddlAddPrimaryKey = String.format(ALTER_ADD_PRIMARY_KEY,
                toSchemaName(storageCode),
                addDoubleQuotes(toTableName(storageCode)),
                addDoubleQuotes(SYS_PRIMARY_COLUMN));
        entityManager.createNativeQuery(ddlAddPrimaryKey).executeUpdate();
    }

    /** Добавление последовательности. */
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    protected void addTableSequence(String storageCode) {

        createTableSequence(storageCode);

        String tableName = toTableName(storageCode);

        String ddlAlterColumn = String.format(ALTER_SET_SEQUENCE_FOR_PRIMARY_KEY,
                toSchemaName(storageCode), addDoubleQuotes(tableName),
                addDoubleQuotes(SYS_PRIMARY_COLUMN), escapeSequenceName(tableName));
        entityManager.createNativeQuery(ddlAlterColumn).executeUpdate();
    }

    @Override
    @Transactional
    public void addVersionedInformation(String storageCode) {

        addColumn(storageCode, SYS_PUBLISHTIME, "timestamp without time zone", MIN_TIMESTAMP_VALUE);
        addColumn(storageCode, SYS_CLOSETIME, "timestamp without time zone", MAX_TIMESTAMP_VALUE);

        createIndex(storageCode,
                escapeTableIndexName(toTableName(storageCode), TABLE_INDEX_SYSDATE_NAME),
                Arrays.asList(SYS_PUBLISHTIME, SYS_CLOSETIME));
    }

    @Override
    @Transactional
    public void addColumn(String storageCode, String name, String type, String defaultValue) {

        String condition = (defaultValue != null) ? "DEFAULT " + defaultValue : "";
        String ddl = String.format(ALTER_ADD_COLUMN,
                toSchemaName(storageCode), addDoubleQuotes(toTableName(storageCode)),
                addDoubleQuotes(name), type, condition);

        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void alterDataType(String storageCode, String fieldName, String oldType, String newType) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        String escapedFieldName = addDoubleQuotes(fieldName);
        String using = getFieldNameByType(escapedFieldName, oldType, newType);

        String ddl = String.format(ALTER_COLUMN_WITH_USING,
                schemaName, addDoubleQuotes(tableName), escapedFieldName, newType, using);
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void deleteColumn(String storageCode, String name) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        String ddl = String.format(ALTER_DELETE_COLUMN,
                schemaName, addDoubleQuotes(tableName), addDoubleQuotes(name));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @Transactional
    public List<String> insertData(String storageCode, List<RowValue> data) {

        if (isEmpty(data))
            return emptyList();

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        List<FieldValue> fieldValues = data.get(0).getFieldValues();
        String insertKeys = fieldValues.stream()
                .map(fieldValue -> addDoubleQuotes(fieldValue.getField()))
                .collect(joining(","));

        List<String> substList = data.stream()
                .map(rowValue ->
                        ((List<FieldValue>) rowValue.getFieldValues()).stream()
                                .map(fieldValue -> toInsertValueSubst(schemaName, fieldValue))
                                .collect(joining(","))
                )
                .collect(toList());

        List<String> hashes = new ArrayList<>(substList.size());

        int batchSize = 500;
        for (int firstIndex = 0, nextIndex = batchSize;
             firstIndex < substList.size();
             firstIndex = nextIndex, nextIndex = firstIndex + batchSize) {

            int valueCount = Math.min(nextIndex, substList.size());
            List<String> batchHashes = insertData(schemaName, tableName, insertKeys,
                    substList.subList(firstIndex, valueCount), data.subList(firstIndex, valueCount));
            hashes.addAll(batchHashes);
        }

        return hashes;
    }

    private String toInsertValueSubst(String schemaName, FieldValue fieldValue) {

        if (fieldValue.getValue() == null) {
            return QUERY_NULL_VALUE;
        }

        if (fieldValue instanceof ReferenceFieldValue) {
            return getReferenceValuationSelect(schemaName,
                    (ReferenceFieldValue) fieldValue, QUERY_VALUE_SUBST);
        }

        if (fieldValue instanceof TreeFieldValue) {
            return QUERY_LTREE_SUBST;
        }

        return QUERY_VALUE_SUBST;
    }

    private List<String> insertData(String schemaName, String tableName, String insertKeys,
                                    List<String> subst, List<RowValue> data) {

        String insertSubsts = subst.stream().collect(joining("),(", "(", ")"));
        String sql = String.format(INSERT_RECORD,
                schemaName, addDoubleQuotes(tableName), insertKeys) +
                String.format(INSERT_VALUES, insertSubsts) +
                "RETURNING " + addDoubleQuotes(SYS_HASH);
        Query query = entityManager.createNativeQuery(sql);

        int i = 1;
        for (RowValue rowValue : data) {
            for (FieldValue fieldValue : (List<FieldValue>) rowValue.getFieldValues()) {
                Serializable parameter = toQueryParameter(fieldValue);
                if (parameter != null) {
                    query.setParameter(i++, parameter);
                }
            }
        }

        return query.getResultList();
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
    private String getReferenceValuationSelect(String schemaName, ReferenceFieldValue fieldValue, String valueSubst) {

        Reference refValue = fieldValue.getValue();
        if (refValue.getValue() == null && QUERY_NULL_VALUE.equals(valueSubst))
            return QUERY_NULL_VALUE;

        ReferenceDisplayType displayType = getReferenceDisplayType(refValue);
        if (displayType == null)
            return "(" + REFERENCE_VALUATION_SELECT_SUBST + ")";

        String sqlExpression;
        switch (displayType) {
            case DISPLAY_EXPRESSION:
                sqlExpression = sqlDisplayExpression(refValue.getDisplayExpression(), REFERENCE_VALUATION_SELECT_TABLE_ALIAS);
                break;

            case DISPLAY_FIELD:
                sqlExpression = sqlFieldExpression(refValue.getDisplayField(), REFERENCE_VALUATION_SELECT_TABLE_ALIAS);
                break;

            default:
                throw new UnsupportedOperationException("unknown.reference.dipslay.type");
        }

        QueryWithParams whereByDate = getWhereByDates(refValue.getDate(), null, REFERENCE_VALUATION_SELECT_TABLE_ALIAS);
        String sqlDateValue = formatDateTime(refValue.getDate());
        String sqlByDate = (whereByDate == null || StringUtils.isNullOrEmpty(whereByDate.getSql()))
                ? ""
                : whereByDate.getSql()
                .replace(":bdate", toTimestampWithoutTimeZone(sqlDateValue))
                .replace(":edate", toTimestampWithoutTimeZone(MIN_TIMESTAMP_VALUE));

        final String refStorageCode = toStorageCode(schemaName, refValue.getStorageCode());
        String sql = String.format(REFERENCE_VALUATION_SELECT_EXPRESSION,
                schemaName,
                addDoubleQuotes(refValue.getStorageCode()),
                REFERENCE_VALUATION_SELECT_TABLE_ALIAS,
                addDoubleQuotes(refValue.getKeyField()),
                sqlExpression,
                valueSubst,
                getFieldType(refStorageCode, refValue.getKeyField()),
                sqlByDate);

        return "(" + sql + ")";
    }

    @Override
    @Transactional
    public String updateData(String storageCode, RowValue rowValue) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        String updateKeys = ((List<FieldValue>) rowValue.getFieldValues()).stream()
                .map(fieldValue -> {
                    String quotedFieldName = addDoubleQuotes(fieldValue.getField());
                    String substValue = toUpdateValueSubst(schemaName, fieldValue);
                    return quotedFieldName + " = " + substValue;
                })
                .collect(joining(","));

        String condition = escapeFieldName(BASE_TABLE_ALIAS, SYS_PRIMARY_COLUMN) + " = " + QUERY_VALUE_SUBST;
        String sql = String.format(UPDATE_RECORD,
                schemaName, addDoubleQuotes(tableName), BASE_TABLE_ALIAS, updateKeys, condition) +
                "RETURNING " + addDoubleQuotes(SYS_HASH);
        Query query = entityManager.createNativeQuery(sql);

        int i = 1;
        for (FieldValue fieldValue : (List<FieldValue>) rowValue.getFieldValues()) {
            Serializable parameter = toQueryParameter(fieldValue);
            if (parameter != null) {
                query.setParameter(i++, parameter);
            }
        }
        query.setParameter(i, rowValue.getSystemId());

        return (String) query.getSingleResult();
    }

    private String toUpdateValueSubst(String schemaName, FieldValue fieldValue) {

        if (isFieldValueNull(fieldValue)) {
            return QUERY_NULL_VALUE;
        }

        if (fieldValue instanceof ReferenceFieldValue) {
            return getReferenceValuationSelect(schemaName, (ReferenceFieldValue) fieldValue, QUERY_VALUE_SUBST);
        }

        if (fieldValue instanceof TreeFieldValue) {
            return QUERY_LTREE_SUBST;
        }

        return QUERY_VALUE_SUBST;
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void deleteData(String storageCode) {

        String tableName = toTableName(storageCode);
        String sql = String.format(DELETE_RECORD, toSchemaName(storageCode), addDoubleQuotes(tableName));

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public List<String> deleteData(String storageCode, List<Object> systemIds) {

        if (isNullOrEmpty(systemIds))
            return emptyList();

        StorageDataCriteria criteria = new StorageDataCriteria(storageCode, null, null, null);
        criteria.setSystemIds(toLongSystemIds(systemIds));

        return deleteTableData(criteria);
    }

    /** Удаление данных таблицы по критерию. */
    protected List<String> deleteTableData(StorageDataCriteria criteria) {

        QueryWithParams where = getWhereClause(criteria, null);
        if (StringUtils.isNullOrEmpty(where.getSql()))
            return emptyList(); // Можно удалять, только если есть хотя бы одно ограничение!

        final String schemaName = getStorageCodeSchemaName(criteria.getStorageCode());
        final String tableName = toTableName(criteria.getStorageCode());

        String sql = String.format(DELETE_RECORD, schemaName, addDoubleQuotes(tableName)) +
                " WHERE " + where.getSql() +
                "RETURNING " + addDoubleQuotes(SYS_HASH);
        Query query = entityManager.createNativeQuery(sql);
        where.fillQueryParameters(query);

        return query.getResultList();
    }

    @Override
    @Transactional
    public void deleteEmptyRows(String draftCode) {

        List<String> fieldNames = getEscapedFieldNames(draftCode);
        if (isEmpty(fieldNames)) {
            deleteData(draftCode);
            return;
        }

        String condition = fieldNames.stream().map(s -> s + " IS NULL").collect(joining(" AND "));
        String sql = String.format(DELETE_RECORD,
                toSchemaName(draftCode), addDoubleQuotes(toTableName(draftCode))) +
                " WHERE " + condition;
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    @Transactional
    public void updateReferenceInRows(String storageCode, ReferenceFieldValue fieldValue, List<Object> systemIds) {

        if (getReferenceDisplayType(fieldValue.getValue()) == null)
            return;

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        String escapedFieldName = addDoubleQuotes(fieldValue.getField());
        String oldFieldExpression = sqlFieldExpression(fieldValue.getField(), REFERENCE_VALUATION_UPDATE_TABLE_ALIAS);
        String oldFieldValue = String.format(REFERENCE_VALUATION_OLD_VALUE, oldFieldExpression);
        String key = escapedFieldName + " = " + getReferenceValuationSelect(schemaName, fieldValue, oldFieldValue);

        String condition = escapeFieldName(REFERENCE_VALUATION_UPDATE_TABLE_ALIAS, SYS_PRIMARY_COLUMN) +
                " = " + String.format(TO_ANY_BIGINT, QUERY_VALUE_SUBST);
        String sql = String.format(UPDATE_RECORD,
                schemaName, addDoubleQuotes(tableName), REFERENCE_VALUATION_UPDATE_TABLE_ALIAS, key, condition);
        Query query = entityManager.createNativeQuery(sql);

        String ids = systemIds.stream().map(String::valueOf).collect(joining(","));
        query.setParameter(1, "{" + ids + "}");

        query.executeUpdate();
    }

    @Override
    public BigInteger countReferenceInRefRows(String storageCode, ReferenceFieldValue fieldValue) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        if (getReferenceDisplayType(fieldValue.getValue()) == null)
            return BigInteger.ZERO;

        Map<String, String> map = new HashMap<>();
        map.put("versionTable", escapeTableName(schemaName, tableName));
        map.put("versionAlias", VERSION_TABLE_ALIAS);
        map.put("refFieldName", addDoubleQuotes(fieldValue.getField()));

        String sql = substitute(COUNT_REFERENCE_IN_REF_ROWS, map);
        BigInteger count = (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();

        if (logger.isDebugEnabled()) {
            logger.debug("countReferenceInRefRows method count: {}, sql: {}", count, sql);
        }

        return count;
    }

    @Override
    @Transactional
    public void updateReferenceInRefRows(String storageCode, ReferenceFieldValue fieldValue, int offset, int limit) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        String oldFieldExpression = escapeFieldName(REFERENCE_VALUATION_UPDATE_TABLE_ALIAS, fieldValue.getField());
        String oldFieldValue = String.format(REFERENCE_VALUATION_OLD_VALUE, oldFieldExpression);
        String updateKey = addDoubleQuotes(fieldValue.getField()) + " = " +
                getReferenceValuationSelect(schemaName, fieldValue, oldFieldValue);

        Map<String, String> map = new HashMap<>();
        map.put("versionTable", escapeTableName(schemaName, tableName));
        map.put("versionAlias", VERSION_TABLE_ALIAS);
        map.put("refFieldName", addDoubleQuotes(fieldValue.getField()));
        map.put("limit", "" + limit);
        map.put("offset", "" + offset);

        String select = substitute(SELECT_REFERENCE_IN_REF_ROWS, map);
        String condition = escapeFieldName(REFERENCE_VALUATION_UPDATE_TABLE_ALIAS, SYS_PRIMARY_COLUMN) +
                " IN (" + select + ")";
        String sql = String.format(UPDATE_RECORD,
                schemaName, addDoubleQuotes(tableName), REFERENCE_VALUATION_UPDATE_TABLE_ALIAS, updateKey, condition);

        if (logger.isDebugEnabled()) {
            logger.debug("updateReferenceInRefRows method sql: {}", sql);
        }

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    public List<String> getFieldNames(String storageCode, String sqlSelect) {

        List<String> results = entityManager.createNativeQuery(sqlSelect)
                .setParameter(BIND_INFO_SCHEMA_NAME, toSchemaName(storageCode))
                .setParameter(BIND_INFO_TABLE_NAME, toTableName(storageCode))
                .getResultList();
        Collections.sort(results);

        return results;
    }

    @Override
    public List<String> getEscapedFieldNames(String storageCode) {
        return getFieldNames(storageCode, SELECT_ESCAPED_FIELD_NAMES + AND_INFO_COLUMN_NOT_IN_SYS_LIST);
    }

    @Override
    public List<String> getAllEscapedFieldNames(String storageCode) {
        return getFieldNames(storageCode, SELECT_ESCAPED_FIELD_NAMES);
    }

    @Override
    public List<String> getHashUsedFieldNames(String storageCode) {
        return getFieldNames(storageCode, SELECT_HASH_USED_FIELD_NAMES);
    }

    @Override
    public String getFieldType(String storageCode, String fieldName) {

        return entityManager.createNativeQuery(SELECT_FIELD_TYPE)
                .setParameter(BIND_INFO_SCHEMA_NAME, toSchemaName(storageCode))
                .setParameter(BIND_INFO_TABLE_NAME, toTableName(storageCode))
                .setParameter(BIND_INFO_COLUMN_NAME, fieldName)
                .getSingleResult().toString();
    }

    @Override
    public boolean isFieldNotNull(String storageCode, String fieldName) {

        String sql = String.format(IS_FIELD_NOT_NULL,
                toSchemaName(storageCode),
                addDoubleQuotes(toTableName(storageCode)),
                addDoubleQuotes(fieldName));
        return (boolean) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    public boolean isFieldContainNullValues(String storageCode, String fieldName) {

        String sql = String.format(IS_FIELD_CONTAIN_NULL_VALUES,
                toSchemaName(storageCode),
                addDoubleQuotes(toTableName(storageCode)),
                addDoubleQuotes(fieldName));
        return (boolean) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    public boolean isUnique(String storageCode, List<String> fieldNames, LocalDateTime publishTime) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        String fields = fieldNames.stream()
                .map(fieldName -> addDoubleQuotes(fieldName) + "\\:\\:text")
                .collect(joining(", "));
        String groupBy = IntStream.rangeClosed(1, fieldNames.size())
                .mapToObj(String::valueOf)
                .collect(joining(", "));

        QueryWithParams whereByDate = getWhereByDates(publishTime, null, DEFAULT_TABLE_ALIAS);
        String sqlByDate = (whereByDate == null || StringUtils.isNullOrEmpty(whereByDate.getSql())) ? "" : whereByDate.getSql();

        String sql = "SELECT " + fields + ", count(*) \n" +
                "  FROM " + escapeTableName(schemaName, tableName) + " AS " + DEFAULT_TABLE_ALIAS + QUERY_NEW_LINE +
                " WHERE true \n" + sqlByDate +
                " GROUP BY " + groupBy + QUERY_NEW_LINE +
                "HAVING count(*) > 1";
        Query query = entityManager.createNativeQuery(sql);

        if (whereByDate != null)
            whereByDate.fillQueryParameters(query);

        return query.getResultList().isEmpty();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRED)
    public void copyTableData(StorageCopyRequest request) {

        String sourceTable = escapeStorageTableName(request.getStorageCode());
        String targetTable = escapeStorageTableName(request.getPurposeCode());

        Map<String, String> mapSelect = new HashMap<>();
        mapSelect.put("sourceTable", sourceTable);
        mapSelect.put("sourceAlias", DEFAULT_TABLE_ALIAS);
        mapSelect.put("sourceColumns", aliasColumnName(DEFAULT_TABLE_ALIAS, "*"));

        String sqlSelect = substitute(SELECT_FROM_SOURCE_TABLE, mapSelect);

        QueryWithParams where = getWhereClause(request, DEFAULT_TABLE_ALIAS);
        if (!StringUtils.isNullOrEmpty(where.getSql())) {

            sqlSelect += " WHERE " + where.getBindedSql();
        }

        sqlSelect += sortingToOrderBy(null, DEFAULT_TABLE_ALIAS);

        List<String> fieldNames = request.getEscapedFieldNames();
        if (isNullOrEmpty(fieldNames)) {
            fieldNames = getAllEscapedFieldNames(request.getPurposeCode());
        }

        Map<String, String> mapInsert = new HashMap<>();
        mapInsert.put("targetTable", targetTable);
        mapInsert.put("strColumns", toStrColumns(fieldNames));
        mapInsert.put("rowColumns", toAliasColumns(fieldNames, ROW_TYPE_VAR_NAME + NAME_SEPARATOR));

        String sqlInsert = substitute(INSERT_INTO_TARGET_TABLE, mapInsert);

        Map<String, String> map = new HashMap<>();
        map.put("offset", "" + request.getOffset());
        map.put("limit", "" + request.getSize());
        map.put("sqlSelect", sqlSelect);
        map.put("sqlInsert", sqlInsert);

        String sql = substitute(INSERT_DATA_BY_SELECT_FROM_TABLE, map);

        if (logger.isDebugEnabled()) {
            logger.debug("copyTableData with method sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertAllDataFromDraft(String draftCode, String targetCode, List<String> fieldNames,
                                       int offset, int limit,
                                       LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String strColumns = toStrColumns(fieldNames);
        String rowColumns = toAliasColumns(fieldNames, ROW_TYPE_VAR_NAME + NAME_SEPARATOR);

        Map<String, String> map = new HashMap<>();
        map.put("offset", "" + offset);
        map.put("limit", "" + limit);
        map.put("draftTable", escapeStorageTableName(draftCode));
        map.put("draftAlias", DEFAULT_TABLE_ALIAS);
        map.put("targetTable", escapeStorageTableName(targetCode));
        map.put("targetSequence", escapeStorageSequenceName(targetCode));
        map.put("strColumns", strColumns);
        map.put("rowColumns", rowColumns);
        map.put("publishTime", formatDateTime(publishTime));
        map.put("closeTime", formatDateTime(closeTime));

        String sql = substitute(INSERT_ALL_VAL_FROM_DRAFT, map);

        if (logger.isDebugEnabled()) {
            logger.debug("insertDataFromDraft method sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    public BigInteger countActualDataFromVersion(String versionTable, String draftTable,
                                                 LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        Map<String, String> map = new HashMap<>();
        map.put("draftTable", escapeTableName(DATA_SCHEMA_NAME, draftTable));
        map.put("draftAlias", DRAFT_TABLE_ALIAS);
        map.put("versionTable", escapeTableName(DATA_SCHEMA_NAME, versionTable));
        map.put("versionAlias", VERSION_TABLE_ALIAS);
        map.put("publishTime", formatDateTime(publishTime));
        map.put("closeTime", formatDateTime(closeTime));

        String sql = substitute(COUNT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME, map);
        return (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertActualDataFromVersion(String targetTable, String versionTable,
                                            String draftTable, Map<String, String> typedNames,
                                            int offset, int limit,
                                            LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String strColumns = toStrColumns(typedNames);
        String typedColumns = toTypedColumns(typedNames);
        String draftColumns = toAliasColumns(typedNames, DRAFT_TABLE_ALIAS + NAME_SEPARATOR);

        Map<String, String> map = new HashMap<>();
        map.put("draftColumns", draftColumns);
        map.put("draftTable", escapeTableName(DATA_SCHEMA_NAME, draftTable));
        map.put("draftAlias", DRAFT_TABLE_ALIAS);
        map.put("versionTable", escapeTableName(DATA_SCHEMA_NAME, versionTable));
        map.put("versionAlias", VERSION_TABLE_ALIAS);
        map.put("publishTime", formatDateTime(publishTime));
        map.put("closeTime", formatDateTime(closeTime));
        map.put("offset", "" + offset);
        map.put("limit", "" + limit);
        map.put("targetTable", escapeTableName(DATA_SCHEMA_NAME, targetTable));
        map.put("targetSequence", escapeSchemaSequenceName(DATA_SCHEMA_NAME, targetTable));
        map.put("strColumns", strColumns);
        map.put("typedColumns", typedColumns);

        String sql = substitute(INSERT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME, map);

        if (logger.isDebugEnabled()) {
            logger.debug("insertActualDataFromVersion with sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    public BigInteger countOldDataFromVersion(String versionTable, String draftTable,
                                              LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String sql = String.format(COUNT_OLD_VAL_FROM_VERSION_WITH_CLOSE_TIME,
                addDoubleQuotes(versionTable),
                addDoubleQuotes(draftTable),
                formatDateTime(publishTime),
                formatDateTime(closeTime));
        return (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertOldDataFromVersion(String targetTable, String versionTable,
                                         String draftTable, List<String> fieldNames,
                                         int offset, int limit,
                                         LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String strColumns = toStrColumns(fieldNames);
        String rowColumns = toAliasColumns(fieldNames, ROW_TYPE_VAR_NAME + NAME_SEPARATOR);

        String sql = String.format(INSERT_OLD_VAL_FROM_VERSION_WITH_CLOSE_DATE,
                addDoubleQuotes(targetTable),
                addDoubleQuotes(versionTable),
                addDoubleQuotes(draftTable),
                offset,
                limit,
                escapeSequenceName(targetTable),
                strColumns,
                rowColumns,
                formatDateTime(publishTime),
                formatDateTime(closeTime));

        if (logger.isDebugEnabled()) {
            logger.debug("insertOldDataFromVersion with sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    public BigInteger countClosedNowDataFromVersion(String versionTable, String draftTable,
                                                    LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        Map<String, String> map = new HashMap<>();
        map.put("draftTable", escapeTableName(DATA_SCHEMA_NAME, draftTable));
        map.put("versionTable", escapeTableName(DATA_SCHEMA_NAME, versionTable));
        map.put("publishTime", formatDateTime(publishTime));
        map.put("closeTime", formatDateTime(closeTime));

        String sql = substitute(COUNT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME, map);
        return (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertClosedNowDataFromVersion(String targetTable, String versionTable,
                                               String draftTable, Map<String, String> typedNames,
                                               int offset, int limit,
                                               LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String strColumns = toStrColumns(typedNames);
        String typedColumns = toTypedColumns(typedNames);

        Map<String, String> map = new HashMap<>();
        map.put("targetTable", escapeTableName(DATA_SCHEMA_NAME, targetTable));
        map.put("draftTable", escapeTableName(DATA_SCHEMA_NAME, draftTable));
        map.put("versionTable", escapeTableName(DATA_SCHEMA_NAME, versionTable));
        map.put("publishTime", formatDateTime(publishTime));
        map.put("closeTime", formatDateTime(closeTime));
        map.put("strColumns", strColumns);
        map.put("offset", "" + offset);
        map.put("limit", "" + limit);
        map.put("typedColumns", typedColumns);
        map.put("sequenceName", escapeSchemaSequenceName(DATA_SCHEMA_NAME, targetTable));

        String sql = substitute(INSERT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME, map);

        if (logger.isDebugEnabled()) {
            logger.debug("insertClosedNowDataFromVersion with sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    public BigInteger countNewValFromDraft(String draftTable, String versionTable,
                                           LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        Map<String, String> map = new HashMap<>();
        map.put("draftTable", escapeTableName(DATA_SCHEMA_NAME, draftTable));
        map.put("versionTable", escapeTableName(DATA_SCHEMA_NAME, versionTable));
        map.put("publishTime", formatDateTime(publishTime));
        map.put("closeTime", formatDateTime(closeTime));

        String sql = substitute(COUNT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME, map);
        return (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertNewDataFromDraft(String targetTable, String versionTable,
                                       String draftTable, List<String> fieldNames,
                                       int offset, int limit,
                                       LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String strColumns = toStrColumns(fieldNames);
        String rowColumns = toAliasColumns(fieldNames, ROW_TYPE_VAR_NAME + NAME_SEPARATOR);

        Map<String, String> map = new HashMap<>();
        map.put("fields", strColumns);
        map.put("draftTable", escapeTableName(DATA_SCHEMA_NAME, draftTable));
        map.put("versionTable", escapeTableName(DATA_SCHEMA_NAME, versionTable));
        map.put("publishTime", formatDateTime(publishTime));
        map.put("closeTime", formatDateTime(closeTime));
        map.put("limit", "" + limit);
        map.put("offset", "" + offset);
        map.put("sequenceName", escapeSchemaSequenceName(DATA_SCHEMA_NAME, targetTable));
        map.put("targetTable", escapeTableName(DATA_SCHEMA_NAME, targetTable));
        map.put("rowFields", rowColumns);

        String sql = substitute(INSERT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME, map);

        if (logger.isDebugEnabled()) {
            logger.debug("insertNewDataFromDraft with sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void deletePointRows(String targetCode) {

        String condition = addDoubleQuotes(SYS_PUBLISHTIME) + " = " + addDoubleQuotes(SYS_CLOSETIME);
        String sql = String.format(DELETE_RECORD,
                toSchemaName(targetCode), addDoubleQuotes(toTableName(targetCode))) +
                " WHERE " + condition;

        if (logger.isDebugEnabled()) {
            logger.debug("deletePointRows method sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    public DataDifference getDataDifference(CompareDataCriteria criteria) {

        List<String> nonPrimaryFields = criteria.getFields().stream()
                .map(Field::getName)
                .filter(name -> !criteria.getPrimaryFields().contains(name))
                .collect(toList());

        String oldAlias = "t1";
        String oldStorageCode = criteria.getStorageCode();
        String oldSchemaName = toSchemaName(oldStorageCode);
        String oldTableName = toTableName(oldStorageCode);

        String newAlias = "t2";
        String newStorageCode = criteria.getNewStorageCode() != null ? criteria.getNewStorageCode() : oldStorageCode;
        String newSchemaName = toSchemaName(newStorageCode);
        String newTableName = toTableName(newStorageCode);

        String oldDataFields = getSelectFields(oldAlias, criteria.getFields(), false);
        String newDataFields = getSelectFields(newAlias, criteria.getFields(), false);

        String dataSelectFormat = "SELECT %1$s AS sysId1 \n %2$s\n, %3$s AS sysId2 \n %4$s \n";
        String dataSelect = String.format(dataSelectFormat,
                escapeFieldName(oldAlias, SYS_PRIMARY_COLUMN),
                StringUtils.isNullOrEmpty(oldDataFields) ? "" : ", " + oldDataFields,
                escapeFieldName(newAlias, SYS_PRIMARY_COLUMN),
                StringUtils.isNullOrEmpty(newDataFields) ? "" : ", " + newDataFields);

        String primaryEquality = criteria.getPrimaryFields().stream()
                .map(field -> escapeFieldName(oldAlias, field ) +
                        " = " + escapeFieldName(newAlias, field))
                .collect(joining(" AND ")) + QUERY_NEW_LINE;

        Map<String, Object> params = new HashMap<>();
        String oldPrimaryValuesFilter = makeFieldValuesFilter(oldAlias, params, criteria.getPrimaryFieldsFilters());
        String newPrimaryValuesFilter = makeFieldValuesFilter(newAlias, params, criteria.getPrimaryFieldsFilters());

        String nonPrimaryFieldsInequality = isEmpty(nonPrimaryFields)
                ? " AND false "
                : " AND (" + nonPrimaryFields.stream()
                .map(field -> escapeFieldName(oldAlias, field) +
                        " is distinct from " + escapeFieldName(newAlias, field))
                .collect(joining(" OR ")) +
                ") ";

        String oldPrimaryIsNull = criteria.getPrimaryFields().stream()
                .map(field -> escapeFieldName(oldAlias, field) + " is null ")
                .collect(joining(" AND "));
        String newPrimaryIsNull = criteria.getPrimaryFields().stream()
                .map(field -> escapeFieldName(newAlias, field ) + " is null ")
                .collect(joining(" AND "));

        final String datesFilterFormat =
                " and date_trunc('second', %1$s) <= :%2$s\\:\\:timestamp without time zone \n" +
                " and date_trunc('second', %3$s) >= :%4$s\\:\\:timestamp without time zone \n";

        String oldVersionDateFilter = "";
        if (criteria.getOldPublishDate() != null || criteria.getOldCloseDate() != null) {
            oldVersionDateFilter = String.format(datesFilterFormat,
                    escapeFieldName(oldAlias, SYS_PUBLISHTIME), "oldPublishDate",
                    escapeFieldName(oldAlias, SYS_CLOSETIME), "oldCloseDate");
            params.put("oldPublishDate", truncateDateTo(criteria.getOldPublishDate(), ChronoUnit.SECONDS, MIN_TIMESTAMP_VALUE));
            params.put("oldCloseDate", truncateDateTo(criteria.getOldCloseDate(), ChronoUnit.SECONDS, PG_MAX_TIMESTAMP));
        }

        String newVersionDateFilter = "";
        if (criteria.getNewPublishDate() != null || criteria.getNewCloseDate() != null) {
            newVersionDateFilter = String.format(datesFilterFormat,
                    escapeFieldName(newAlias, SYS_PUBLISHTIME), "newPublishDate",
                    escapeFieldName(newAlias, SYS_CLOSETIME), "newCloseDate");
            params.put("newPublishDate", truncateDateTo(criteria.getNewPublishDate(), ChronoUnit.SECONDS, MIN_TIMESTAMP_VALUE));
            params.put("newCloseDate", truncateDateTo(criteria.getNewCloseDate(), ChronoUnit.SECONDS, PG_MAX_TIMESTAMP));
        }

        String joinType = diffReturnTypeToJoinType(criteria.getReturnType());

        final String fromJoinFormat = "  FROM %1$s AS %2$s \n  %3$s JOIN %4$s AS %5$s \n    ON %6$s";
        String fromJoin = String.format(fromJoinFormat,
                escapeTableName(oldSchemaName, oldTableName), oldAlias, joinType,
                escapeTableName(newSchemaName, newTableName), newAlias, primaryEquality);

        String sql = fromJoin +
                " AND (true " + oldPrimaryValuesFilter + " OR true " + newPrimaryValuesFilter + ")" +
                oldVersionDateFilter +
                newVersionDateFilter +
                " WHERE ";

        if (criteria.getStatus() == null) {
            sql += oldPrimaryIsNull + newVersionDateFilter +
                    " OR " + newPrimaryIsNull + oldVersionDateFilter +
                    " OR (" + primaryEquality + nonPrimaryFieldsInequality + ") ";

        } else if (DiffStatusEnum.UPDATED.equals(criteria.getStatus())) {
            sql += primaryEquality + nonPrimaryFieldsInequality;

        } else if (DiffStatusEnum.INSERTED.equals(criteria.getStatus())) {
            sql += oldPrimaryIsNull + newVersionDateFilter;

        } else if (DiffStatusEnum.DELETED.equals(criteria.getStatus())) {
            sql += newPrimaryIsNull + oldVersionDateFilter;
        }

        QueryWithParams countQueryWithParams = new QueryWithParams(SELECT_COUNT_ONLY + sql, params);
        Query countQuery = countQueryWithParams.createQuery(entityManager);
        BigInteger count = (BigInteger) countQuery.getSingleResult();

        if (Boolean.TRUE.equals(criteria.getCountOnly())) {
            return new DataDifference(new CollectionPage<>(count.intValue(), null, criteria));
        }

        String orderBy = " ORDER BY " +
                criteria.getPrimaryFields().stream()
                        .map(field -> escapeFieldName(newAlias, field))
                        .collect(joining(",")) + "," +
                criteria.getPrimaryFields().stream()
                        .map(field -> escapeFieldName(oldAlias, field))
                        .collect(joining(","));

        QueryWithParams dataQueryWithParams = new QueryWithParams(dataSelect + sql + orderBy, params);
        Query dataQuery = dataQueryWithParams.createQuery(entityManager)
                .setFirstResult(criteria.getOffset())
                .setMaxResults(criteria.getSize());
        List<Object[]> resultList = dataQuery.getResultList();

        List<DiffRowValue> diffRowValues = toDiffRowValues(criteria.getFields(), resultList, criteria);
        return new DataDifference(new CollectionPage<>(count.intValue(), diffRowValues, criteria));
    }

    private String diffReturnTypeToJoinType(DiffReturnTypeEnum typeEnum) {
        switch (typeEnum) {
            case NEW: return "RIGHT";
            case OLD: return "LEFT";
            default: return "FULL";
        }
    }

    private String makeFieldValuesFilter(String alias, Map<String, Object> params,
                                         Set<List<FieldSearchCriteria>> fieldFilters) {

        QueryWithParams query = getWhereByFilters(fieldFilters, alias);
        if (query == null || StringUtils.isNullOrEmpty(query.getSql()))
            return "";

        if (!isNullOrEmpty(query.getParams())) {
            params.putAll(query.getParams());
        }

        return query.getSql();
    }

    // Используются для возможности переопределения схемы.
    protected String getSchemaName(String schemaName) {

        return getSchemaNameOrDefault(schemaName);
    }

    // Используются для возможности переопределения схемы.
    protected String getTableSchemaName(String schemaName, String tableName) {

        return !StringUtils.isNullOrEmpty(tableName) ? getSchemaNameOrDefault(schemaName) : DATA_SCHEMA_NAME;
    }

    protected String getStorageCodeSchemaName(String storageCode) {

        return getTableSchemaName(toSchemaName(storageCode), toTableName(storageCode));
    }
}
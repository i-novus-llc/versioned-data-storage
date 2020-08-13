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
import ru.i_novus.platform.datastorage.temporal.util.StringUtils;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;

import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.transaction.Transactional;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Collections.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.springframework.util.CollectionUtils.isEmpty;
import static ru.i_novus.platform.datastorage.temporal.model.StorageConstants.*;
import static ru.i_novus.platform.datastorage.temporal.util.CollectionUtils.isNullOrEmpty;
import static ru.i_novus.platform.datastorage.temporal.util.StorageUtils.*;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.*;

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

        final String schemaName = getTableSchemaName(criteria.getSchemaName(), criteria.getTableName());

        List<Field> fields = new ArrayList<>(criteria.getFields());
        fields.add(0, new IntegerField(SYS_PRIMARY_COLUMN));
        if (fields.stream().noneMatch(field -> SYS_HASH.equals(field.getName())))
            fields.add(1, new StringField(SYS_HASH));

        String sqlFields = getSelectFields(DEFAULT_TABLE_ALIAS, fields, true);

        final String sqlFormat = "SELECT %1$s \n  FROM %2$s as %3$s ";
        String sql = String.format(sqlFormat, sqlFields,
                escapeTableName(schemaName, criteria.getTableName()),
                DEFAULT_TABLE_ALIAS);

        QueryWithParams queryWithParams = new QueryWithParams(sql);
        queryWithParams.concat(getCriteriaWhereClause(criteria, DEFAULT_TABLE_ALIAS));

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

        final String schemaName = getTableSchemaName(criteria.getSchemaName(), criteria.getTableName());

        final String sqlFormat = "  FROM %s as %s\n";
        String sql = SELECT_COUNT_ONLY +
                String.format(sqlFormat,
                        escapeTableName(schemaName, criteria.getTableName()),
                        DEFAULT_TABLE_ALIAS);

        QueryWithParams queryWithParams = new QueryWithParams(sql);
        queryWithParams.concat(getCriteriaWhereClause(criteria, DEFAULT_TABLE_ALIAS));

        return (BigInteger) queryWithParams.createQuery(entityManager).getSingleResult();
    }

    /** Получение строки данных таблицы по системному идентификатору. */
    @Override
    public RowValue getRowData(String storageCode, List<String> fieldNames, Object systemId) {

        String schemaName = getSchemaName(toSchemaName(storageCode));
        String tableName = toTableName(storageCode);

        List<Field> fields = dataTypesToFields(getColumnDataTypes(storageCode), fieldNames);

        String sqlFields = getSelectFields(null, fields, true);
        String sql = String.format(SELECT_ROWS_FROM_DATA_BY_FIELD_ONE,
                sqlFields,
                schemaName,
                addDoubleQuotes(tableName),
                addDoubleQuotes(SYS_PRIMARY_COLUMN),
                QUERY_VALUE_SUBST);

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

        String schemaName = getSchemaName(toSchemaName(storageCode));
        String tableName = toTableName(storageCode);

        List<Field> fields = dataTypesToFields(getColumnDataTypes(storageCode), fieldNames);

        String sqlFields = getSelectFields(null, fields, true);
        String sql = String.format(SELECT_ROWS_FROM_DATA_BY_FIELD_ANY,
                sqlFields,
                schemaName,
                addDoubleQuotes(tableName),
                addDoubleQuotes(SYS_PRIMARY_COLUMN),
                QUERY_VALUE_SUBST);
        Query query = entityManager.createNativeQuery(sql);

        String ids = systemIds.stream().map(String::valueOf).collect(joining(","));
        query.setParameter(1, "{" + ids + "}");

        @SuppressWarnings("unchecked")
        List<Object[]> list = query.getResultList();
        return !isNullOrEmpty(list) ? toRowValues(fields, list) : emptyList();
    }

    private List<Field> dataTypesToFields(Map<String, String> dataTypes, List<String> fieldNames) {

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
    public List<String> findNonExistentHashes(String tableName, LocalDateTime bdate, LocalDateTime edate,
                                              List<String> hashList) {

        // Переделать на = ANY() для ускорения.
        // Получить те хеши, которые есть, и исключить их из всего списка.
        Map<String, Object> params = new HashMap<>();
        String sqlHashArray = "array[" + hashList.stream().map(hash -> {
            String hashPlaceHolder = "hash" + params.size();
            params.put(hashPlaceHolder, hash);
            return ":" + hashPlaceHolder;
        }).collect(joining(",")) + "]";

        QueryWithParams whereByDates = getWhereByDates(bdate, edate, DEFAULT_TABLE_ALIAS);
        String sqlByDate = (whereByDates == null || StringUtils.isNullOrEmpty(whereByDates.getSql())) ? "" : whereByDates.getSql();

        String sql = "SELECT hash \n" +
                "  FROM (\n" +
                "    SELECT unnest(" + sqlHashArray + ") hash \n" +
                "  ) hashes \n" +
                " WHERE hash NOT IN ( \n" +
                "   SELECT " + addDoubleQuotes(SYS_HASH) + " \n" +
                "     FROM " + escapeTableName(DATA_SCHEMA_NAME, tableName) +
                ALIAS_OPERATOR + DEFAULT_TABLE_ALIAS + " \n" +
                "    WHERE " + WHERE_DEFAULT +
                "   " + sqlByDate +
                " )";
        QueryWithParams queryWithParams = new QueryWithParams(sql, params);

        if (whereByDates != null) {
            queryWithParams.concat(whereByDates.getParams());
        }

        return queryWithParams.createQuery(entityManager).getResultList();
    }

    @Override
    public boolean tableStructureEquals(String tableName1, String tableName2) {

        Map<String, String> dataTypes1 = getColumnDataTypes(tableName1);
        Map<String, String> dataTypes2 = getColumnDataTypes(tableName2);
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
            if (!systemFieldList().contains(fieldName))
                map.put(fieldName, (String) dataType[1]);
        }
        return map;
    }

    private String sortingToOrderBy(Sorting sorting, String alias) {

        String orderBy = SELECT_ORDER;
        if (sorting != null && sorting.getField() != null) {
            orderBy = orderBy + formatFieldForQuery(sorting.getField(), alias) +
                    " " + sorting.getDirection().toString() + ", ";
        }

        return orderBy + " " + formatFieldForQuery(SYS_PRIMARY_COLUMN, alias);
    }

    private QueryWithParams getCriteriaWhereClause(StorageDataCriteria criteria, String alias) {

        StorageDataCriteria whereCriteria = new StorageDataCriteria(criteria);
        if (!isEmpty(criteria.getHashList())) {
            // Поиск только по списку hash:
            Set<List<FieldSearchCriteria>> filters = singleton(singletonList(
                    new FieldSearchCriteria(new StringField(SYS_HASH), SearchTypeEnum.EXACT, criteria.getHashList())
            ));
            whereCriteria.setFieldFilters(filters);
            whereCriteria.setSystemIds(null);
            whereCriteria.setHashList(null);
        }

        return getWhereClause(whereCriteria, alias);
    }

    private QueryWithParams getWhereClause(StorageDataCriteria criteria, String alias) {

        QueryWithParams query = new QueryWithParams(SELECT_WHERE_DEFAULT);
        query.concat(getWhereByDates(criteria.getBdate(), criteria.getEdate(), alias));
        query.concat(getWhereByFts(criteria.getCommonFilter(), alias));
        query.concat(getWhereByFilters(criteria.getFieldFilters(), alias));
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

            return WHERE_DEFAULT + String.join("\n", filters);
        })
                .filter(Objects::nonNull)
                .collect(joining(" or "));

        if (!"".equals(sql))
            sql = " and (" + sql + ")";

        return new QueryWithParams(sql, params);
    }

    private void toWhereClauseByFilter(FieldSearchCriteria searchCriteria,
                                       int index, String alias,
                                       List<String> filters, Map<String, Object> params) {

        Field field = searchCriteria.getField();

        String fieldName = searchCriteria.getField().getName();
        String escapedFieldName = escapeFieldName(alias, fieldName);

        if (searchCriteria.getValues() == null || searchCriteria.getValues().get(0) == null) {
            filters.add(" and " + escapedFieldName + " is null");
            return;
        }

        String indexedFieldName = fieldName + index;

        if (field instanceof IntegerField || field instanceof FloatField || field instanceof DateField) {
            filters.add(" and " + escapedFieldName + " in (:" + indexedFieldName + ")");
            params.put(indexedFieldName, searchCriteria.getValues());

        } else if (field instanceof ReferenceField) {
            filters.add(" and " + escapedFieldName + "->> 'value' in (:" + indexedFieldName + ")");
            params.put(indexedFieldName, searchCriteria.getValues().stream().map(Object::toString).collect(toList()));

        } else if (field instanceof TreeField) {
            if (SearchTypeEnum.LESS.equals(searchCriteria.getType())) {
                filters.add(" and " + escapedFieldName + "@> (cast(:" + indexedFieldName + " AS ltree[]))");
                String v = searchCriteria.getValues().stream()
                        .map(Object::toString)
                        .collect(joining(",", "{", "}"));
                params.put(indexedFieldName, v);
            }
        } else if (field instanceof BooleanField) {
            if (searchCriteria.getValues().size() == 1) {
                filters.add(" and " + escapedFieldName +
                        (Boolean.TRUE.equals(searchCriteria.getValues().get(0)) ? " IS TRUE " : " IS NOT TRUE")
                );
            }
        } else if (field instanceof StringField) {
            if (SearchTypeEnum.LIKE.equals(searchCriteria.getType()) && searchCriteria.getValues().size() == 1) {
                filters.add(" and lower(" + escapedFieldName + ") like :" + indexedFieldName + "");
                params.put(indexedFieldName, "%" + searchCriteria.getValues().get(0).toString().trim().toLowerCase() + "%");
            } else {
                filters.add(" and " + escapedFieldName + " in (:" + indexedFieldName + ")");
                params.put(indexedFieldName, searchCriteria.getValues());
            }
        } else {
            params.put(indexedFieldName, searchCriteria.getValues());
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
                List<Object> typedValues = (List<Object>) typedCriteria.getValues();
                typedValues.addAll(criteria.getValues());
            }
        }

        return typedGroup.values().stream().map(Map::values).flatMap(Collection::stream).collect(toList());
    }

    private QueryWithParams getWhereBySystemIds(List<Long> systemIds, String alias) {

        if (isNullOrEmpty(systemIds))
            return null;

        Map<String, Object> params = new HashMap<>();
        String escapedColumn = escapeFieldName(alias, SYS_PRIMARY_COLUMN);

        String sql;
        if (systemIds.size() > 1) {
            sql = " and (" + escapedColumn + " in (:systemIds))";
            params.put("systemIds", systemIds);

        } else {
            sql = " and (" + escapedColumn + " = :systemId)";
            params.put("systemId", systemIds.get(0));
        }

        return new QueryWithParams(sql, params);
    }

    @Override
    public BigInteger countData(String tableName) {

        String sql = SELECT_COUNT_ONLY + SELECT_FROM + escapeTableName(DATA_SCHEMA_NAME, tableName);
        return (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();
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

        String ddl = String.format(DROP_TABLE, schemaName, addDoubleQuotes(tableName));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    protected void createTableSequence(String storageCode) {

        String ddl = String.format(CREATE_TABLE_SEQUENCE, escapeStorageSequenceName(storageCode));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    protected void dropTableSequence(String storageCode) {

        String ddl = String.format(DROP_TABLE_SEQUENCE, escapeStorageSequenceName(storageCode));
        entityManager.createNativeQuery(ddl).executeUpdate();
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

        String condition = String.format(TO_ANY_TEXT, QUERY_VALUE_SUBST);
        String sql = SELECT_EXISTENT_SCHEMA_NAME_LIST + condition;

        Query query = entityManager.createNativeQuery(sql);
        query.setParameter(1, "{" + String.join(",", schemaNames) + "}");

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
    }

    /** Добавление SYS_PRIMARY_COLUMN. */
    private void addPrimaryKey(String storageCode) {

        createTableSequence(storageCode);

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        String ddlAddPrimaryKey = String.format(ALTER_ADD_PRIMARY_KEY,
                schemaName, addDoubleQuotes(tableName), SYS_PRIMARY_COLUMN);
        entityManager.createNativeQuery(ddlAddPrimaryKey).executeUpdate();

        String ddlAlterColumn = String.format(ALTER_SET_SEQUENCE_FOR_PRIMARY_KEY,
                schemaName, addDoubleQuotes(tableName), SYS_PRIMARY_COLUMN, escapeSequenceName(tableName));
        entityManager.createNativeQuery(ddlAlterColumn).executeUpdate();
    }

    /** Копирование всех индексов (кроме индекса для SYS_HASH). */
    private void copyIndexes(String sourceCode, String targetCode) {

        String sourceSchema = toSchemaName(sourceCode);
        String sourceTable = toTableName(sourceCode);

        String sql = SELECT_DDL_INDEXES +
                String.format(AND_DDL_INDEX_NOT_LIKE, addDoubleQuotes(SYS_HASH));
        List<String> ddlIndexes = entityManager.createNativeQuery(sql)
                .setParameter(BIND_INFO_SCHEMA_NAME, sourceSchema)
                .setParameter(BIND_INFO_TABLE_NAME, sourceTable)
                .getResultList();

        String targetSchema = toSchemaName(targetCode);
        String targetTable = toTableName(targetCode);

        for (String ddlIndex : ddlIndexes) {
            String ddl = ddlIndex.replace(
                    escapeTableName(sourceSchema, sourceTable),
                    escapeTableName(targetSchema, targetTable))
                    .replace(sourceTable, targetTable);
            entityManager.createNativeQuery(ddl).executeUpdate();
        }
    }

    @Override
    @Transactional
    public void addColumn(String storageCode, String name, String type, String defaultValue) {

        String condition = (defaultValue != null) ? String.format(COLUMN_DEFAULT, defaultValue) : "";
        String ddl = String.format(ALTER_ADD_COLUMN,
                toSchemaName(storageCode), addDoubleQuotes(toTableName(storageCode)),
                addDoubleQuotes(name), type, condition);

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
    public void insertData(String storageCode, List<RowValue> data) {

        if (isEmpty(data))
            return;

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        List<FieldValue> fieldValues = data.get(0).getFieldValues();
        List<String> insertKeyList = fieldValues.stream()
                .map(fieldValue -> addDoubleQuotes(fieldValue.getField()))
                .collect(toList());

        List<String> substList = data.stream()
                .map(rowValue -> {
                    List<String> substValues =
                            ((List<FieldValue>) rowValue.getFieldValues()).stream()
                                    .map(fieldValue -> toInsertValueSubst(schemaName, fieldValue))
                                    .collect(toList());
                    return String.join(",", substValues);
                })
                .collect(toList());

        int batchSize = 500;
        String keys = String.join(",", insertKeyList);

        for (int firstIndex = 0, nextIndex = batchSize;
             firstIndex < substList.size();
             firstIndex = nextIndex, nextIndex = firstIndex + batchSize) {

            int valueCount = Math.min(nextIndex, substList.size());
            insertData(schemaName, tableName, keys,
                    substList.subList(firstIndex, valueCount),
                    data.subList(firstIndex, valueCount));
        }
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

    private void insertData(String schemaName, String tableName, String keys,
                            List<String> subst, List<RowValue> data) {

        String sql = String.format(INSERT_RECORD,
                schemaName, addDoubleQuotes(tableName), keys) +
                String.format(INSERT_VALUES, String.join("),(", subst));
        Query query = entityManager.createNativeQuery(sql);

        int i = 1;
        for (RowValue rowValue : data) {
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
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void loadData(String draftTable, String sourceTable, List<String> fields,
                         LocalDateTime fromDate, LocalDateTime toDate) {

        String keys = String.join(",", fields);
        String values = fields.stream()
                .map(f -> DEFAULT_TABLE_ALIAS + NAME_SEPARATOR + f)
                .collect(joining(","));

        String sql = String.format(INSERT_RECORD, DATA_SCHEMA_NAME, addDoubleQuotes(draftTable), keys) +
                String.format(INSERT_SELECT, DATA_SCHEMA_NAME, addDoubleQuotes(sourceTable), DEFAULT_TABLE_ALIAS, values);

        QueryWithParams queryWithParams = new QueryWithParams(sql);
        queryWithParams.concat(getWhereByDates(fromDate, toDate, DEFAULT_TABLE_ALIAS));

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

        String sql = String.format(REFERENCE_VALUATION_SELECT_EXPRESSION,
                schemaName,
                addDoubleQuotes(refValue.getStorageCode()),
                REFERENCE_VALUATION_SELECT_TABLE_ALIAS,
                addDoubleQuotes(refValue.getKeyField()),
                sqlExpression,
                valueSubst,
                getFieldType(schemaName, refValue.getStorageCode(), refValue.getKeyField()),
                sqlByDate);

        return "(" + sql + ")";
    }

    @Override
    @Transactional
    public void updateData(String storageCode, RowValue rowValue) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        List<String> updateKeyList = ((List<FieldValue>) rowValue.getFieldValues()).stream()
                .map(fieldValue -> {
                    String quotedFieldName = addDoubleQuotes(fieldValue.getField());
                    String substValue = toUpdateValueSubst(schemaName, fieldValue);
                    return String.format(UPDATE_VALUE, quotedFieldName, substValue);
                })
                .collect(toList());

        final String tableAlias = "b";
        String keys = String.join(",", updateKeyList);
        String condition = String.format(CONDITION_EQUAL,
                escapeFieldName(tableAlias, SYS_PRIMARY_COLUMN), QUERY_VALUE_SUBST);
        String sql = String.format(UPDATE_RECORD,
                schemaName, addDoubleQuotes(tableName), tableAlias, keys, condition);
        Query query = entityManager.createNativeQuery(sql);

        int i = 1;
        for (Object obj : rowValue.getFieldValues()) {
            FieldValue fieldValue = (FieldValue) obj;
            if (fieldValue.getValue() != null) {
                if (fieldValue instanceof ReferenceFieldValue) {
                    if (((ReferenceFieldValue) fieldValue).getValue().getValue() != null)
                        query.setParameter(i++, ((ReferenceFieldValue) fieldValue).getValue().getValue());
                } else {
                    query.setParameter(i++, fieldValue.getValue());
                }
            }
        }
        query.setParameter(i, rowValue.getSystemId());

        query.executeUpdate();
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
        String sql = String.format(DELETE_RECORD,
                toSchemaName(storageCode), addDoubleQuotes(tableName), WHERE_DEFAULT);

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void deleteData(String storageCode, List<Object> systemIds) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        String ids = systemIds.stream().map(id -> QUERY_VALUE_SUBST).collect(joining(","));
        String condition = String.format(CONDITION_IN, addDoubleQuotes(SYS_PRIMARY_COLUMN), ids);

        String sql = String.format(DELETE_RECORD, schemaName, addDoubleQuotes(tableName), condition);
        Query query = entityManager.createNativeQuery(sql);

        int i = 1;
        for (Object systemId : systemIds) {
            query.setParameter(i++, systemId);
        }
        query.executeUpdate();
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
        String key = String.format(UPDATE_VALUE,
                escapedFieldName, getReferenceValuationSelect(schemaName, fieldValue, oldFieldValue));

        String condition = String.format(CONDITION_EQUAL,
                escapeFieldName(REFERENCE_VALUATION_UPDATE_TABLE_ALIAS, SYS_PRIMARY_COLUMN),
                String.format(TO_ANY_BIGINT, QUERY_VALUE_SUBST)
        );
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
        String updateKey = String.format(UPDATE_VALUE,
                addDoubleQuotes(fieldValue.getField()),
                getReferenceValuationSelect(schemaName, fieldValue, oldFieldValue));

        Map<String, String> map = new HashMap<>();
        map.put("versionTable", escapeTableName(schemaName, tableName));
        map.put("versionAlias", VERSION_TABLE_ALIAS);
        map.put("refFieldName", addDoubleQuotes(fieldValue.getField()));
        map.put("limit", "" + limit);
        map.put("offset", "" + offset);

        String select = substitute(SELECT_REFERENCE_IN_REF_ROWS, map);
        String condition = String.format(CONDITION_IN,
                escapeFieldName(REFERENCE_VALUATION_UPDATE_TABLE_ALIAS, SYS_PRIMARY_COLUMN), select);
        String sql = String.format(UPDATE_RECORD,
                schemaName, addDoubleQuotes(tableName), REFERENCE_VALUATION_UPDATE_TABLE_ALIAS, updateKey, condition);

        if (logger.isDebugEnabled()) {
            logger.debug("updateReferenceInRefRows method sql: {}", sql);
        }

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    @Transactional
    public void deleteEmptyRows(String draftCode) {

        List<String> fieldNames = getEscapedFieldNames(draftCode);
        if (isEmpty(fieldNames)) {
            deleteData(draftCode);

            return;
        }

        String condition = fieldNames.stream()
                .map(s -> s + " IS NULL")
                .collect(joining(" AND "));

        String sql = String.format(DELETE_RECORD, DATA_SCHEMA_NAME, addDoubleQuotes(draftCode), condition);
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    public boolean isUnique(String storageCode, List<String> fieldNames, LocalDateTime publishTime) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        String fields = fieldNames.stream()
                .map(fieldName -> addDoubleQuotes(fieldName) + "\\:\\:text")
                .collect(joining(","));
        String groupBy = Stream.iterate(1, n -> n + 1).limit(fieldNames.size())
                .map(String::valueOf)
                .collect(joining(","));

        QueryWithParams whereByDate = getWhereByDates(publishTime, null, DEFAULT_TABLE_ALIAS);
        String sqlByDate = (whereByDate == null || StringUtils.isNullOrEmpty(whereByDate.getSql())) ? "" : whereByDate.getSql();

        String sql = "SELECT " + fields + ", COUNT(*)" + "\n" +
                "  FROM " + escapeTableName(schemaName, tableName) +
                " as " + DEFAULT_TABLE_ALIAS + "\n" +
                " WHERE " + WHERE_DEFAULT + sqlByDate +
                " GROUP BY " + groupBy + "\n" +
                "HAVING COUNT(*) > 1";
        Query query = entityManager.createNativeQuery(sql);

        if (whereByDate != null)
            whereByDate.fillQueryParameters(query);

        return query.getResultList().isEmpty();
    }


    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void updateSequence(String tableName) {

        String sql = String.format("SELECT setval('%1$s.%2$s', (SELECT max(\"SYS_RECORDID\") FROM %1$s.%3$s))",
                DATA_SCHEMA_NAME, escapeSequenceName(tableName), addDoubleQuotes(tableName));
        entityManager.createNativeQuery(sql).getSingleResult();
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
        String tableFields = fieldNames.stream().map(this::getFieldClearName).collect(joining(", "));

        String expression = String.format(HASH_EXPRESSION,
                fieldNames.stream().map(field -> alias + field).collect(joining(", ")));
        String triggerBody = String.format(ASSIGN_FIELD, alias + addDoubleQuotes(SYS_HASH), expression);
        String ddl = String.format(CREATE_TRIGGER, schemaName, tableName,
                HASH_FUNCTION_NAME, HASH_TRIGGER_NAME, tableFields, triggerBody + ";");
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    protected void createFtsTrigger(String storageCode, List<String> fieldNames) {

        String schemaName = toSchemaName(storageCode);
        String tableName = toTableName(storageCode);

        final String alias = TRIGGER_NEW_ALIAS + NAME_SEPARATOR;
        String tableFields = fieldNames.stream().map(this::getFieldClearName).collect(joining(", "));

        String expression = fieldNames.stream()
                .map(field -> "coalesce( to_tsvector('ru', " + alias + field + "\\:\\:text),'')")
                .collect(joining(" || ' ' || "));
        String triggerBody = String.format(ASSIGN_FIELD, alias + addDoubleQuotes(SYS_FTS), expression);
        String ddl = String.format(CREATE_TRIGGER, schemaName, tableName,
                FTS_FUNCTION_NAME, FTS_TRIGGER_NAME, tableFields, triggerBody + ";");
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    /** Получение наименования поля с кавычками из наименования, сформированного по getHashUsedFieldNames. */
    private String getFieldClearName(String fieldName) {

        int closeQuoteIndex = fieldName.indexOf('"', 1);
        return fieldName.substring(0, closeQuoteIndex + 1);
    }

    @Override
    @Transactional
    public void updateHashRows(String tableName, List<String> fieldNames) {

        String expression = String.format(HASH_EXPRESSION, String.join(", ", fieldNames));
        String ddlAssign = String.format(ASSIGN_FIELD, addDoubleQuotes(SYS_HASH), expression);

        String ddl = String.format(UPDATE_FIELD, DATA_SCHEMA_NAME, addDoubleQuotes(tableName), ddlAssign);
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @Transactional
    public void updateFtsRows(String tableName, List<String> fieldNames) {

        String expression = fieldNames.stream()
                .map(field -> "coalesce( to_tsvector('ru', " + field + "\\:\\:text),'')")
                .collect(joining(" || ' ' || "));
        String ddlAssign = String.format(ASSIGN_FIELD, addDoubleQuotes(SYS_FTS), expression);

        String ddl = String.format(UPDATE_FIELD, DATA_SCHEMA_NAME, addDoubleQuotes(tableName), ddlAssign);
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void dropTriggers(String tableName) {

        String escapedTableName = addDoubleQuotes(tableName);

        String dropHashTrigger = String.format(DROP_TRIGGER, HASH_TRIGGER_NAME, DATA_SCHEMA_NAME, escapedTableName);
        entityManager.createNativeQuery(dropHashTrigger).executeUpdate();

        String dropFtsTrigger = String.format(DROP_TRIGGER, FTS_TRIGGER_NAME, DATA_SCHEMA_NAME, escapedTableName);
        entityManager.createNativeQuery(dropFtsTrigger).executeUpdate();
    }

    @Override
    @Transactional
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createIndex(String storageCode, String name, List<String> fields) {

        String ddl = String.format(CREATE_TABLE_INDEX,
                name,
                toSchemaName(storageCode),
                addDoubleQuotes(toTableName(storageCode)),
                fields.stream().map(StringUtils::addDoubleQuotes).collect(joining(",")));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createFullTextSearchIndex(String storageCode) {

        String tableName = toTableName(storageCode);

        String ddl = String.format(CREATE_FTS_INDEX,
                escapeTableIndexName(tableName, TABLE_INDEX_FTS_NAME),
                toSchemaName(storageCode),
                addDoubleQuotes(tableName),
                addDoubleQuotes(SYS_FTS));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createLtreeIndex(String storageCode, String field) {

        String tableName = toTableName(storageCode);

        String ddl = String.format(CREATE_LTREE_INDEX,
                escapeTableIndexName(tableName, field.toLowerCase()),
                toSchemaName(storageCode),
                addDoubleQuotes(tableName),
                addDoubleQuotes(field));
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
                addDoubleQuotes(SYS_HASH));
        entityManager.createNativeQuery(ddl).executeUpdate();
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

        return getFieldType(toSchemaName(storageCode), toTableName(storageCode), fieldName);
    }

    private String getFieldType(String schemaName, String tableName, String columnName) {

        return entityManager.createNativeQuery(SELECT_FIELD_TYPE)
                .setParameter(BIND_INFO_SCHEMA_NAME, schemaName)
                .setParameter(BIND_INFO_TABLE_NAME, tableName)
                .setParameter(BIND_INFO_COLUMN_NAME, columnName)
                .getSingleResult().toString();
    }

    @Override
    public void alterDataType(String tableName, String field, String oldType, String newType) {

        String escapedField = addDoubleQuotes(field);
        String using = "";
        if (DateField.TYPE.equals(oldType) && isVarcharType(newType)) {
            using = "to_char(" + escapedField + ", '" + QUERY_DATE_FORMAT + "')";

        } else if (DateField.TYPE.equals(newType) && StringField.TYPE.equals(oldType)) {
            using = "to_date(" + escapedField + ", '" + QUERY_DATE_FORMAT + "')";

        } else if (ReferenceField.TYPE.equals(oldType)) {
            using = "(" + escapedField +
                    REFERENCE_FIELD_VALUE_OPERATOR + addSingleQuotes(REFERENCE_VALUE_NAME) +
                    ")" + "\\:\\:varchar\\:\\:" + newType;

        } else if (ReferenceField.TYPE.equals(newType)) {
            using = String.format("nullif(jsonb_build_object(%1$s, %2$s), jsonb_build_object(%1$s, null))",
                    addSingleQuotes(REFERENCE_VALUE_NAME),
                    escapedField);

        } else if (isVarcharType(oldType) || isVarcharType(newType)) {
            using = escapedField + "\\:\\:" + newType;

        } else {
            using = escapedField + "\\:\\:varchar\\:\\:" + newType;
        }

        String ddl = String.format(ALTER_COLUMN_WITH_USING,
                DATA_SCHEMA_NAME,
                addDoubleQuotes(tableName),
                escapedField, newType, using);
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    public boolean isFieldNotEmpty(String tableName, String fieldName) {

        String sql = String.format(IS_FIELD_NOT_EMPTY,
                addDoubleQuotes(tableName),
                addDoubleQuotes(tableName),
                addDoubleQuotes(fieldName));
        return (boolean) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    public boolean isFieldContainEmptyValues(String tableName, String fieldName) {

        String sql = String.format(IS_FIELD_CONTAIN_EMPTY_VALUES,
                addDoubleQuotes(tableName),
                addDoubleQuotes(tableName),
                addDoubleQuotes(fieldName));
        return (boolean) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void copyTableData(String sourceCode, String targetCode, int offset, int limit) {

        String sourceTable = escapeStorageTableName(sourceCode);

        Map<String, String> mapSelect = new HashMap<>();
        mapSelect.put("sourceTable", sourceTable);
        mapSelect.put("sourceAlias", DEFAULT_TABLE_ALIAS);

        String sqlSelect = substitute(SELECT_ALL_DATA_BY_FROM_TABLE, mapSelect);

        List<String> fieldNames = getAllEscapedFieldNames(sourceCode);

        Map<String, String> mapInsert = new HashMap<>();
        mapInsert.put("targetTable", escapeStorageTableName(targetCode));
        mapInsert.put("strColumns", toStrColumns(fieldNames));
        mapInsert.put("rowColumns", toRowColumns(fieldNames));

        String sqlInsert = substitute(INSERT_ALL_DATA_BY_FROM_TABLE, mapInsert);

        Map<String, String> map = new HashMap<>();
        map.put("offset", "" + offset);
        map.put("limit", "" + limit);
        map.put("sourceTable", sourceTable);
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
        String rowColumns = toRowColumns(fieldNames);

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
                                            String draftTable, Map<String, String> fieldNames,
                                            int offset, int limit,
                                            LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String strColumns = fieldNames.keySet().stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        String typedColumns = fieldNames.keySet().stream().map(s -> s + " " + fieldNames.get(s)).reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        String draftColumns = fieldNames.keySet().stream().map(s -> DRAFT_TABLE_ALIAS + "." + s + "").reduce((s1, s2) -> s1 + ", " + s2).orElse("");

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
        String rowColumns = toRowColumns(fieldNames);

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
                                               String draftTable, Map<String, String> fieldNames,
                                               int offset, int limit,
                                               LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String strColumns = fieldNames.keySet().stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        String typedColumns = fieldNames.keySet().stream().map(s -> s + " " + fieldNames.get(s)).reduce((s1, s2) -> s1 + ", " + s2).orElse("");

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
        String rowColumns = toRowColumns(fieldNames);

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

    private String toStrColumns(List<String> columns) {

        return columns.stream().map(s -> "" + s + "")
                .reduce((s1, s2) -> s1 + ", " + s2).orElse("");
    }

    private String toRowColumns(List<String> columns) {

        return columns.stream().map(s -> ROW_TYPE_VAR_NAME + "." + s + "")
                .reduce((s1, s2) -> s1 + ", " + s2).orElse("");
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void deletePointRows(String targetCode) {

        String tableName = toTableName(targetCode);

        String condition = String.format(CONDITION_EQUAL,
                addDoubleQuotes(SYS_PUBLISHTIME), addDoubleQuotes(SYS_CLOSETIME));
        String sql = String.format(DELETE_RECORD,
                toSchemaName(targetCode), addDoubleQuotes(tableName), condition);

        if (logger.isDebugEnabled()) {
            logger.debug("deletePointRows method sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
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

        String dataSelectFormat = "SELECT %1$s as sysId1 \n %2$s\n, %3$s as sysId2 \n %4$s \n";
        String dataSelect = String.format(dataSelectFormat,
                escapeFieldName(oldAlias, SYS_PRIMARY_COLUMN),
                StringUtils.isNullOrEmpty(oldDataFields) ? "" : ", " + oldDataFields,
                escapeFieldName(newAlias, SYS_PRIMARY_COLUMN),
                StringUtils.isNullOrEmpty(newDataFields) ? "" : ", " + newDataFields);

        String primaryEquality = criteria.getPrimaryFields().stream()
                .map(f -> formatFieldForQuery(f, oldAlias) +
                        " = " + formatFieldForQuery(f, newAlias))
                .collect(joining(" and ")) + "\n";

        Map<String, Object> params = new HashMap<>();
        String oldPrimaryValuesFilter = makeFieldValuesFilter(oldAlias, params, criteria.getPrimaryFieldsFilters());
        String newPrimaryValuesFilter = makeFieldValuesFilter(newAlias, params, criteria.getPrimaryFieldsFilters());

        String nonPrimaryFieldsInequality = isEmpty(nonPrimaryFields)
                ? " and false "
                : " and (" + nonPrimaryFields.stream()
                .map(f -> formatFieldForQuery(f, oldAlias) +
                        " is distinct from " + formatFieldForQuery(f, newAlias))
                .collect(joining(" or ")) +
                ") ";

        String oldPrimaryIsNull = criteria.getPrimaryFields().stream()
                .map(f -> formatFieldForQuery(f, oldAlias) + " is null ")
                .collect(joining(" and "));
        String newPrimaryIsNull = criteria.getPrimaryFields().stream()
                .map(f -> formatFieldForQuery(f, newAlias) + " is null ")
                .collect(joining(" and "));

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

        final String fromFormat = "  from %1$s as %2$s \n  %3$s join %4$s as %5$s \n  on %6$s";
        String from = String.format(fromFormat,
                escapeTableName(oldSchemaName, oldTableName), oldAlias, joinType,
                escapeTableName(newSchemaName, newTableName), newAlias, primaryEquality);

        String sql = from +
                " and (true" + oldPrimaryValuesFilter +
                " or true" + newPrimaryValuesFilter + ")" +
                oldVersionDateFilter +
                newVersionDateFilter +
                " where ";

        if (criteria.getStatus() == null)
            sql += oldPrimaryIsNull + newVersionDateFilter +
                    " or " + newPrimaryIsNull + oldVersionDateFilter +
                    " or (" + primaryEquality + nonPrimaryFieldsInequality + ") ";

        else if (DiffStatusEnum.UPDATED.equals(criteria.getStatus())) {
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

        String orderBy = SELECT_ORDER +
                criteria.getPrimaryFields().stream()
                        .map(f -> formatFieldForQuery(f, newAlias))
                        .collect(joining(",")) + "," +
                criteria.getPrimaryFields().stream()
                        .map(f -> formatFieldForQuery(f, oldAlias))
                        .collect(joining(","));

        QueryWithParams dataQueryWithParams = new QueryWithParams(dataSelect + sql + orderBy, params);
        Query dataQuery = dataQueryWithParams.createQuery(entityManager)
                .setFirstResult(criteria.getOffset())
                .setMaxResults(criteria.getSize());
        List<Object[]> resultList = dataQuery.getResultList();

        List<DiffRowValue> diffRowValues = toDiffRowValues(fields, fieldMap, resultList, criteria);
        return new DataDifference(new CollectionPage<>(count.intValue(), diffRowValues, criteria));
    }

    private String diffReturnTypeToJoinType(DiffReturnTypeEnum typeEnum) {
        switch (typeEnum) {
            case NEW: return "right";
            case OLD: return "left";
            default: return "full";
        }
    }

    // Используются для возможности переопределения схемы.
    protected String getSchemaName(String schemaName) {

        return getSchemaNameOrDefault(schemaName);
    }

    // Используются для возможности переопределения схемы.
    protected String getTableSchemaName(String schemaName, String tableName) {

        return !StringUtils.isNullOrEmpty(tableName) ? getSchemaNameOrDefault(schemaName) : DATA_SCHEMA_NAME;
    }

    private List<DiffRowValue> toDiffRowValues(List<String> fields, Map<String, Field> fieldMap,
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

                if (primaryFields.contains(field)) {
                    rowStatus = diffFieldValueToStatusEnum(fieldValue, rowStatus);
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

    private DiffStatusEnum diffFieldValueToStatusEnum(DiffFieldValue value, DiffStatusEnum defaultValue) {

        if (value.getOldValue() == null) {
            return DiffStatusEnum.INSERTED;
        }

        if (value.getNewValue() == null) {
            return DiffStatusEnum.DELETED;
        }

        if (value.getOldValue().equals(value.getNewValue())) {
            return DiffStatusEnum.UPDATED;
        }

        return defaultValue;
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
}
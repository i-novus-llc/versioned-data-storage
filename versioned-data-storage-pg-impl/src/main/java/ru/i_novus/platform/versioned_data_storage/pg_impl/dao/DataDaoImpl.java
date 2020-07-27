package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import net.n2oapp.criteria.api.CollectionPage;
import net.n2oapp.criteria.api.Sorting;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.i_novus.platform.datastorage.temporal.enums.DiffReturnTypeEnum;
import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.enums.ReferenceDisplayType;
import ru.i_novus.platform.datastorage.temporal.model.*;
import ru.i_novus.platform.datastorage.temporal.model.criteria.*;
import ru.i_novus.platform.datastorage.temporal.model.value.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.util.DataUtil;

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
import java.util.stream.Stream;

import static java.util.Collections.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.springframework.util.CollectionUtils.isEmpty;
import static ru.i_novus.platform.datastorage.temporal.CollectionUtils.isNullOrEmpty;
import static ru.i_novus.platform.datastorage.temporal.model.DataConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.DataUtil.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.*;

public class DataDaoImpl implements DataDao {

    private static final Logger logger = LoggerFactory.getLogger(DataDaoImpl.class);

    private static final LocalDateTime PG_MAX_TIMESTAMP = LocalDateTime.of(294276, 12, 31, 23, 59);
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DATETIME_FORMAT);

    private static final Pattern DATE_PATTERN = Pattern.compile("([0-9]{2})\\.([0-9]{2})\\.([0-9]{4})");

    private EntityManager entityManager;

    public DataDaoImpl(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    private String formatDateTime(LocalDateTime localDateTime) {
        return (localDateTime != null) ? localDateTime.format(DATETIME_FORMATTER) : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<RowValue> getData(DataCriteria criteria) {

        // В l10n перенести подмену схемы при отсутствии схемы/таблицы.
        // Здесь оставить только использование схемы по умолчанию, если она не была задана.
        // Схемы предлагается именовать в виде "data_" + код локализации, чтобы исключить случайное совпадение с другими схемами.
        // Код локализации должен содержать только строчные латинские буквы a-z, цифры 0-9 и символ подчёркивания "_".
        // В postgres макс. длина имени = NAMEDATALEN - 1 = 64 - 1, поэтому длина кода локализации должна быть <= 64 - 1 - "data_".len() = 58.
        final String schemaName = getExistentTableSchemaName(criteria.getSchemaName(), criteria.getTableName());

        List<Field> fields = new ArrayList<>(criteria.getFields());
        fields.add(0, new IntegerField(SYS_PRIMARY_COLUMN));
        if (fields.stream().noneMatch(field -> SYS_HASH.equals(field.getName())))
            fields.add(1, new StringField(SYS_HASH));

        String sqlFields = getSelectFields(DEFAULT_TABLE_ALIAS, fields, true);

        final String sqlFormat = "SELECT %1$s \n  FROM %2$s as %3$s ";
        String sql = String.format(sqlFormat, sqlFields,
                escapeSchemaTableName(schemaName, criteria.getTableName()),
                DEFAULT_TABLE_ALIAS);

        QueryWithParams queryWithParams = new QueryWithParams(sql, null);
        queryWithParams.concat(getCriteriaWhereClause(criteria, DEFAULT_TABLE_ALIAS));

        Sorting sorting = !isNullOrEmpty(criteria.getSortings()) ? criteria.getSortings().get(0) : null;
        String orderBy = sortingToOrderBy(sorting, DEFAULT_TABLE_ALIAS);
        queryWithParams.concat(new QueryWithParams(orderBy, null));

        Query query = queryWithParams.createQuery(entityManager);
        if (criteria.getPage() >= DataCriteria.MIN_PAGE
                && criteria.getSize() >= DataCriteria.MIN_SIZE) {
            query.setFirstResult(getOffset(criteria)).setMaxResults(criteria.getSize());
        }

        List<Object[]> resultList = query.getResultList();
        return toRowValues(fields, resultList);
    }

    @Override
    public BigInteger getDataCount(DataCriteria criteria) {

        final String schemaName = getExistentTableSchemaName(criteria.getSchemaName(), criteria.getTableName());

        final String sqlFormat = "  FROM %s as %s\n";
        String sql = SELECT_COUNT_ONLY +
                String.format(sqlFormat,
                        escapeSchemaTableName(schemaName, criteria.getTableName()),
                        DEFAULT_TABLE_ALIAS);

        QueryWithParams queryWithParams = new QueryWithParams(sql, null);
        queryWithParams.concat(getCriteriaWhereClause(criteria, DEFAULT_TABLE_ALIAS));

        return (BigInteger) queryWithParams.createQuery(entityManager).getSingleResult();
    }

    /** Получение строки данных таблицы по системному идентификатору. */
    @Override
    public RowValue getRowData(String tableName, List<String> fieldNames, Object systemId) {

        List<Field> fields = dataTypesToFields(getColumnDataTypes(tableName), fieldNames);

        String sqlFields = getSelectFields(null, fields, true);
        String sql = String.format(SELECT_ROWS_FROM_DATA_BY_FIELD,
                sqlFields,
                DATA_SCHEMA_NAME,
                addDoubleQuotes(tableName),
                addDoubleQuotes(SYS_PRIMARY_COLUMN),
                QUERY_VALUE_SUBST);

        @SuppressWarnings("unchecked")
        List<Object[]> list = entityManager.createNativeQuery(sql)
                .setParameter(1, systemId)
                .getResultList();
        if (list.isEmpty())
            return null;

        RowValue row = toRowValues(fields, list).get(0);
        row.setSystemId(systemId); // ??
        return row;
    }

    /** Получение строк данных таблицы по системным идентификаторам. */
    @Override
    public List<RowValue> getRowData(String tableName, List<String> fieldNames, List<Object> systemIds) {

        List<Field> fields = dataTypesToFields(getColumnDataTypes(tableName), fieldNames);

        String sqlFields = getSelectFields(null, fields, true);
        String sql = String.format(SELECT_ROWS_FROM_DATA_BY_FIELD_LIST,
                sqlFields,
                DATA_SCHEMA_NAME,
                addDoubleQuotes(tableName),
                addDoubleQuotes(SYS_PRIMARY_COLUMN),
                QUERY_VALUE_SUBST);
        Query query = entityManager.createNativeQuery(sql);

        String ids = systemIds.stream().map(String::valueOf).collect(joining(","));
        query.setParameter(1, "{" + ids + "}");

        @SuppressWarnings("unchecked")
        List<Object[]> list = query.getResultList();
        return !list.isEmpty() ? toRowValues(fields, list) : emptyList();
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
    public List<String> getNotExists(String tableName, LocalDateTime bdate, LocalDateTime edate, List<String> hashList) {

        Map<String, Object> params = new HashMap<>();
        String sqlHashArray = "array[" + hashList.stream().map(hash -> {
            String hashPlaceHolder = "hash" + params.size();
            params.put(hashPlaceHolder, hash);
            return ":" + hashPlaceHolder;
        }).collect(joining(",")) + "]";

        QueryWithParams whereByDates = getWhereByDates(bdate, edate, DEFAULT_TABLE_ALIAS);
        String sqlByDate = (whereByDates == null || DataUtil.isNullOrEmpty(whereByDates.getSql())) ? "" : whereByDates.getSql();

        String sql = "SELECT hash \n" +
                "  FROM (\n" +
                "    SELECT unnest(" + sqlHashArray + ") hash \n" +
                "  ) hashes \n" +
                " WHERE hash NOT IN ( \n" +
                "   SELECT " + addDoubleQuotes(SYS_HASH) + " \n" +
                "     FROM " + escapeSchemaTableName(DATA_SCHEMA_NAME, tableName) +
                ALIAS_OPERATOR + DEFAULT_TABLE_ALIAS + " \n" +
                "    WHERE " + WHERE_DEFAULT +
                "   " + sqlByDate +
                " )";
        QueryWithParams queryWithParams = new QueryWithParams(sql, params);

        if (whereByDates != null) {
            queryWithParams.concat(whereByDates.params);
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

    private String sortingToOrderBy(Sorting sorting, String alias) {

        String orderBy = " ORDER BY ";
        if (sorting != null && sorting.getField() != null) {
            orderBy = orderBy + formatFieldForQuery(sorting.getField(), alias) + " " + sorting.getDirection().toString() + ", ";
        }

        return orderBy + " " + formatFieldForQuery(SYS_PRIMARY_COLUMN, alias);
    }

    private QueryWithParams getCriteriaWhereClause(DataCriteria criteria, String alias) {

        DataCriteria whereCriteria = new DataCriteria(criteria);
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

    private QueryWithParams getWhereClause(DataCriteria criteria, String alias) {

        QueryWithParams query = new QueryWithParams(SELECT_WHERE, null);
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

        search = search != null ? search.trim() : null;
        if (DataUtil.isNullOrEmpty(search))
            return null;

        String sql = "";
        Map<String, Object> params = new HashMap<>();

        // full text search
        String escapedFtsColumn = escapeTableFieldName(alias, SYS_FTS);
        if (DATE_PATTERN.matcher(search).matches()) {
            sql += " and (" +
                    escapedFtsColumn + " @@ to_tsquery(:search) or " +
                    escapedFtsColumn + " @@ to_tsquery(:reverseSearch) ) ";
            String[] dateArr = search.split("\\.");
            String reverseSearch = dateArr[2] + "-" + dateArr[1] + "-" + dateArr[0];
            params.put("search", search.trim());
            params.put("reverseSearch", reverseSearch);

        } else {
            String formattedSearch = search.toLowerCase()
                    .replaceAll(":", "\\\\:")
                    .replaceAll("/", "\\\\/")
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
        final int[] i = {-1};
        sql += fieldFilters.stream().map(list -> {
            if (isEmpty(list))
                return null;

            List<String> filters = new ArrayList<>();
            for (FieldSearchCriteria searchCriteria : list) {
                i[0]++;
                toWhereClauseByFilter(searchCriteria, i[0], alias, filters, params);
            }

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
        String escapedFieldName = escapeTableFieldName(alias, fieldName);

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

        String escapedColumn = escapeTableFieldName(alias, SYS_PRIMARY_COLUMN);
        String sql = " and (" + escapedColumn + " in (:systemIds))";
        params.put("systemIds", systemIds);

        return new QueryWithParams(sql, params);
    }

    @Override
    public BigInteger countData(String tableName) {

        String sql = String.format(SELECT_COUNT_QUERY_TEMPLATE, DATA_SCHEMA_NAME, addDoubleQuotes(tableName));
        return (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    @Transactional
    public void createDraftTable(String storageCode, List<Field> fields) {

        String schemaName = getExistentSchemaName(toSchemaName(storageCode));
        String tableName = toTableName(storageCode);
        
        if (storageExists(storageCode))
            throw new IllegalArgumentException("table.already.exists");

        String tableFields = "";
        if (!isNullOrEmpty(fields)) {
            tableFields = fields.stream()
                    .map(f -> addDoubleQuotes(f.getName()) + " " + f.getType())
                    .collect(joining(", \n")) + ", \n";
        }

        String ddl = String.format(CREATE_DRAFT_TABLE_TEMPLATE,
                schemaName, addDoubleQuotes(tableName), tableFields, tableName);

        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @Transactional
    public void copyTable(String newTableName, String sourceTableName) {

        String ddlCopyTable = String.format(COPY_TABLE_TEMPLATE,
                DATA_SCHEMA_NAME,
                addDoubleQuotes(newTableName),
                addDoubleQuotes(sourceTableName));
        entityManager.createNativeQuery(ddlCopyTable).executeUpdate();

        String ddlCreateSequence = String.format(CREATE_TABLE_SEQUENCE,
                DATA_SCHEMA_NAME,
                newTableName,
                SYS_PRIMARY_COLUMN);
        entityManager.createNativeQuery(ddlCreateSequence).executeUpdate();

        String sqlSelectIndexes = "SELECT indexdef FROM pg_indexes WHERE tablename=? AND NOT indexdef LIKE '%\"SYS_HASH\"%'";
        List<String> indexes = entityManager.createNativeQuery(sqlSelectIndexes)
                .setParameter(1, sourceTableName)
                .getResultList();
        for (String index : indexes) {
            entityManager.createNativeQuery(index.replaceAll(sourceTableName, newTableName)).executeUpdate();
        }

        createHashIndex(DATA_SCHEMA_NAME, newTableName);

        String ddlAddPrimaryKey = String.format("ALTER TABLE %1$s.%2$s ADD PRIMARY KEY (\"%3$s\");",
                DATA_SCHEMA_NAME,
                addDoubleQuotes(newTableName),
                SYS_PRIMARY_COLUMN);
        entityManager.createNativeQuery(ddlAddPrimaryKey).executeUpdate();

        String ddlAlterColumn = String.format("ALTER TABLE %1$s.%2$s ALTER COLUMN \"%3$s\" SET DEFAULT nextval('%1$s.\"%4$s_%3$s_seq\"');",
                DATA_SCHEMA_NAME,
                addDoubleQuotes(newTableName),
                SYS_PRIMARY_COLUMN,
                newTableName);
        entityManager.createNativeQuery(ddlAlterColumn).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void dropTable(String tableName) {

        String ddl = String.format(DROP_TABLE, DATA_SCHEMA_NAME, addDoubleQuotes(tableName));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public boolean schemaExists(String schemaName) {

        Boolean result = (Boolean) entityManager.createNativeQuery(SELECT_SCHEMA_EXISTS)
                .setParameter("schemaName", schemaName)
                .getSingleResult();

        return result != null && result;
    }

    private String getExistentSchemaName(String schemaName) {

        schemaName = getSchemaName(schemaName);
        if (DATA_SCHEMA_NAME.equals(schemaName) || schemaExists(schemaName))
            return schemaName;

        return DATA_SCHEMA_NAME;
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public boolean storageExists(String storageCode) {

        Boolean result = (Boolean) entityManager.createNativeQuery(SELECT_TABLE_EXISTS)
                .setParameter("schemaName", toSchemaName(storageCode))
                .setParameter("tableName", toTableName(storageCode))
                .getSingleResult();

        return result != null && result;
    }

    private String getExistentTableSchemaName(String schemaName, String tableName) {

        schemaName = getExistentSchemaName(schemaName);
        if (DATA_SCHEMA_NAME.equals(schemaName) || storageExists(schemaName + NAME_SEPARATOR + tableName))
            return schemaName;

        return DATA_SCHEMA_NAME;
    }

    @Override
    @Transactional
    public void addColumnToTable(String tableName, String name, String type, String defaultValue) {

        String ddl;
        if (defaultValue != null) {
            ddl = String.format(ADD_NEW_COLUMN_WITH_DEFAULT, DATA_SCHEMA_NAME, tableName, name, type, defaultValue);

        } else {
            ddl = String.format(ADD_NEW_COLUMN, DATA_SCHEMA_NAME, tableName, name, type);
        }

        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void deleteColumnFromTable(String tableName, String field) {

        String ddl = String.format(DELETE_COLUMN, DATA_SCHEMA_NAME, tableName, field);
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
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
        for (int firstIndex = 0, maxIndex = batchSize;
             firstIndex < values.size();
             firstIndex = maxIndex, maxIndex = firstIndex + batchSize) {

            int valueCount = Math.min(maxIndex, values.size());
            List<String> subValues = values.subList(firstIndex, valueCount);
            List<RowValue> subData = data.subList(firstIndex, valueCount);
            String stringValues = String.join("),(", subValues);

            String sql = String.format(INSERT_QUERY_TEMPLATE,
                    DATA_SCHEMA_NAME,
                    addDoubleQuotes(tableName),
                    String.join(",", keys),
                    stringValues);
            Query query = entityManager.createNativeQuery(sql);

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

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void loadData(String draftTable, String sourceTable, List<String> fields,
                         LocalDateTime fromDate, LocalDateTime toDate) {

        String keys = String.join(",", fields);
        String values = fields.stream()
                .map(f -> DEFAULT_TABLE_ALIAS + NAME_SEPARATOR + f)
                .collect(joining(","));

        String sql = String.format(COPY_QUERY_TEMPLATE,
                DATA_SCHEMA_NAME,
                addDoubleQuotes(draftTable),
                keys, values,
                addDoubleQuotes(sourceTable));

        QueryWithParams queryWithParams = new QueryWithParams(sql, null);
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

        QueryWithParams whereByDate = getWhereByDates(refValue.getDate(), null, REFERENCE_VALUATION_SELECT_TABLE);
        String sqlDateValue = formatDateTime(refValue.getDate());
        sqlDateValue = String.format(TO_TIMESTAMP, addSingleQuotes(sqlDateValue)) + TIMESTAMP_NO_TZ;
        String sqlByDate = (whereByDate == null || DataUtil.isNullOrEmpty(whereByDate.getSql()))
                ? ""
                : whereByDate.getSql()
                .replace(":bdate", sqlDateValue)
                .replace(":edate", "'-infinity'\\:\\:timestamp");

        String sql = String.format(REFERENCE_VALUATION_SELECT_EXPRESSION,
                DATA_SCHEMA_NAME,
                addDoubleQuotes(refValue.getStorageCode()),
                REFERENCE_VALUATION_SELECT_TABLE,
                addDoubleQuotes(refValue.getKeyField()),
                sqlExpression,
                valueSubst,
                getFieldType(refValue.getStorageCode(), refValue.getKeyField()),
                sqlByDate);

        return "(" + sql + ")";
    }

    @Override
    @Transactional
    public void updateData(String tableName, RowValue rowValue) {

        List<String> keyList = new ArrayList<>();
        for (Object objectValue : rowValue.getFieldValues()) {
            FieldValue<?> fieldValue = (FieldValue<?>) objectValue;

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
        String sql = String.format(UPDATE_QUERY_TEMPLATE, addDoubleQuotes(tableName), keys, QUERY_VALUE_SUBST);
        Query query = entityManager.createNativeQuery(sql);

        int i = 1;
        for (Object obj : rowValue.getFieldValues()) {
            FieldValue fieldValue = (FieldValue) obj;
            if (fieldValue.getValue() != null)
                if (fieldValue instanceof ReferenceFieldValue) {
                    if (((ReferenceFieldValue) fieldValue).getValue().getValue() != null)
                        query.setParameter(i++, ((ReferenceFieldValue) fieldValue).getValue().getValue());
                } else {
                    query.setParameter(i++, fieldValue.getValue());
                }
        }
        query.setParameter(i, rowValue.getSystemId());

        query.executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void deleteData(String tableName) {

        String sql = String.format(DELETE_ALL_RECORDS_FROM_TABLE_QUERY_TEMPLATE, addDoubleQuotes(tableName));
        Query query = entityManager.createNativeQuery(sql);
        query.executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void deleteData(String tableName, List<Object> systemIds) {

        String ids = systemIds.stream().map(id -> "?").collect(joining(","));
        String sql = String.format(DELETE_QUERY_TEMPLATE, addDoubleQuotes(tableName), ids);
        Query query = entityManager.createNativeQuery(sql);

        int i = 1;
        for (Object systemId : systemIds) {
            query.setParameter(i++, systemId);
        }
        query.executeUpdate();
    }

    @Override
    @Transactional
    public void updateReferenceInRows(String tableName, ReferenceFieldValue fieldValue, List<Object> systemIds) {

        if (getReferenceDisplayType(fieldValue.getValue()) == null)
            return;

        String quotedFieldName = addDoubleQuotes(fieldValue.getField());
        String oldFieldExpression = sqlFieldExpression(fieldValue.getField(), REFERENCE_VALUATION_UPDATE_TABLE);
        String oldFieldValue = String.format(REFERENCE_VALUATION_OLD_VALUE, oldFieldExpression);
        String key = quotedFieldName + " = " + getReferenceValuationSelect(fieldValue, oldFieldValue);

        String sql = String.format(UPDATE_REFERENCE_QUERY_TEMPLATE, addDoubleQuotes(tableName), key, QUERY_VALUE_SUBST);
        Query query = entityManager.createNativeQuery(sql);

        String ids = systemIds.stream().map(String::valueOf).collect(joining(","));
        query.setParameter(1, "{" + ids + "}");

        query.executeUpdate();
    }

    @Override
    public BigInteger countReferenceInRefRows(String tableName, ReferenceFieldValue fieldValue) {

        if (getReferenceDisplayType(fieldValue.getValue()) == null)
            return BigInteger.ZERO;

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("versionTable", escapeSchemaTableName(DATA_SCHEMA_NAME, tableName));
        placeholderValues.put("refFieldName", addDoubleQuotes(fieldValue.getField()));

        String sql = StrSubstitutor.replace(COUNT_REFERENCE_IN_REF_ROWS, placeholderValues);
        BigInteger count = (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();

        if (logger.isDebugEnabled()) {
            logger.debug("countReferenceInRefRows method count: {}, sql: {}", count, sql);
        }

        return count;
    }

    @Override
    @Transactional
    public void updateReferenceInRefRows(String tableName, ReferenceFieldValue fieldValue, int offset, int limit) {

        String quotedFieldName = addDoubleQuotes(fieldValue.getField());
        String oldFieldExpression = sqlFieldExpression(fieldValue.getField(), REFERENCE_VALUATION_UPDATE_TABLE);
        String oldFieldValue = String.format(REFERENCE_VALUATION_OLD_VALUE, oldFieldExpression);
        String key = quotedFieldName + " = " + getReferenceValuationSelect(fieldValue, oldFieldValue);

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("versionTable", escapeSchemaTableName(DATA_SCHEMA_NAME, tableName));
        placeholderValues.put("refFieldName", addDoubleQuotes(fieldValue.getField()));
        placeholderValues.put("limit", "" + limit);
        placeholderValues.put("offset", "" + offset);

        String where = StrSubstitutor.replace(WHERE_REFERENCE_IN_REF_ROWS, placeholderValues);
        String sql = String.format(UPDATE_QUERY_TEMPLATE, addDoubleQuotes(tableName), key, where);

        if (logger.isDebugEnabled()) {
            logger.debug("updateReferenceInRefRows method sql: {}", sql);
        }

        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    @Transactional
    public void deleteEmptyRows(String draftCode) {

        List<String> fieldNames = getFieldNames(draftCode);
        if (isEmpty(fieldNames)) {
            deleteData(draftCode);

        } else {
            String allFieldsNullWhere = fieldNames.stream()
                    .map(s -> s + " IS NULL")
                    .collect(joining(" AND "));

            String sql = String.format(DELETE_EMPTY_RECORDS_FROM_TABLE_QUERY_TEMPLATE,
                    addDoubleQuotes(draftCode),
                    allFieldsNullWhere);
            entityManager.createNativeQuery(sql).executeUpdate();
        }
    }

    @Override
    public boolean isUnique(String storageCode, List<String> fieldNames, LocalDateTime publishTime) {

        String fields = fieldNames.stream()
                .map(fieldName -> addDoubleQuotes(fieldName) + "\\:\\:text")
                .collect(joining(","));
        String groupBy = Stream.iterate(1, n -> n + 1).limit(fieldNames.size())
                .map(String::valueOf)
                .collect(joining(","));

        QueryWithParams whereByDate = getWhereByDates(publishTime, null, DEFAULT_TABLE_ALIAS);
        String sqlByDate = (whereByDate == null || DataUtil.isNullOrEmpty(whereByDate.getSql())) ? "" : whereByDate.getSql();

        String sql = "SELECT " + fields + ", COUNT(*)" + "\n" +
                "  FROM " + escapeSchemaTableName(DATA_SCHEMA_NAME, storageCode) +
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
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createTriggers(String schemaName, String tableName) {
        createTriggers(schemaName, tableName, getHashUsedFieldNames(tableName));
    }

    @Override
    @Transactional
    public void createTriggers(String schemaName, String tableName, List<String> fieldNames) {

        final String alias = TRIGGER_NEW_ALIAS + NAME_SEPARATOR;
        String tableFields = fieldNames.stream().map(this::getFieldClearName).collect(joining(", "));

        String hashExpression = String.format(HASH_EXPRESSION,
                fieldNames.stream().map(field -> alias + field).collect(joining(", ")));
        String hashTriggerBody = String.format(ASSIGN_FIELD, alias + addDoubleQuotes(SYS_HASH), hashExpression);
        String createHashTrigger = String.format(CREATE_TRIGGER,
                schemaName,
                tableName,
                HASH_FUNCTION_NAME,
                HASH_TRIGGER_NAME,
                tableFields,
                hashTriggerBody + ";");
        entityManager.createNativeQuery(createHashTrigger).executeUpdate();

        String ftsExpression = fieldNames.stream()
                .map(field -> "coalesce( to_tsvector('ru', " + alias + field + "\\:\\:text),'')")
                .collect(joining(" || ' ' || "));
        String ftsTriggerBody = String.format(ASSIGN_FIELD, alias + addDoubleQuotes(SYS_FTS), ftsExpression);
        String createFtsTrigger = String.format(CREATE_TRIGGER,
                schemaName,
                tableName,
                FTS_FUNCTION_NAME,
                FTS_TRIGGER_NAME,
                tableFields,
                ftsTriggerBody + ";");
        entityManager.createNativeQuery(createFtsTrigger).executeUpdate();
    }

    /** Получение наименования поля с кавычками из наименования, сформированного по getHashUsedFieldNames. */
    private String getFieldClearName(String fieldName) {

        int closeQuoteIndex = fieldName.indexOf('"', 1);
        return fieldName.substring(0, closeQuoteIndex + 1);
    }

    @Override
    @Transactional
    public void updateHashRows(String tableName) {

        List<String> fieldNames = getHashUsedFieldNames(tableName);
        String expression = String.format(HASH_EXPRESSION,
                String.join(", ", fieldNames));
        String ddlAssign = String.format(ASSIGN_FIELD, addDoubleQuotes(SYS_HASH), expression);

        String ddl = String.format(UPDATE_FIELD, DATA_SCHEMA_NAME, addDoubleQuotes(tableName), ddlAssign);
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @Transactional
    public void updateFtsRows(String tableName) {

        List<String> fieldNames = getHashUsedFieldNames(tableName);
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
    public void createIndex(String schemaName, String tableName, String name, List<String> fields) {

        String ddl = String.format(CREATE_TABLE_INDEX,
                name,
                schemaName,
                addDoubleQuotes(tableName),
                fields.stream().map(DataUtil::addDoubleQuotes).collect(joining(",")));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createFullTextSearchIndex(String schemaName, String tableName) {

        String ddl = String.format(CREATE_FTS_INDEX,
                addDoubleQuotes(tableName + "_fts_idx"),
                schemaName,
                addDoubleQuotes(tableName),
                addDoubleQuotes(SYS_FTS));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createLtreeIndex(String schemaName, String tableName, String field) {

        String ddl = String.format(CREATE_LTREE_INDEX,
                addDoubleQuotes(tableName + "_" + field.toLowerCase() + "_idx"),
                schemaName,
                addDoubleQuotes(tableName),
                addDoubleQuotes(field));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void createHashIndex(String schemaName, String tableName) {

        String ddl = String.format(CREATE_TABLE_INDEX,
                addDoubleQuotes(tableName + "_sys_hash_ix"),
                schemaName,
                addDoubleQuotes(tableName),
                addDoubleQuotes(SYS_HASH));
        entityManager.createNativeQuery(ddl).executeUpdate();
    }

    @Override
    public List<String> getFieldNames(String tableName, String sqlFieldNames) {

        String sql = String.format(sqlFieldNames, tableName);
        List<String> results = entityManager.createNativeQuery(sql).getResultList();
        Collections.sort(results);

        return results;
    }

    @Override
    public List<String> getFieldNames(String tableName) {
        return getFieldNames(tableName, SELECT_FIELD_NAMES);
    }

    @Override
    public List<String> getHashUsedFieldNames(String tableName) {
        return getFieldNames(tableName, SELECT_HASH_USED_FIELD_NAMES);
    }

    @Override
    public String getFieldType(String tableName, String field) {

        String sql = String.format(SELECT_FIELD_TYPE, tableName, field);
        return entityManager.createNativeQuery(sql).getSingleResult().toString();
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
    public BigInteger countActualDataFromVersion(String versionTable, String draftTable,
                                                 LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("draftTable", escapeSchemaTableName(DATA_SCHEMA_NAME, draftTable));
        placeholderValues.put("versionTable", escapeSchemaTableName(DATA_SCHEMA_NAME, versionTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));

        String sql = StrSubstitutor.replace(COUNT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);
        return (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertActualDataFromVersion(String tableToInsert, String versionTable,
                                            String draftTable, Map<String, String> columns,
                                            int offset, int transactionSize,
                                            LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String columnsStr = columns.keySet().stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        String columnsWithType = columns.keySet().stream().map(s -> s + " " + columns.get(s)).reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        String columnsWithPrefixValue = columns.keySet().stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        String columnsWithPrefixD = columns.keySet().stream().map(s -> "d." + s + "").reduce((s1, s2) -> s1 + ", " + s2).orElse("");

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("dColumns", columnsWithPrefixD);
        placeholderValues.put("vValues", columnsWithPrefixValue);
        placeholderValues.put("draftTable", escapeSchemaTableName(DATA_SCHEMA_NAME, draftTable));
        placeholderValues.put("versionTable", escapeSchemaTableName(DATA_SCHEMA_NAME, versionTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));
        placeholderValues.put("offset", "" + offset);
        placeholderValues.put("transactionSize", "" + transactionSize);
        placeholderValues.put("newTableSeqName", escapeSchemaSequenceName(DATA_SCHEMA_NAME, tableToInsert));
        placeholderValues.put("tableToInsert", escapeSchemaTableName(DATA_SCHEMA_NAME, tableToInsert));
        placeholderValues.put("columns", columnsStr);
        placeholderValues.put("columnsWithType", columnsWithType);

        String sql = StrSubstitutor.replace(INSERT_ACTUAL_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);

        if (logger.isDebugEnabled()) {
            logger.debug("insertActualDataFromVersion with closeTime method sql: {}", sql);
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
    public void insertOldDataFromVersion(String tableToInsert, String versionTable,
                                         String draftTable, List<String> columns,
                                         int offset, int transactionSize,
                                         LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).orElse(null);
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).orElse(null);

        String sql = String.format(INSERT_OLD_VAL_FROM_VERSION_WITH_CLOSE_DATE,
                addDoubleQuotes(tableToInsert),
                addDoubleQuotes(versionTable),
                addDoubleQuotes(draftTable),
                offset,
                transactionSize,
                escapeSequenceName(tableToInsert),
                columnsStr,
                columnsWithPrefix,
                formatDateTime(publishTime),
                formatDateTime(closeTime));

        if (logger.isDebugEnabled()) {
            logger.debug("insertOldDataFromVersion with closeTime method sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    public BigInteger countClosedNowDataFromVersion(String versionTable, String draftTable,
                                                    LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("versionTable", escapeSchemaTableName(DATA_SCHEMA_NAME, versionTable));
        placeholderValues.put("draftTable", escapeSchemaTableName(DATA_SCHEMA_NAME, draftTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));

        String sql = StrSubstitutor.replace(COUNT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);
        return (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertClosedNowDataFromVersion(String tableToInsert, String versionTable,
                                               String draftTable, Map<String, String> columns,
                                               int offset, int transactionSize,
                                               LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        String columnsStr = columns.keySet().stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        String columnsWithType = columns.keySet().stream().map(s -> s + " " + columns.get(s)).reduce((s1, s2) -> s1 + ", " + s2).orElse("");

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("tableToInsert", escapeSchemaTableName(DATA_SCHEMA_NAME, tableToInsert));
        placeholderValues.put("draftTable", escapeSchemaTableName(DATA_SCHEMA_NAME, draftTable));
        placeholderValues.put("versionTable", escapeSchemaTableName(DATA_SCHEMA_NAME, versionTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));
        placeholderValues.put("columns", columnsStr);
        placeholderValues.put("offset", "" + offset);
        placeholderValues.put("transactionSize", "" + transactionSize);
        placeholderValues.put("columnsWithType", columnsWithType);
        placeholderValues.put("sequenceName", escapeSchemaSequenceName(DATA_SCHEMA_NAME, tableToInsert));

        String sql = StrSubstitutor.replace(INSERT_CLOSED_NOW_VAL_FROM_VERSION_WITH_CLOSE_TIME, placeholderValues);

        if (logger.isDebugEnabled()) {
            logger.debug("insertClosedNowDataFromVersion with closeTime method sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    public BigInteger countNewValFromDraft(String draftTable, String versionTable,
                                           LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("draftTable", escapeSchemaTableName(DATA_SCHEMA_NAME, draftTable));
        placeholderValues.put("versionTable", escapeSchemaTableName(DATA_SCHEMA_NAME, versionTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));

        String sql = StrSubstitutor.replace(COUNT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME, placeholderValues);
        return (BigInteger) entityManager.createNativeQuery(sql).getSingleResult();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertNewDataFromDraft(String tableToInsert, String versionTable, String draftTable,
                                       List<String> columns, int offset, int transactionSize,
                                       LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;
        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).get();

        Map<String, String> placeholderValues = new HashMap<>();
        placeholderValues.put("fields", columnsStr);
        placeholderValues.put("draftTable", escapeSchemaTableName(DATA_SCHEMA_NAME, draftTable));
        placeholderValues.put("versionTable", escapeSchemaTableName(DATA_SCHEMA_NAME, versionTable));
        placeholderValues.put("publishTime", formatDateTime(publishTime));
        placeholderValues.put("closeTime", formatDateTime(closeTime));
        placeholderValues.put("transactionSize", "" + transactionSize);
        placeholderValues.put("offset", "" + offset);
        placeholderValues.put("sequenceName", escapeSchemaSequenceName(DATA_SCHEMA_NAME, tableToInsert));
        placeholderValues.put("tableToInsert", escapeSchemaTableName(DATA_SCHEMA_NAME, tableToInsert));
        placeholderValues.put("rowFields", columnsWithPrefix);

        String sql = StrSubstitutor.replace(INSERT_NEW_VAL_FROM_DRAFT_WITH_CLOSE_TIME, placeholderValues);

        if (logger.isDebugEnabled()) {
            logger.debug("insertNewDataFromDraft with closeTime method sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void insertDataFromDraft(String draftTable, String tableToInsert, List<String> columns,
                                    int offset, int transactionSize,
                                    LocalDateTime publishTime, LocalDateTime closeTime) {
        closeTime = closeTime == null ? PG_MAX_TIMESTAMP : closeTime;

        String columnsStr = columns.stream().map(s -> "" + s + "").reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        String columnsWithPrefix = columns.stream().map(s -> "row." + s + "").reduce((s1, s2) -> s1 + ", " + s2).orElse("");

        String sql = String.format(INSERT_FROM_DRAFT_TEMPLATE_WITH_CLOSE_TIME,
                addDoubleQuotes(draftTable),
                offset,
                transactionSize,
                addDoubleQuotes(tableToInsert),
                formatDateTime(publishTime),
                formatDateTime(closeTime),
                escapeSequenceName(tableToInsert),
                columnsStr,
                columnsWithPrefix);

        if (logger.isDebugEnabled()) {
            logger.debug("insertDataFromDraft with closeTime method sql: {}", sql);
        }
        entityManager.createNativeQuery(sql).executeUpdate();
    }

    @Override
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void deletePointRows(String targetTable) {

        String sql = String.format(DELETE_POINT_ROWS_QUERY_TEMPLATE,
                addDoubleQuotes(targetTable));

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

        String oldStorage = criteria.getStorageCode();
        String newStorage = criteria.getNewStorageCode() != null ? criteria.getNewStorageCode() : criteria.getStorageCode();

        String oldDataFields = getSelectFields("t1", criteria.getFields(), false);
        String newDataFields = getSelectFields("t2", criteria.getFields(), false);

        String dataSelectFormat = "SELECT t1.%1$s as sysId1 \n %2$s \n, t2.%1$s as sysId2 \n %3$s \n";
        String dataSelect = String.format(dataSelectFormat,
                addDoubleQuotes(SYS_PRIMARY_COLUMN),
                DataUtil.isNullOrEmpty(oldDataFields) ? "" : ", " + oldDataFields,
                DataUtil.isNullOrEmpty(newDataFields) ? "" : ", " + newDataFields);

        String primaryEquality = criteria.getPrimaryFields().stream()
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
            params.put("oldPublishDate", truncateDateTo(criteria.getOldPublishDate(), ChronoUnit.SECONDS, MIN_TIMESTAMP_VALUE));
            params.put("oldCloseDate", truncateDateTo(criteria.getOldCloseDate(), ChronoUnit.SECONDS, PG_MAX_TIMESTAMP));
        }

        String newVersionDateFilter = "";
        if (criteria.getNewPublishDate() != null || criteria.getNewCloseDate() != null) {
            newVersionDateFilter = " and date_trunc('second', t2.\"SYS_PUBLISHTIME\") <= :newPublishDate\\:\\:timestamp without time zone \n" +
                    " and date_trunc('second', t2.\"SYS_CLOSETIME\") >= :newCloseDate\\:\\:timestamp without time zone ";
            params.put("newPublishDate", truncateDateTo(criteria.getNewPublishDate(), ChronoUnit.SECONDS, MIN_TIMESTAMP_VALUE));
            params.put("newCloseDate", truncateDateTo(criteria.getNewCloseDate(), ChronoUnit.SECONDS, PG_MAX_TIMESTAMP));
        }

        String joinType = diffReturnTypeToJoinType(criteria.getReturnType());

        String sql = " from " + escapeSchemaTableName(DATA_SCHEMA_NAME, oldStorage) + " t1 " + joinType +
                " join " + escapeSchemaTableName(DATA_SCHEMA_NAME, newStorage) + " t2 on " + primaryEquality +
                " and (true" + oldPrimaryValuesFilter + " or true" + newPrimaryValuesFilter + ")" +
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

        String orderBy = " order by " +
                criteria.getPrimaryFields().stream()
                        .map(f -> formatFieldForQuery(f, "t2"))
                        .collect(joining(",")) + "," +
                criteria.getPrimaryFields().stream()
                        .map(f -> formatFieldForQuery(f, "t1"))
                        .collect(joining(","));

        QueryWithParams dataQueryWithParams = new QueryWithParams(dataSelect + sql + orderBy, params);
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

    private String getFieldValuesFilter(String alias, Map<String, Object> params,
                                        Set<List<FieldSearchCriteria>> fieldFilters) {

        QueryWithParams query = getWhereByFilters(fieldFilters, alias);
        if (query == null || DataUtil.isNullOrEmpty(query.getSql()))
            return "";

        if (!isNullOrEmpty(query.getParams())) {
            params.putAll(query.getParams());
        }

        return query.getSql();
    }

    private static class QueryWithParams {

        private String sql;

        private Map<String, Object> params;

        public QueryWithParams() {
            this("", new HashMap<>());
        }

        public QueryWithParams(String sql, Map<String, Object> params) {
            this.sql = sql;
            this.params = params;
        }

        public void concat(String sql) {

            if (DataUtil.isNullOrEmpty(sql))
                return;

            this.sql = this.sql + " " + sql;
        }

        public void concat(Map<String, Object> params) {

            if (isNullOrEmpty(params))
                return;

            if (this.params == null) {
                this.params = new HashMap<>(params);

            } else {
                this.params.putAll(params);
            }
        }

        public void concat(String sql, Map<String, Object> params) {

            concat(sql);
            concat(params);
        }

        public void concat(QueryWithParams queryWithParams) {

            if (queryWithParams == null)
                return;

            concat(queryWithParams.getSql(), queryWithParams.getParams());
        }

        public Query createQuery(EntityManager entityManager) {

            Query query = entityManager.createNativeQuery(getSql());
            fillQueryParameters(query);

            return query;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public void setParams(Map<String, Object> params) {
            this.params = params;
        }

        public void fillQueryParameters(Query query) {
            if (getParams() == null)
                return;

            for (Map.Entry<String, Object> entry : getParams().entrySet()) {
                query = query.setParameter(entry.getKey(), entry.getValue());
            }
        }
    }
}
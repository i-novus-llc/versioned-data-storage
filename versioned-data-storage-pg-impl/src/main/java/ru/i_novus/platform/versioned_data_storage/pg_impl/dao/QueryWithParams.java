package ru.i_novus.platform.versioned_data_storage.pg_impl.dao;

import ru.i_novus.platform.datastorage.temporal.util.StringUtils;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.HashMap;
import java.util.Map;

import static ru.i_novus.platform.datastorage.temporal.util.CollectionUtils.isNullOrEmpty;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.addSingleQuotes;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.QUERY_NULL_VALUE;

/** Запрос с параметрами.
 * <p>
 * Позволяет одновременно формировать запрос и список параметров для него.
 */
public class QueryWithParams {

    private String sql;

    private Map<String, Object> params;

    public QueryWithParams() {
        this("", null);
    }

    public QueryWithParams(String sql) {
        this(sql, null);
    }

    public QueryWithParams(String sql, Map<String, Object> params) {

        this.sql = sql;
        this.params = params;
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

    public void concat(String sql) {

        if (StringUtils.isNullOrEmpty(sql))
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

    public String getBindedSql() {

        if (StringUtils.isNullOrEmpty(sql))
            return null;

        if (isNullOrEmpty(params))
            return sql;

        String result = sql;
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            result = result.replaceAll(":" + entry.getKey(), paramToString(entry.getValue()));
        }

        return result;
    }

    private String paramToString(Object param) {

        if (param == null)
            return QUERY_NULL_VALUE;

        if (param instanceof Number)
            return param.toString();

        return addSingleQuotes(param.toString());
    }

    public Query createQuery(EntityManager entityManager) {

        Query query = entityManager.createNativeQuery(getSql());
        fillQueryParameters(query);

        return query;
    }

    public void fillQueryParameters(Query query) {

        if (getParams() == null)
            return;

        for (Map.Entry<String, Object> entry : getParams().entrySet()) {
            query = query.setParameter(entry.getKey(), entry.getValue());
        }
    }
}

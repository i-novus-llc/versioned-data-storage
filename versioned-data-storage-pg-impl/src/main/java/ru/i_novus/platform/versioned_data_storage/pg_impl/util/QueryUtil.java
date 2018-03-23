package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lgalimova
 * @since 21.03.2018
 */
public class QueryUtil {

    public static List<Map<String, Object>> convertToMap(List<String> fields, List<Object[]> resultList, boolean isGetReferenceFullData) {
        List<Map<String, Object>> resultData = new ArrayList<>(resultList.size());
        for (Object objects : resultList) {
            Map<String, Object> returnedData = new LinkedHashMap<>();
            if (objects instanceof Object[]) {
                Object[] row = (Object[]) objects;
                for (int i = 0; i < row.length; i++) {
                    if(fields.get(i).contains("->>")){
                        returnedData.put(isGetReferenceFullData ? formatJsonbAttrValueForMapping(fields.get(i)) : fields.get(i).split("->>")[0], row[i]);
                    } else {
                        returnedData.put(fields.get(i), row[i]);
                    }
                }
            } else {
                returnedData.put(fields.get(0), objects);
            }
            resultData.add(returnedData);
        }
        return resultData;
    }

    public static String generateSqlQuery(String alias, List<String> fields) {
        return fields.stream().map(field -> {
            String query = formatFieldForQuery(field, alias);
            if (field.contains("->>"))
                return query + " as " + addEscapeCharacters(alias + formatJsonbAttrValueForMapping(field));
            else
                return query + " as " + addEscapeCharacters(alias + field);
        }).collect(Collectors.joining(", "));
    }

    public static String formatFieldForQuery(String field, String alias) {
        alias = alias + ".";
        if (field.contains("->>")) {
            String[] queryParts = field.split("->>");
            return alias + addEscapeCharacters(queryParts[0]) + "->>" + queryParts[1];
        } else {
            return alias + addEscapeCharacters(field);
        }
    }

    public static String addEscapeCharacters(String source) {
        return "\"" + source + "\"";
    }

    public static String formatJsonbAttrValueForMapping(String field) {
        if (!field.contains("->>"))
            return field;
        else {
            String[] parts = field.split("->>");
            return parts[0] +  "." + StringUtils.strip(parts[1], "'").toUpperCase();
        }
    }
}

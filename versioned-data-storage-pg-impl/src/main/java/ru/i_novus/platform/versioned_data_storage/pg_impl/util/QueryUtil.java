package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import org.apache.commons.lang.StringUtils;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.RowValue;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lgalimova
 * @since 21.03.2018
 */
public class QueryUtil {

    public static List<RowValue> convertToRowValue(List<Field> fields, List<Object[]> data) {
        List<RowValue> resultData = new ArrayList<>(data.size());
        for (Object objects : data) {
            RowValue rowValue = new LongRowValue();
            if (objects instanceof Object[]) {
                Object[] row = (Object[]) objects;
                for (int i = 0; i < row.length; i++) {
                    Field field = fields.get(i);
                    FieldValue fieldValue;
                    if (field instanceof BooleanField) {
                        fieldValue = new FieldValue<>(field, (Boolean) row[i]);
                    } else if (field instanceof DateField) {
                        fieldValue = new FieldValue<>(field, (Date) row[i]);
                    } else if (field instanceof FloatField) {
                        fieldValue = new FieldValue<>(field, (Float) row[i]);
                    } else if (field instanceof IntegerField) {
                        fieldValue = new FieldValue<>(field, (Float) row[i]);
                    } else if (field instanceof ReferenceField && field.getName().contains("->>")) {
                        ReferenceField referenceField = (ReferenceField) field;
                        ReferenceField newReferenceField = new ReferenceField(formatJsonbAttrValueForMapping(referenceField.getName()));
                        fieldValue = new FieldValue<>(newReferenceField, (String) row[i]);
                    } else {
                        fieldValue = new FieldValue<>(field, (String) row[i]);
                    }
                    rowValue.getFieldValues().add(fieldValue);
                }
            }
            //todo ?
//            else {
//                returnedData.put(fields.get(0).getName(), objects);
//            }
            resultData.add(rowValue);
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
            return parts[0] + "." + StringUtils.strip(parts[1], "'").toUpperCase();
        }
    }

    public static String getSequenceName(String table) {
        return addEscapeCharacters(table + "_SYS_RECORDID_seq");
    }
}

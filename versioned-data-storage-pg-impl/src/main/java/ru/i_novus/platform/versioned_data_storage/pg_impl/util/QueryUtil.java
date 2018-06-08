package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import net.n2oapp.criteria.api.Criteria;
import org.apache.commons.lang.StringUtils;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.LongRowValue;
import ru.i_novus.platform.datastorage.temporal.model.Reference;
import ru.i_novus.platform.datastorage.temporal.model.value.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;

import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * @author lgalimova
 * @since 21.03.2018
 */
public class QueryUtil {

    public static List<RowValue> convertToRowValue(List<Field> fields, List<Object[]> data) {
        List<RowValue> resultData = new ArrayList<>(data.size());
        for (Object objects : data) {
            LongRowValue rowValue = new LongRowValue();
            Iterator<Field> fieldIterator = fields.iterator();
            if (objects instanceof Object[]) {
                Object[] row = (Object[]) objects;
                for (int i = 0; i < row.length; i++) {
                    Field field = fieldIterator.next();
                    Object value = row[i];
                    if (i == 0) {
                        //SYS_RECORD_ID
                        rowValue.setSystemId(Long.parseLong(row[i].toString()));
                        continue;
                    }
                    if (field instanceof ReferenceField) {
                        value = new Reference(row[i], row[i + 1]);
                        i++;
                    }
                    rowValue.getFieldValues().add(getFieldValue(field, value));
                }
            } else {
                rowValue.getFieldValues().add(getFieldValue(fields.get(0), objects));
            }
            resultData.add(rowValue);
        }
        return resultData;
    }

    public static FieldValue getFieldValue(Field field, Object value) {
        FieldValue fieldValue = null;
        String name = field.getName();
        if (field instanceof BooleanField) {
            fieldValue = new BooleanFieldValue(name, (Boolean) value);
        } else if (field instanceof DateField) {
            fieldValue = new DateFieldValue(name, (Date) value);
        } else if (field instanceof FloatField) {
            fieldValue = new FloatFieldValue(name, (Number) value);
        } else if (field instanceof IntegerField) {
            fieldValue = new IntegerFieldValue(name, (Number) value);
        } else if (field instanceof ReferenceField) {
            fieldValue = new ReferenceFieldValue(name, (Reference) value);
        } else {
            fieldValue = new StringFieldValue(name, value != null ? value.toString() : null);
        }
        return fieldValue;
    }

    public static String generateSqlQuery(String alias, List<Field> fields) {
        if (StringUtils.isEmpty(alias))
            alias = "";
        List<String> queryFields = new ArrayList<>();
        for (Field field : fields) {
            String query = formatFieldForQuery(field.getName(), alias);
            if (field instanceof ReferenceField) {
                String queryValue = query + "->>'value' as " + addDoubleQuotes(alias + field.getName() + ".value");
                String queryDisplayValue = query + "->>'displayValue' as " + addDoubleQuotes(alias + field.getName() + ".displayValue");
                queryFields.add(queryValue);
                queryFields.add(queryDisplayValue);
            } else {
                if (field instanceof TreeField) {
                    query += "\\:\\:text";
                }
                query += " as " + addDoubleQuotes(alias + field.getName() + fields.indexOf(field));
                queryFields.add(query);
            }
        }
        return String.join(",", queryFields);
    }

    public static String formatFieldForQuery(String field, String alias) {
        if (!StringUtils.isEmpty(alias))
            alias = alias + ".";
        if (field.contains("->>")) {
            String[] queryParts = field.split("->>");
            return alias + addDoubleQuotes(queryParts[0]) + "->>" + queryParts[1];
        } else {
            return alias + addDoubleQuotes(field);
        }
    }

    public static String addDoubleQuotes(String source) {
        return "\"" + source + "\"";
    }

    public static String addSingleQuotes(String source) {
        return "'" + source + "'";
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
        return addDoubleQuotes(table + "_SYS_RECORDID_seq");
    }

    public static Date truncateDateTo(Date date, ChronoUnit unit) {
        return Date.from(date.toInstant().truncatedTo(unit));
    }

    public static int getOffset(Criteria criteria) {
        if (criteria != null) {
            if (criteria.getPage() <= 0 || criteria.getSize() <= 0)
                throw new IllegalStateException("Criteria page and size should be greater than zero");
            return (criteria.getPage() - 1) * criteria.getSize();
        }
        return 0;
    }

    public static boolean isCompatibleTypes(String oldDataType, String newDataType) {
//        if (ReferenceField.TYPE.equals(newDataType) || ListField.TYPE.equals(newDataType) || ReferenceField.TYPE.equals(oldDataType) || ListField.TYPE.equals(oldDataType)) {
//            return false;
//        }
        if (oldDataType.equals(newDataType) || StringField.TYPE.equals(newDataType)) {
            return true;
        }
        if ((StringField.TYPE.equals(oldDataType) || IntegerStringField.TYPE.equals(oldDataType))
                && (IntegerField.TYPE.equals(newDataType)) || IntegerStringField.TYPE.equals(newDataType)) {
            return true;
        }
        return false;
    }

    public static Field getField(String name, String type) {
        switch (type) {
            case BooleanField.TYPE:
                return new BooleanField(name);
            case DateField.TYPE:
                return new DateField(name);
            case FloatField.TYPE:
                return new FloatField(name);
            case IntegerField.TYPE:
                return new IntegerField(name);
            case ReferenceField.TYPE:
                return new ReferenceField(name);
            default:
                return new StringField(name);
        }

    }
}

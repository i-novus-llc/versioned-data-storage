package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import net.n2oapp.criteria.api.Criteria;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import ru.i_novus.platform.datastorage.temporal.enums.ReferenceDisplayType;
import ru.i_novus.platform.datastorage.temporal.model.*;
import ru.i_novus.platform.datastorage.temporal.model.value.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static ru.i_novus.platform.datastorage.temporal.model.DataConstants.*;

/**
 * @author lgalimova
 * @since 21.03.2018
 */
public class QueryUtil {

    private QueryUtil() {
    }

    /**
     * Преобразование полученных данных в список записей.
     *
     * При получении всех данных необходимо использовать
     * совместно с {@link #generateSqlQuery} при {@code includeReference} = true.
     *
     * @param fields список полей
     * @param data   список данных
     * @return Список записей
     */
    public static List<RowValue> convertToRowValue(List<Field> fields, List<Object[]> data) {
        List<RowValue> resultData = new ArrayList<>(data.size());
        for (Object objects : data) {
            LongRowValue rowValue = new LongRowValue();
            Iterator<Field> fieldIterator = fields.iterator();
            if (objects instanceof Object[]) {
                Object[] row = (Object[]) objects;

                int i = 0;
                while (i < row.length) {
                    Field field = fieldIterator.next();
                    Object value = row[i];

                    if (i == 0) { // SYS_RECORD_ID
                        rowValue.setSystemId(Long.parseLong(row[i].toString()));

                    } else if (i == 1) { // SYS_HASH
                        rowValue.setHash(row[i].toString());

                    } else { // FIELD
                        if (field instanceof ReferenceField
                                && (i + 1 < row.length)) {
                            value = new Reference(row[i] != null ? row[i].toString() : null,
                                    row[i + 1] != null ? row[i + 1].toString() : null);
                            i++;
                        }

                        rowValue.getFieldValues().add(getFieldValue(field, value));
                    }

                    i++;
                }
            } else {
                rowValue.getFieldValues().add(getFieldValue(fields.get(0), objects));
            }
            resultData.add(rowValue);
        }
        return resultData;
    }

    /**
     * Получение значения поля в виде объекта соответствующего класса
     *
     * @param field поле
     * @param value значение
     * @return Значение поля
     */
    private static FieldValue getFieldValue(Field field, Object value) {

        String name = field.getName();

        if (field instanceof BooleanField) {
            return new BooleanFieldValue(name, (Boolean) value);

        } else if (field instanceof DateField) {
            return new DateFieldValue(name,
                    value != null ? ((java.sql.Date) value).toLocalDate() : null);

        } else if (field instanceof FloatField) {
            return new FloatFieldValue(name, (Number) value);

        } else if (field instanceof IntegerField) {
            return new IntegerFieldValue(name,
                    value != null ? new BigInteger(value.toString()) : null);

        } else if (field instanceof ReferenceField) {
            return new ReferenceFieldValue(name, (Reference) value);

        }

        return new StringFieldValue(name, value != null ? value.toString() : null);
    }

    /**
     * Проверка значения поля на отсутствие.
     *
     * @param fieldValue значение поля
     * @return Отсутствие значения
     */
    public static boolean isFieldValueNull(FieldValue fieldValue) {
        return fieldValue.getValue() == null || fieldValue.getValue().equals("null");
    }

    public static String generateSqlQuery(String alias, List<Field> fields, boolean includeReference) {
        if (StringUtils.isEmpty(alias))
            alias = "";

        List<String> queryFields = new ArrayList<>();
        for (Field field : fields) {
            String query = formatFieldForQuery(field.getName(), alias);

            if (field instanceof ReferenceField) {
                String queryValue = query + "->>'value' as " + addDoubleQuotes(alias + field.getName() + ".value");
                queryFields.add(queryValue);
                if (includeReference) {
                    String queryDisplayValue = query + "->>'displayValue' as " + addDoubleQuotes(alias + field.getName() + ".displayValue");
                    queryFields.add(queryDisplayValue);
                }
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

    public static String getTableName(String table) {
        return addDoubleQuotes(table);
    }

    public static String getSequenceName(String table) {
        return addDoubleQuotes(table + "_SYS_RECORDID_seq");
    }

    public static String getSchemeTableName(String table) {
        return DATA_SCHEME_NAME + "." + getTableName(table);
    }

    public static String getSchemeSequenceName(String table) {
        return DATA_SCHEME_NAME + "." + getSequenceName(table);
    }

    public static LocalDateTime truncateDateTo(LocalDateTime date, ChronoUnit unit) {
        return date.truncatedTo(unit);
    }

    public static int getOffset(Criteria criteria) {
        if (criteria != null) {
            if (criteria.getPage() <= 0 || criteria.getSize() <= 0)
                throw new IllegalStateException("Criteria page and size should be greater than zero");
            return (criteria.getPage() - 1) * criteria.getSize();
        }
        return 0;
    }

    @SuppressWarnings("all")
    public static boolean isVarcharType(String type) {
        return StringField.TYPE.equals(type) || IntegerStringField.TYPE.equals(type);
    }

    public static boolean isCompatibleTypes(String oldDataType, String newDataType) {
//        if (ReferenceField.TYPE.equals(newDataType) || ListField.TYPE.equals(newDataType) || ReferenceField.TYPE.equals(oldDataType) || ListField.TYPE.equals(oldDataType)) {
//            return false;
//        }
        if (oldDataType.equals(newDataType) || StringField.TYPE.equals(newDataType)) {
            return true;
        }
        if ((isVarcharType(oldDataType))
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

    /**
     * Получение типа отображения ссылки.
     *
     * @param reference ссылка
     * @return Тип отображения ссылки
     */
    public static ReferenceDisplayType getReferenceDisplayType(Reference reference) {

        DisplayExpression displayExpression = reference.getDisplayExpression();
        if (displayExpression != null && displayExpression.getValue() != null)
            return ReferenceDisplayType.DISPLAY_EXPRESSION;

        if (reference.getDisplayField() != null)
            return ReferenceDisplayType.DISPLAY_FIELD;

        return null;
    }

    /**
     * Формирование sql-текста для значения отображаемого выражения.
     *
     * @param displayField  поле для получения отображаемого значения
     * @param table         таблица, к которой привязано поле
     * @return Текст для подстановки в SQL
     */
    public static String sqlFieldExpression(String displayField, String table) {
        return table + "." + addDoubleQuotes(displayField);
    }

    /**
     * Формирование sql-текста для значения отображаемого выражения.
     *
     * @param displayExpression выражение для вычисления отображаемого значения
     * @param table             таблица, к которой привязаны поля-placeholder`ы
     * @return Текст для подстановки в SQL
     */
    public static String sqlDisplayExpression(DisplayExpression displayExpression, String table) {
        String sqlDisplayExpression = StringEscapeUtils.escapeSql(displayExpression.getValue());
        Map<String, String> map = new HashMap<>();
        for (String placeholder : displayExpression.getPlaceholders()) {
            map.put(placeholder, "' || " + table + "." + addDoubleQuotes(placeholder) + " || '");
        }
        return addSingleQuotes(StrSubstitutor.replace(sqlDisplayExpression, map));
    }
}

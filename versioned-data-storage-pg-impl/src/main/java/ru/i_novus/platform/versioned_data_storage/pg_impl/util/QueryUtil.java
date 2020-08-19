package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import org.apache.commons.text.StringSubstitutor;
import ru.i_novus.platform.datastorage.temporal.enums.ReferenceDisplayType;
import ru.i_novus.platform.datastorage.temporal.model.*;
import ru.i_novus.platform.datastorage.temporal.model.value.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;

import java.io.Serializable;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static java.util.stream.Collectors.joining;
import static ru.i_novus.platform.datastorage.temporal.model.StorageConstants.*;
import static ru.i_novus.platform.datastorage.temporal.util.StorageUtils.escapeFieldName;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.*;

/**
 * @author lgalimova
 * @since 21.03.2018
 */
public class QueryUtil {

    private QueryUtil() {
        throw new UnsupportedOperationException();
    }

    /**
     * Преобразование полученных данных в список записей.
     * <p>
     * При получении всех данных необходимо использовать
     * совместно с {@link #getSelectFields} при {@code detailed} = true.
     *
     * @param fields список полей
     * @param data   список данных
     * @return Список записей
     */
    public static List<RowValue> toRowValues(List<Field> fields, List<Object[]> data) {

        List<RowValue> result = new ArrayList<>(data.size());
        for (Object objects : data) {
            LongRowValue rowValue = new LongRowValue();
            if (objects instanceof Object[]) {
                addToRowValue((Object[]) objects, fields, rowValue);

            } else {
                rowValue.getFieldValues().add(getFieldValue(fields.get(0), objects));
            }
            result.add(rowValue);
        }
        return result;
    }

    private static void addToRowValue(Object[] row, List<Field> fields, LongRowValue rowValue) {

        Iterator<Field> fieldIterator = fields.iterator();

        int i = 0;
        while (i < row.length) {
            Field field = fieldIterator.next();
            Object value = row[i];

            if (i == 0) { // SYS_RECORD_ID
                rowValue.setSystemId(value != null ? Long.parseLong(value.toString()) : null);

            } else if (i == 1 && SYS_HASH.equals(field.getName())) { // SYS_HASH
                rowValue.setHash(value != null ? value.toString() : null);

            } else { // FIELD
                if (field instanceof ReferenceField && (i + 1 < row.length)) {
                    Object displayValue = row[i + 1];
                    value = new Reference(value != null ? value.toString() : null,
                            displayValue != null ? displayValue.toString() : null);
                    i++;
                }

                rowValue.getFieldValues().add(getFieldValue(field, value));
            }

            i++;
        }
    }

    /** Получение наименования поля с кавычками из наименования, сформированного по getHashUsedFieldNames. */
    public static String getFieldClearName(String fieldName) {

        int closeQuoteIndex = fieldName.indexOf('"', 1);
        return fieldName.substring(0, closeQuoteIndex + 1);
    }

    /**
     * Получение значения поля в виде объекта соответствующего класса.
     *
     * @param field поле
     * @param value значение
     * @return Значение поля
     */
    private static FieldValue getFieldValue(Field field, Object value) {

        String name = field.getName();

        if (field instanceof BooleanField) {
            return new BooleanFieldValue(name, (Boolean) value);
        }

        if (field instanceof DateField) {
            return new DateFieldValue(name, value != null ? ((java.sql.Date) value).toLocalDate() : null);
        }

        if (field instanceof FloatField) {
            return new FloatFieldValue(name, (Number) value);
        }

        if (field instanceof IntegerField) {
            return new IntegerFieldValue(name,
                    value != null ? new BigInteger(value.toString()) : null);
        }

        if (field instanceof ReferenceField) {
            return new ReferenceFieldValue(name, (Reference) value);
        }

        return new StringFieldValue(name, value != null ? value.toString() : null);
    }

    /**
     * Преобразование значения поля в параметр запроса.
     *
     * @param fieldValue значение поля
     * @return Параметр запроса
     */
    public static Serializable toQueryParameter(FieldValue fieldValue) {

        if (fieldValue.getValue() == null)
            return null;

        if (fieldValue instanceof ReferenceFieldValue) {
            Reference refValue = ((ReferenceFieldValue) fieldValue).getValue();
            return refValue.getValue();
        }

        return fieldValue.getValue();
    }

    /**
     * Проверка значения поля на отсутствие.
     *
     * @param fieldValue значение поля
     * @return Отсутствие значения
     */
    public static boolean isFieldValueNull(FieldValue<?> fieldValue) {
        return fieldValue.getValue() == null || fieldValue.getValue().equals("null");
    }

    /**
     * Генерация списка полей для запроса.
     * 
     * @param alias    псевдоним таблицы
     * @param fields   список полей таблицы
     * @param detailed отображение дополнительных частей составных полей
     */
    public static String getSelectFields(String alias, List<Field> fields, boolean detailed) {

        if (isNullOrEmpty(alias))
            alias = "";

        List<String> selectedFields = new ArrayList<>();
        for (Field<?> field : fields) {
            toSelectedField(alias, field, fields.indexOf(field), detailed, selectedFields);
        }

        return String.join(", ", selectedFields);
    }

    private static void toSelectedField(String alias, Field<?> field, int index,
                                        boolean detailed, List<String> selectedFields) {

        String selectedField = escapeFieldName(alias, field.getName());

        if (field instanceof ReferenceField) {
            String queryValue = selectedField +
                    REFERENCE_FIELD_VALUE_OPERATOR + addSingleQuotes(REFERENCE_VALUE_NAME) +
                    ALIAS_OPERATOR + sqlFieldAlias(field, index, alias, REFERENCE_VALUE_NAME);
            selectedFields.add(queryValue);

            if (detailed) {
                String queryDisplayValue = selectedField +
                        REFERENCE_FIELD_VALUE_OPERATOR + addSingleQuotes(REFERENCE_DISPLAY_VALUE_NAME) +
                        ALIAS_OPERATOR + sqlFieldAlias(field, index, alias, REFERENCE_DISPLAY_VALUE_NAME);
                selectedFields.add(queryDisplayValue);
            }
        } else {
            if (field instanceof TreeField) {
                selectedField += "\\:\\:text";
            }

            selectedField += ALIAS_OPERATOR + sqlFieldAlias(field, index, alias);
            selectedFields.add(selectedField);
        }
    }

    private static String sqlFieldAlias(Field<?> field, int index, String prefix) {

        return addDoubleQuotes(prefix + field.getName() + index);
    }

    private static String sqlFieldAlias(Field<?> field, int index, String prefix, String suffix) {

        return addDoubleQuotes(prefix + field.getName() + index + "." + suffix);
    }

    @SuppressWarnings("all")
    public static boolean isVarcharType(String type) {
        return StringField.TYPE.equals(type) || IntegerStringField.TYPE.equals(type);
    }

    /**
     * Создание поля по наименованию и типу.
     *
     * @param name наименование
     * @param type тип
     * @return Поле
     */
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
     * Получение наименования поля с учётом преобразования типов для использования в запросах.
     *
     * @param fieldName поле
     * @param oldType   старый тип
     * @param newType   новый тип
     * @return Наименование поля с учётом преобразования
     */
    public static String getFieldNameByType(String fieldName, String oldType, String newType) {

        if (DateField.TYPE.equals(oldType) && isVarcharType(newType)) {
            return "to_char(" + fieldName + ", '" + QUERY_DATE_FORMAT + "')";
        }

        if (DateField.TYPE.equals(newType) && StringField.TYPE.equals(oldType)) {
            return "to_date(" + fieldName + ", '" + QUERY_DATE_FORMAT + "')";
        }

        if (ReferenceField.TYPE.equals(oldType)) {
            return "(" + fieldName +
                    REFERENCE_FIELD_VALUE_OPERATOR + addSingleQuotes(REFERENCE_VALUE_NAME) +
                    ")" + "\\:\\:varchar\\:\\:" + newType;
        }

        if (ReferenceField.TYPE.equals(newType)) {
            return String.format("nullif(jsonb_build_object(%1$s, %2$s), jsonb_build_object(%1$s, null))",
                    addSingleQuotes(REFERENCE_VALUE_NAME), fieldName);
        }

        if (isVarcharType(oldType) || isVarcharType(newType)) {
            return fieldName + "\\:\\:" + newType;
        }

        return fieldName + "\\:\\:varchar\\:\\:" + newType;
    }

    /**
     * Получение типа отображения ссылки.
     *
     * @param reference ссылка
     * @return Тип отображения ссылки
     */
    public static ReferenceDisplayType getReferenceDisplayType(Reference reference) {

        DisplayExpression displayExpression = reference.getDisplayExpression();
        if (displayExpression != null && displayExpression.getValue() != null) {
            return ReferenceDisplayType.DISPLAY_EXPRESSION;
        }

        if (reference.getDisplayField() != null) {
            return ReferenceDisplayType.DISPLAY_FIELD;
        }

        return null;
    }

    /**
     * Формирование sql-текста для значения отображаемого выражения.
     *
     * @param displayField поле для получения отображаемого значения
     * @param tableAlias   таблица, к которой привязано поле
     * @return Текст для подстановки в SQL
     */
    public static String sqlFieldExpression(String displayField, String tableAlias) {

        return escapeFieldName(tableAlias, displayField);
    }

    /**
     * Формирование sql-текста для значения отображаемого выражения.
     *
     * @param displayExpression выражение для вычисления отображаемого значения
     * @param tableAlias        таблица, к которой привязаны поля-placeholder`ы
     * @return Текст для подстановки в SQL
     */
    public static String sqlDisplayExpression(DisplayExpression displayExpression, String tableAlias) {

        final String valueFormat = "' || coalesce(%1$s\\:\\:text, '%2$s') || '";

        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, String> e : displayExpression.getPlaceholders().entrySet()) {
            String value = e.getValue() == null ? "" : e.getValue();
            value = String.format(valueFormat, escapeFieldName(tableAlias, e.getKey()), value);
            map.put(e.getKey(), value);
        }

        String escapedDisplayExpression = escapeSql(displayExpression.getValue());
        String displayValue = createDisplayExpressionSubstitutor(map).replace(escapedDisplayExpression);
        return addSingleQuotes(displayValue);
    }

    /**
     * <p>Escapes the characters in a <code>String</code> to be suitable to pass to an SQL query.
     *
     * <p>For example,
     * <pre>
     *      statement.executeQuery("SELECT * FROM data_table WHERE name='" +
     *          StringEscapeUtils.escapeSql("i_novus' style") +
     *      "'");
     * </pre>
     * <p>
     * <p>At present, this method only turns single-quotes into doubled single-quotes
     * (<code>"i_novus' style"</code> => <code>"i_novus'' style"</code>).
     * It does not handle the cases of percent (%) or underscore (_) for use in LIKE clauses.
     *
     * see http://www.jguru.com/faq/view.jsp?EID=8881
     * 
     * @param str the string to escape, may be null
     * @return A new String, escaped for SQL, <code>null</code> if null string input
     */
    public static String escapeSql(String str) {

        return (str == null) ? null : str.replace("'", "''");
    }

    /** Преобразование списка значений в БД-строку-массив. */
    public static String valuesToDbArray(List<?> values) {

        return values.stream()
                .map(String::valueOf)
                .collect(joining(",", "{", "}"));
    }

    /** Преобразование списка строковых значений в БД-строку-массив. */
    public static String stringsToDbArray(List<String> values) {

        return "{" + String.join(",", values) + "}";
    }

    /** Создание объекта подстановки в выражение для вычисления отображаемого значения. */
    public static StringSubstitutor createDisplayExpressionSubstitutor(Map<String, Object> map) {

        StringSubstitutor substitutor = new StringSubstitutor(map,
                DisplayExpression.PLACEHOLDER_START, DisplayExpression.PLACEHOLDER_END);
        substitutor.setValueDelimiter(DisplayExpression.PLACEHOLDER_DEFAULT_DELIMITER);
        return substitutor;
    }

    public static String formatDateTime(LocalDateTime localDateTime) {
        return (localDateTime != null) ? localDateTime.format(DATETIME_FORMATTER) : null;
    }

    public static String toTimestamp(String value) {

        // Учесть другие константы: now, today etc.
        switch(value) {
            case MIN_TIMESTAMP_VALUE:
            case MAX_TIMESTAMP_VALUE:
                return value;

            default:
                return String.format(TO_TIMESTAMP, addSingleQuotes(value));
        }
    }

    public static String toTimestampWithoutTimeZone(String value) {
        return toTimestamp(value) + TIMESTAMP_WITHOUT_TIME_ZONE;
    }

    public static Object truncateDateTo(LocalDateTime date, ChronoUnit unit, Object defaultValue) {
        return date != null ? date.truncatedTo(unit) : defaultValue;
    }
}

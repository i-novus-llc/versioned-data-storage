package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import org.apache.commons.text.StringSubstitutor;
import ru.i_novus.platform.datastorage.temporal.enums.ReferenceDisplayType;
import ru.i_novus.platform.datastorage.temporal.model.*;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;

import java.io.Serializable;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.addSingleQuotes;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.stringFrom;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.StorageConstants.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StorageUtils.*;

/**
 * @author lgalimova
 * @since 21.03.2018
 */
@SuppressWarnings({"rawtypes","unchecked","java:S3740"})
public class QueryUtil {

    private QueryUtil() {
        // Nothing to do.`
    }

    /**
     * Преобразование полученных данных в список записей.
     * <p>
     * При получении всех данных необходимо использовать
     * совместно с {@link #toSelectedFields} при {@code detailed} = true.
     *
     * @param fields     список полей
     * @param valueParts дополнительные части значений для составных полей
     * @param data       данные или список данных
     * @return Список записей
     */
    public static List<RowValue> toRowValues(List<Field> fields,
                                             Set<FieldValuePartEnum> valueParts,
                                             List<Object> data) {

        List<RowValue> result = new ArrayList<>(data.size());
        for (Object row : data) {
            LongRowValue rowValue = new LongRowValue();
            if (row instanceof Object[]) {
                addToRowValue((Object[]) row, fields, valueParts, rowValue);

            } else {
                rowValue.getFieldValues().add(toFieldValue(fields.get(0), row));
            }
            result.add(rowValue);
        }
        return result;
    }

    private static void addToRowValue(Object[] row, List<Field> fields,
                                      Set<FieldValuePartEnum> valueParts,
                                      LongRowValue rowValue) {

        Iterator<Field> fieldIterator = fields.iterator();

        final AtomicInteger next = new AtomicInteger(0);
        while (next.get() < row.length) {
            addNextFieldValue(next, row, fieldIterator.next(), valueParts, rowValue);
        }
    }

    private static void addNextFieldValue(AtomicInteger index,
                                          Object[] row, Field field,
                                          Set<FieldValuePartEnum> valueParts,
                                          LongRowValue rowValue) {
        int i = index.get();
        Object value = row[i];

        if (i == 0) { // SYS_RECORD_ID

            rowValue.setSystemId(value != null ? Long.parseLong(value.toString()) : null);
            index.incrementAndGet();
            return;
        }

        if (i == 1 && SYS_HASH.equals(field.getName())) { // SYS_HASH

            rowValue.setHash(stringFrom(value));
            index.incrementAndGet();
            return;
        }

        // SPECIAL FIELDS:
        if (field instanceof ReferenceField) {

            value = getNextReference(index, row, value, valueParts);
        }

        rowValue.getFieldValues().add(toFieldValue(field, value));

        index.incrementAndGet();
    }

    /** Формирование ссылки из полученных частей значений в записи. */
    private static Reference getNextReference(AtomicInteger index,
                                              Object[] row, Object value,
                                              Set<FieldValuePartEnum> valueParts) {
        Object hash = null;
        if (valueParts.contains(FieldValuePartEnum.REFERENCE_HASH)) {

            int i = index.incrementAndGet();
            hash = (i < row.length) ? row[i] : null;
        }

        Object displayValue = null;
        if (valueParts.contains(FieldValuePartEnum.REFERENCE_DISPLAY_VALUE)) {

            int i = index.incrementAndGet();
            displayValue = (i < row.length) ? row[i] : null;
        }

        return new Reference(stringFrom(hash), stringFrom(value), stringFrom(displayValue));
    }

    /** Получение наименования поля с кавычками для вычисления hash и fts. */
    public static String getHashUsedFieldName(Field field) {

        String name = escapeFieldName(field.getName());

        if (REFERENCE_FIELD_SQL_TYPE.equals(field.getType()))
            name += REFERENCE_FIELD_VALUE_OPERATOR + addSingleQuotes(REFERENCE_VALUE_NAME);

        return name;
    }

    /** Получение наименования поля с кавычками из наименования, сформированного по getHashUsedFieldNames. */
    public static String getClearedFieldName(String fieldName) {

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
    private static FieldValue toFieldValue(Field field, Object value) {

        return field.valueOf(toValueByField(field, value));
    }

    /**
     * Получение значения в виде соответствующего объекта.
     *
     * @param field поле
     * @param value значение
     * @return Значение
     */
    public static Serializable toValueByField(Field field, Object value) {

        if (value == null) {
            return null;
        }

        if (field instanceof DateField) {
            return ((java.sql.Date) value).toLocalDate();
        }

        if (field instanceof IntegerField) {
            return new BigInteger(value.toString());
        }

        if (field instanceof BooleanField ||
                field instanceof FloatField ||
                field instanceof ReferenceField) {
            return (Serializable) value;
        }

        return value.toString();
    }

    /**
     * Преобразование списка наименований полей в наименования колонок.
     *
     * @param fieldNames список наименований полей
     * @return Наименования колонок
     */
    public static String toStrColumns(List<String> fieldNames) {

        return fieldNames.stream().filter(Objects::nonNull).collect(joining(", "));
    }

    /**
     * Преобразование списка наименований полей в наименования колонок с учётом псевдонима.
     *
     * @param tableAlias псевдоним таблицы/записи
     * @param fieldNames список наименований полей
     * @return Наименования колонок
     */
    public static String toAliasColumns(String tableAlias, List<String> fieldNames) {

        return fieldNames.stream().filter(Objects::nonNull)
                .map(name -> aliasColumnName(tableAlias, name))
                .collect(joining(", "));
    }

    /**
     * Преобразование набора наименований полей с типами в наименования колонок.
     *
     * @param typedNames набор наименований полей с типами
     * @return Наименования колонок
     */
    public static String toStrColumns(Map<String, String> typedNames) {

        return typedNames.keySet().stream().filter(Objects::nonNull).collect(joining(", "));
    }

    /**
     * Преобразование набора наименований полей с типами в наименования колонок с типами.
     *
     * @param typedNames набор наименований полей с типами
     * @return Наименования колонок с типами
     */
    public static String toTypedColumns(Map<String, String> typedNames) {

        return typedNames.keySet().stream().filter(Objects::nonNull)
                .map(name -> typedColumnName(name, typedNames.get(name)))
                .collect(joining(", "));
    }

    /**
     * Преобразование набора наименований полей с типами в наименования колонок с учётом псевдонима.
     *
     * @param tableAlias псевдоним таблицы/записи
     * @param typedNames набор наименований полей с типами
     * @return Наименования колонок
     */
    public static String toAliasColumns(String tableAlias, Map<String, String> typedNames) {

        return typedNames.keySet().stream().filter(Objects::nonNull)
                .map(name -> aliasColumnName(tableAlias, name))
                .collect(joining(", "));
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
     * Проверка наличия поля в списке по наименованию.
     *
     * @param fieldName наименование поля
     * @param fields    список полей
     * @return Результат проверки
     */
    public static boolean hasField(String fieldName, List<Field> fields) {

        return fields.stream().anyMatch(field -> fieldName.equals(field.getName()));
    }

    /**
     * Поиск поля в списке по наименованию.
     *
     * @param fieldName наименование поля
     * @param fields    список полей
     * @return Поле или null
     */
    public static Field findField(String fieldName, List<Field> fields) {

        return fields.stream()
                .filter(field -> fieldName.equals(field.getName()))
                .findFirst().orElse(null);
    }

    /**
     * Получение списка полей для запроса.
     *
     * @param alias      псевдоним хранилища
     * @param fields     список полей таблицы
     * @param valueParts дополнительные части значений для составных полей
     */
    public static String toSelectedFields(String alias, List<Field> fields,
                                          Set<FieldValuePartEnum> valueParts) {

        if (alias == null)
            alias = "";

        List<String> selectedFields = new ArrayList<>();
        for (Field<?> field : fields) {
            toSelectedField(alias, field, fields.indexOf(field), valueParts, selectedFields);
        }

        return String.join(", ", selectedFields);
    }

    private static void toSelectedField(String alias, Field<?> field, int index,
                                        Set<FieldValuePartEnum> valueParts,
                                        List<String> selectedFields) {

        String selectedField = aliasFieldName(alias, field.getName());

        if (field instanceof ReferenceField) {
            String queryValue = selectedField +
                    REFERENCE_FIELD_VALUE_OPERATOR + addSingleQuotes(REFERENCE_VALUE_NAME) +
                    " AS " + sqlFieldAlias(field, index, alias, REFERENCE_VALUE_NAME);
            selectedFields.add(queryValue);

            if (valueParts.contains(FieldValuePartEnum.REFERENCE_HASH)) {
                String queryHash = selectedField +
                        REFERENCE_FIELD_VALUE_OPERATOR + addSingleQuotes(REFERENCE_HASH_NAME) +
                        " AS " + sqlFieldAlias(field, index, alias, REFERENCE_HASH_NAME);
                selectedFields.add(queryHash);
            }

            if (valueParts.contains(FieldValuePartEnum.REFERENCE_DISPLAY_VALUE)) {
                String queryDisplayValue = selectedField +
                        REFERENCE_FIELD_VALUE_OPERATOR + addSingleQuotes(REFERENCE_DISPLAY_VALUE_NAME) +
                        " AS " + sqlFieldAlias(field, index, alias, REFERENCE_DISPLAY_VALUE_NAME);
                selectedFields.add(queryDisplayValue);
            }
        } else {
            if (field instanceof TreeField) {
                selectedField += "\\:\\:text";
            }

            selectedField += " AS " + sqlFieldAlias(field, index, alias);
            selectedFields.add(selectedField);
        }
    }

    private static String sqlFieldAlias(Field<?> field, int index, String prefix) {

        return escapeCustomName(prefix + field.getName() + index);
    }

    private static String sqlFieldAlias(Field<?> field, int index, String prefix, String suffix) {

        return escapeCustomName(prefix + field.getName() + index + "." + suffix);
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
    public static String useFieldNameByType(String fieldName, String oldType, String newType) {

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
     * @param tableAlias   таблица, к которой привязано поле
     * @param displayField поле для получения отображаемого значения
     * @return Текст для подстановки в SQL
     */
    public static String sqlDisplayField(String tableAlias, String displayField) {

        return aliasFieldName(tableAlias, displayField);
    }

    /**
     * Формирование sql-текста для значения отображаемого выражения.
     *
     * @param tableAlias        таблица, к которой привязаны поля-placeholder`ы
     * @param displayExpression выражение для вычисления отображаемого значения
     * @return Текст для подстановки в SQL
     */
    public static String sqlDisplayExpression(String tableAlias, DisplayExpression displayExpression) {

        final String valueFormat = "' || coalesce(%1$s\\:\\:text, '%2$s') || '";

        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, String> e : displayExpression.getPlaceholders().entrySet()) {
            String value = e.getValue() == null ? "" : e.getValue();
            value = String.format(valueFormat, aliasFieldName(tableAlias, e.getKey()), value);
            map.put(e.getKey(), value);
        }

        String escapedDisplayExpression = escapeSql(displayExpression.getValue());
        String displayValue = createDisplayExpressionSubstitutor(map).replace(escapedDisplayExpression);
        return addSingleQuotes(displayValue);
    }

    /**
     * Escapes the characters in a <code>String</code> to be suitable to pass to an SQL query.
     * <p>
     * At present, this method only turns single-quotes into doubled single-quotes
     * (<code>"i_novus' style"</code> => <code>"i_novus'' style"</code>).
     * It does not handle the cases of percent (%) or underscore (_) for use in LIKE clauses.
     *
     * @param str the string to escape, may be null
     * @return A new String, escaped for SQL, <code>null</code> if null string input
     */
    public static String escapeSql(String str) {

        return (str == null) ? null : str.replace("'", "''");
    }

    /** Преобразование списка systemIds в список для LongRowValue. */
    public static List<Long> toLongSystemIds(List<Object> systemIds) {
        return systemIds.stream().map(systemId -> (Long) systemId).collect(toList());
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

        return (value != null) ? toTimestamp(value) + TIMESTAMP_WITHOUT_TIME_ZONE : QUERY_NULL_VALUE;
    }

    public static Object truncateDateTo(LocalDateTime date, ChronoUnit unit, Object defaultValue) {

        return date != null ? date.truncatedTo(unit) : defaultValue;
    }
}

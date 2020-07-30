package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import net.n2oapp.criteria.api.Criteria;
import org.apache.commons.text.StringSubstitutor;
import ru.i_novus.platform.datastorage.temporal.enums.ReferenceDisplayType;
import ru.i_novus.platform.datastorage.temporal.model.*;
import ru.i_novus.platform.datastorage.temporal.model.value.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static ru.i_novus.platform.datastorage.temporal.model.StorageConstants.*;
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

                    } else if (i == 1 && SYS_HASH.equals(field.getName())) { // SYS_HASH
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

        List<String> queryFields = new ArrayList<>();
        for (Field<?> field : fields) {
            String query = formatFieldForQuery(field.getName(), alias);

            if (field instanceof ReferenceField) {
                final String jsonOperator = "->>";

                String queryValue = query + jsonOperator +
                        addSingleQuotes(REFERENCE_VALUE_NAME) +
                        ALIAS_OPERATOR +
                        sqlFieldAlias(field, alias, REFERENCE_VALUE_NAME);
                queryFields.add(queryValue);

                if (detailed) {
                    String queryDisplayValue = query + jsonOperator +
                            addSingleQuotes(REFERENCE_DISPLAY_VALUE_NAME) +
                            ALIAS_OPERATOR +
                            sqlFieldAlias(field, alias, REFERENCE_DISPLAY_VALUE_NAME);
                    queryFields.add(queryDisplayValue);
                }
            } else {
                if (field instanceof TreeField) {
                    query += "\\:\\:text";
                }
                query += ALIAS_OPERATOR +
                        sqlFieldAlias(field, alias, fields.indexOf(field));
                queryFields.add(query);
            }
        }
        return String.join(", ", queryFields);
    }

    private static String sqlFieldAlias(Field<?> field, String prefix, String suffix) {
        return addDoubleQuotes(prefix + field.getName() + "." + suffix);
    }

    private static String sqlFieldAlias(Field<?> field, String prefix, int index) {
        return addDoubleQuotes(prefix + field.getName() + index);
    }

    public static String formatFieldForQuery(String field, String alias) {

        if (alias == null) {
            alias = "";

        } else if (!alias.isEmpty()) {
            alias = alias + NAME_SEPARATOR;
        }

        if (field.contains(REFERENCE_FIELD_VALUE_OPERATOR)) {
            String[] queryParts = field.split(REFERENCE_FIELD_VALUE_OPERATOR);

            return alias + addDoubleQuotes(queryParts[0]) +
                    REFERENCE_FIELD_VALUE_OPERATOR + queryParts[1];
        } else {
            return alias + addDoubleQuotes(field);
        }
    }

    public static String getSchemaName(String schemaName) {

        return isNullOrEmpty(schemaName) ? DATA_SCHEMA_NAME : schemaName;
    }

    public static String escapeTableName(String schemaName, String tableName) {

        return getSchemaName(schemaName) + NAME_SEPARATOR + addDoubleQuotes(tableName);
    }

    public static String escapeFieldName(String tableAlias, String fieldName) {

        String escapedFieldName = addDoubleQuotes(fieldName);
        return isNullOrEmpty(tableAlias) ? escapedFieldName : tableAlias + NAME_SEPARATOR + escapedFieldName;
    }

    public static String escapeSequenceName(String tableName) {
        return addDoubleQuotes(tableName + "_" + SYS_PRIMARY_COLUMN + "_seq");
    }

    public static String escapeSchemaSequenceName(String schemaName, String tableName) {

        return getSchemaName(schemaName) + NAME_SEPARATOR + escapeSequenceName(tableName);
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
     * @param displayField поле для получения отображаемого значения
     * @param tableAlias   таблица, к которой привязано поле
     * @return Текст для подстановки в SQL
     */
    public static String sqlFieldExpression(String displayField, String tableAlias) {

        return tableAlias + NAME_SEPARATOR + addDoubleQuotes(displayField);
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

    /** Создание объекта подстановки в выражение для вычисления отображаемого значения. */
    public static StringSubstitutor createDisplayExpressionSubstitutor(Map<String, Object> map) {

        StringSubstitutor substitutor = new StringSubstitutor(map,
                DisplayExpression.PLACEHOLDER_START, DisplayExpression.PLACEHOLDER_END);
        substitutor.setValueDelimiter(DisplayExpression.PLACEHOLDER_DEFAULT_DELIMITER);
        return substitutor;
    }

    public static Object truncateDateTo(LocalDateTime date, ChronoUnit unit, Object defaultValue) {
        return date != null ? date.truncatedTo(unit) : defaultValue;
    }
}

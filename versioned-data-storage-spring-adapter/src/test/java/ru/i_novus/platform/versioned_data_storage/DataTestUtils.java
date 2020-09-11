package ru.i_novus.platform.versioned_data_storage;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.LongRowValue;
import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.value.IntegerFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.model.value.StringFieldValue;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.IntegerField;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.StringField;
import ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil;

import javax.persistence.EntityManager;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.StorageConstants.SYS_PRIMARY_COLUMN;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StorageUtils.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.StringUtils.addDoubleQuotes;

@SuppressWarnings("java:S3740")
public class DataTestUtils {

    public static final String TEST_SCHEMA_NAME = "data_test";
    public static final String NONEXISTENT_SCHEMA_NAME = "data_null";

    public static final String FIELD_ID_CODE = "ID"; // Идентификатор
    public static final String FIELD_NAME_CODE = "NAME"; // Наименование
    public static final String FIELD_CODE_CODE = "CODE"; // Код

    private static final int INDEX_TO_ID_FACTOR = 10;

    public static final List<String> dataNames = asList(
            "первый", "второй", "третий", "четвёртый", "пятый", "шестой"
    );
    public static final List<String> testNames = asList(
            "first", "second", "third", "fourth", "fifth", "sixth"
    );
    private static final int MIXED_NAME_DIVIDER = 2;

    private DataTestUtils() {
        // Nothing to do.
    }

    /** Формирование смешанного списка наименований из dataNames + testNames. */
    public static List<String> getMixedNames() {

        int allCount = dataNames.size();

        return IntStream.range(0, allCount)
                .mapToObj(DataTestUtils::toMixedName)
                .collect(toList());
    }

    /** Формирование наименования из dataNames + testNames с учётом индекса. */
    public static String toMixedName(int index) {

        int allCount = dataNames.size();

        return (index < allCount / MIXED_NAME_DIVIDER) ? dataNames.get(index) : testNames.get(index);
    }

    /** Формирование списка основных полей. */
    public static List<Field> newIdNameFields() {

        List<Field> fields = new ArrayList<>();

        Field idField = new IntegerField(FIELD_ID_CODE);
        fields.add(idField);

        Field nameField = new StringField(FIELD_NAME_CODE);
        fields.add(nameField);

        return fields;
    }

    /** Поиск поля по наименованию. */
    public static Field findFieldOrThrow(String fieldName, List<Field> fields) {

        Field field = QueryUtil.findField(fieldName, fields);
        if (field == null)
            throw new IllegalArgumentException("field '" + fieldName + "' is not found");

        return field;
    }

    /** Преобразование проверяемых данных для выполнения операции. */
    public static List<RowValue> toRowValues(int count, Function<Integer, RowValue> toRowValue) {

        return IntStream.range(0, count).boxed().map(toRowValue).collect(toList());
    }

    /** Преобразование основных полей проверяемых данных для выполнения операции. */
    public static List<RowValue> toIdNameRowValues(List<Field> fields, List<String> nameValues) {

        return toRowValues(nameValues.size(), index -> toIdNameRowValue(index, nameValues.get(index), fields));
    }

    /** Преобразование основных полей проверяемых данных для выполнения операции. */
    public static RowValue toIdNameRowValue(int index, String name, List<Field> fields) {

        FieldValue idValue = findFieldOrThrow(FIELD_ID_CODE, fields).valueOf(indexToId(index));
        FieldValue nameValue = findFieldOrThrow(FIELD_NAME_CODE, fields).valueOf(name);

        List<FieldValue> fieldValues = new ArrayList<>(2);
        fieldValues.add(idValue);
        fieldValues.add(nameValue);

        return new LongRowValue(indexToSystemId(index), fieldValues);
    }

    /** Преобразование параметров в критерий поиска данных. */
    public static StorageDataCriteria toCriteria(String storageCode, List<Field> fields) {

        return new StorageDataCriteria(storageCode, null, null, fields);
    }

    /** Преобразование параметров в критерий поиска данных. */
    public static StorageDataCriteria toCriteria(String schemaName, String tableName, List<Field> fields) {

        return toCriteria(toStorageCode(schemaName, tableName), fields);
    }

    /** Получение значения поля ID из результата поиска. */
    public static BigInteger getRowIdFieldValue(RowValue rowValue) {

        return (BigInteger) rowValue.getFieldValue(FIELD_ID_CODE).getValue();
    }

    /** Сравнение объектов с учётом хеша и преобразования в строку. */
    public static void assertObjects(BiConsumer<Object, Object> objectAssert, Object current, Object actual) {

        objectAssert.accept(current, actual);

        if (current != null && actual != null) {
            objectAssert.accept(current.hashCode(), actual.hashCode());
            objectAssert.accept(current.toString(), actual.toString());
        }
    }

    /** Сравнение результата поиска данных с проверяемыми данными. */
    public static void assertValues(List<RowValue> dataValues, List<String> nameValues) {

        assertNotNull(dataValues);
        if (nameValues.isEmpty()) {
            assertTrue(dataValues.isEmpty());
            return;
        }

        assertEquals(nameValues.stream().filter(Objects::nonNull).count(), dataValues.size());

        IntStream.range(0, nameValues.size()).forEach(index -> {
            StringFieldValue fieldNameValue = dataValues.stream()
                    .filter(rowValue -> {
                        IntegerFieldValue idValue = (IntegerFieldValue) rowValue.getFieldValue(FIELD_ID_CODE);
                        BigInteger value = idValue != null ? idValue.getValue() : null;
                        return value != null && value.equals(indexToId(index));
                    })
                    .map(rowValue -> (StringFieldValue) rowValue.getFieldValue(FIELD_NAME_CODE))
                    .findFirst().orElse(null);

            String indexNameValue = nameValues.get(index);
            if (indexNameValue == null) {
                assertNull(fieldNameValue);

            } else {
                assertNotNull(fieldNameValue);
                assertEquals(indexNameValue, fieldNameValue.getValue());
            }
        });
    }

    /** Преобразование индекса nameValues в системный идентификатор. */
    public static Long indexToSystemId(int index) {

        return (long) index;
    }

    /** Преобразование индекса nameValues в значение поля FIELD_ID_CODE. */
    public static BigInteger indexToId(int index) {

        return BigInteger.valueOf(index * INDEX_TO_ID_FACTOR);
    }

    /** Преобразование значения поля FIELD_ID_CODE в индекс nameValues. */
    public static int idToIndex(BigInteger id) {

        return id.divide(BigInteger.valueOf(INDEX_TO_ID_FACTOR)).intValue();
    }

    public static boolean tableSequenceExists(String storageCode, EntityManager entityManager) {

        final String selectSequenceExists = "SELECT EXISTS(\n" +
                "  SELECT 1 \n" +
                "    FROM pg_class \n" +
                "   WHERE relkind = 'S' \n" +
                "     AND relname = :seqOnlyName \n" +
                "     AND oid\\:\\:regclass\\:\\:text = :seqFullName \n" +
                ")";

        String seqOnlyName = tableSequenceName(toTableName(storageCode));
        String seqFullName = escapeStorageSequenceName(storageCode);

        Boolean result = (Boolean) entityManager.createNativeQuery(selectSequenceExists)
                        .setParameter("seqOnlyName", seqOnlyName)
                        .setParameter("seqFullName", seqFullName)
                        .getSingleResult();

        return result != null && result;
    }

    public static boolean tableTriggerExists(String storageCode, String triggerName, EntityManager entityManager) {

        final String selectTriggerExists = "SELECT EXISTS(\n" +
                "  SELECT 1 \n" +
                "    FROM pg_trigger \n" +
                "   WHERE not tgisinternal \n" +
                "     AND tgname = :triggerName \n" +
                "     AND tgrelid\\:\\:regclass\\:\\:text = :tableName \n" +
                ")";

        String tableName = escapeStorageTableName(storageCode);

        Boolean result = (Boolean) entityManager.createNativeQuery(selectTriggerExists)
                        .setParameter("triggerName", triggerName)
                        .setParameter("tableName", tableName)
                        .getSingleResult();

        return result != null && result;
    }
}

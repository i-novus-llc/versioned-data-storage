package ru.i_novus.platform.versioned_data_storage;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.LongRowValue;
import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.value.IntegerFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.model.value.StringFieldValue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static ru.i_novus.platform.datastorage.temporal.util.StorageUtils.toStorageCode;

public class DataTestUtils {

    public static final String TEST_SCHEMA_NAME = "data_test";
    public static final String NULL_SCHEMA_NAME = "data_null";

    public static final String FIELD_ID_CODE = "id";
    public static final String FIELD_NAME_CODE = "name";
    public static final String FIELD_CODE_CODE = "CODE";

    public static final List<String> dataNames = asList("первый", "второй", "третий", "четвёртый");
    public static final List<String> testNames = asList("first", "second", "third", "fourth");

    private DataTestUtils() {
        // Nothing to do.
    }

    /** Поиск поля по наименованию. */
    public static Field findFieldOrThrow(String name, List<Field> fields) {
        return fields.stream()
                .filter(field -> name.equals(field.getName()))
                .findFirst().orElseThrow(() ->
                        new IllegalArgumentException("field '" + name + "' is not found"));
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

    /** Сравнение результата поиска данных с проверяемыми данными. */
    public static void assertValues(List<RowValue> dataValues, List<String> nameValues) {

        assertNotNull(dataValues);
        assertEquals(nameValues.size(), dataValues.size());

        IntStream.range(0, nameValues.size()).forEach(index -> {
            StringFieldValue nameValue = dataValues.stream()
                    .filter(rowValue -> {
                        IntegerFieldValue idValue = (IntegerFieldValue) rowValue.getFieldValue(FIELD_ID_CODE);
                        BigInteger value = idValue != null ? idValue.getValue() : null;
                        return value != null && value.equals(indexToId(index));
                    })
                    .map(rowValue -> (StringFieldValue) rowValue.getFieldValue(FIELD_NAME_CODE))
                    .findFirst().orElse(null);
            assertNotNull(nameValue);
            assertEquals(nameValues.get(index), nameValue.getValue());
        });
    }

    public static Long indexToSystemId(int index) {
        return (long) index;
    }

    public static BigInteger indexToId(int index) {
        return BigInteger.valueOf(index * 10);
    }
}

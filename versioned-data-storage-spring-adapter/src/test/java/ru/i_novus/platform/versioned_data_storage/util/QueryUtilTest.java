package ru.i_novus.platform.versioned_data_storage.util;

import org.junit.Test;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.toValueByField;

public class QueryUtilTest {

    private static final BooleanField BOOLEAN_FIELD = new BooleanField("bool");
    private static final StringField STRING_FIELD = new StringField("str");
    private static final IntegerField INTEGER_FIELD = new IntegerField("int");
    private static final FloatField FLOAT_FIELD = new FloatField("flt");
    private static final DateField DATE_FIELD = new DateField("dt");
    private static final ReferenceField REFERENCE_FIELD = new ReferenceField("ref");
    private static final IntegerStringField INTEGER_STRING_FIELD = new IntegerStringField("isf");
    private static final TestField TEST_FIELD = new TestField("test");

    @Test
    public void testToValueByField() {

        testToValueByBooleanField(Boolean.TRUE);
        testToValueByBooleanField(Boolean.FALSE);

        String strValue = "text";
        assertEquals(strValue, toValueByField(STRING_FIELD, strValue));

        int intValue = 1001;
        assertEquals(BigInteger.valueOf(intValue), toValueByField(INTEGER_FIELD, intValue));

        BigDecimal floatValue = BigDecimal.valueOf(100.001);
        assertEquals(floatValue, toValueByField(FLOAT_FIELD, floatValue));

        LocalDate dateValue = LocalDate.of(2021, 2, 3);
        assertEquals(dateValue, toValueByField(DATE_FIELD, java.sql.Date.valueOf(dateValue)));

        String refValue = "{\"hash\": \"hash1\", \"value\": \"1\", \"displayValue\": \"1-one-один\"}";
        assertEquals(refValue, toValueByField(REFERENCE_FIELD, refValue));

        assertEquals(String.valueOf(intValue), toValueByField(INTEGER_STRING_FIELD, intValue));

        short testValue = 100;
        assertEquals(String.valueOf(testValue), toValueByField(TEST_FIELD, testValue));
    }

    private void testToValueByBooleanField(Boolean boolValue) {
        assertEquals(boolValue, toValueByField(BOOLEAN_FIELD, boolValue));
    }

    @Test
    public void testToValueByFieldForNull() {

        List.of(BOOLEAN_FIELD, STRING_FIELD, INTEGER_FIELD, FLOAT_FIELD,
                DATE_FIELD, REFERENCE_FIELD, INTEGER_STRING_FIELD)
                .forEach(field -> assertNull(toValueByField(field, null)));
        assertNull(toValueByField(TEST_FIELD, null));
    }
}

package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

import java.math.BigDecimal;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class FloatFieldValue extends FieldValue<Number> {

    public FloatFieldValue() {
    }

    public FloatFieldValue(String field, Number value) {
        super(field, toFloatValue(value));
    }

    @Override
    public void setValue(Number value) {
        super.setValue(toFloatValue(value));
    }

    public static BigDecimal toFloatValue(Number value) {

        return value instanceof BigDecimal
                ? (BigDecimal) value
                : (value != null ? new BigDecimal(value.toString()) : null);
    }
}

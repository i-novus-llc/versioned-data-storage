package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;

import java.math.BigInteger;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class IntegerFieldValue extends FieldValue<BigInteger> {

    public IntegerFieldValue() {
    }

    public IntegerFieldValue(String field, BigInteger value) {
        super(field, value);
    }

    public IntegerFieldValue(String field, Integer value) {
        super(field, BigInteger.valueOf(value));
    }


}

package ru.i_novus.platform.datastorage.temporal.model;

import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class LongRowValue extends RowValue<Long> {
    public LongRowValue() {
        super();
    }

    public LongRowValue(Long systemId, List<FieldValue> fieldValues) {
        super(systemId, fieldValues);
    }

    public LongRowValue(FieldValue ... fieldValues) {
        super(null, Arrays.asList(fieldValues));
    }

}

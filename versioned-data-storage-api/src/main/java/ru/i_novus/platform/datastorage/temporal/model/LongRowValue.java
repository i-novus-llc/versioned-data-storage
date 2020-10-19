package ru.i_novus.platform.datastorage.temporal.model;

import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
@SuppressWarnings("rawtypes")
public class LongRowValue extends RowValue<Long> {

    public static final LongRowValue EMPTY = new LongRowValue(null, emptyList(), null);

    public LongRowValue() {
        super();
    }

    public LongRowValue(Long systemId, List<FieldValue> fieldValues) {
        super(systemId, fieldValues);
    }

    public LongRowValue(FieldValue ... fieldValues) {
        super(null, asList(fieldValues));
    }

    public LongRowValue(Long systemId, List<FieldValue> fieldValues, String hash) {
        super(systemId, fieldValues, hash);
    }
}

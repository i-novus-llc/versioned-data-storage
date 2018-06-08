package ru.i_novus.platform.datastorage.temporal.model;

import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;

import java.util.List;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class LongRowValue extends RowValue<Long> {
    public LongRowValue() {
    }

    public LongRowValue(Long systemId, List<FieldValue> fieldValues) {
        super(systemId, fieldValues);
    }
}

package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.Reference;

/**
 * @author lgalimova
 * @since 06.06.2018
 */
public class ReferenceFieldValue extends FieldValue<Reference> {

    public ReferenceFieldValue() {
    }

    public ReferenceFieldValue(String field, Reference value) {
        super(field, value);
    }
}

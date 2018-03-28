package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

import ru.i_novus.platform.datastorage.temporal.model.Field;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class ReferenceField extends Field {

    private boolean getReferenceData;

    public ReferenceField(String name, boolean getReferenceData) {
        super(name);
        this.getReferenceData = getReferenceData;
    }

    public boolean isGetReferenceData() {
        return getReferenceData;
    }

    public void setGetReferenceData(boolean getReferenceData) {
        this.getReferenceData = getReferenceData;
    }

    @Override
    public String getType() {
        return "jsonb";
    }
}

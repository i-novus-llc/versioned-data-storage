package ru.i_novus.platform.versioned_data_storage.api.model;

import java.util.List;

/**
 * @author lgalimova
 * @since 20.03.2018
 */
public class CompareData {

    private List<FieldValue> created;

    private List<FieldValue> updated;

    private List<FieldValue> deleted;
}

package ru.i_novus.platform.datastorage.temporal.model;

import java.util.List;

/**
 * @author lgalimova
 * @since 20.03.2018
 */
public class DataDifference {

    private List<FieldValue> created;

    private List<FieldValue> updated;

    private List<FieldValue> deleted;
}

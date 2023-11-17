package ru.i_novus.platform.datastorage.temporal.model;

import ru.i_novus.platform.datastorage.temporal.model.criteria.DataPage;
import ru.i_novus.platform.datastorage.temporal.model.value.DiffRowValue;

/**
 * @author lgalimova
 * @since 20.03.2018
 */
public class DataDifference {

    private final DataPage<DiffRowValue> rows;

    public DataDifference(DataPage<DiffRowValue> rows) {
        this.rows = rows;
    }

    public DataPage<DiffRowValue> getRows() {
        return rows;
    }
}

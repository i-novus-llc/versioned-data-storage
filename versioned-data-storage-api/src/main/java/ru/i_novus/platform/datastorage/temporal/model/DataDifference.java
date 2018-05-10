package ru.i_novus.platform.datastorage.temporal.model;

import java.util.List;

/**
 * @author lgalimova
 * @since 20.03.2018
 */
public class DataDifference {
    private List<DiffRowValue> rows;

    public DataDifference(List<DiffRowValue> rows) {
        this.rows = rows;
    }

    public List<DiffRowValue> getRows() {
        return rows;
    }
}

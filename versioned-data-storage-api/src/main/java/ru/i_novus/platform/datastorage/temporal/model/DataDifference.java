package ru.i_novus.platform.datastorage.temporal.model;

import net.n2oapp.criteria.api.CollectionPage;
import ru.i_novus.platform.datastorage.temporal.model.value.DiffRowValue;

/**
 * @author lgalimova
 * @since 20.03.2018
 */
public class DataDifference {

    private final CollectionPage<DiffRowValue> rows;

    public DataDifference(CollectionPage<DiffRowValue> rows) {
        this.rows = rows;
    }

    public CollectionPage<DiffRowValue> getRows() {
        return rows;
    }
}

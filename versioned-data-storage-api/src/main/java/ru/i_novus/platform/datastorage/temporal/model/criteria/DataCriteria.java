package ru.i_novus.platform.datastorage.temporal.model.criteria;

import net.n2oapp.criteria.api.Criteria;

/** Критерий поиска данных. */
public class DataCriteria extends Criteria {

    // Нумерация страниц с 1.
    public static final int MIN_PAGE = 1;
    public static final int MIN_SIZE = 1;
    public static final int NO_PAGINATION_PAGE = 0;
    public static final int NO_PAGINATION_SIZE = 0;

    public DataCriteria() {
    }

    public DataCriteria(DataCriteria criteria) {
        super(criteria);
    }

    public int getOffset() {

        if (getPage() < MIN_PAGE || getSize() < MIN_SIZE)
            throw new IllegalStateException("Criteria page and size should be greater than zero");

        return (getPage() - 1) * getSize();
    }
}

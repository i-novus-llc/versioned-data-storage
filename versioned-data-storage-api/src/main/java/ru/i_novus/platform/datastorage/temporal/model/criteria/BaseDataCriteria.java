package ru.i_novus.platform.datastorage.temporal.model.criteria;

import net.n2oapp.criteria.api.Criteria;

/** Базовый критерий поиска данных. */
public class BaseDataCriteria extends Criteria {

    // Нумерация страниц с 1, поэтому сдвиг = +1.
    @SuppressWarnings("unused")
    public static final int PAGE_SHIFT = 1;

    public static final int MIN_PAGE = 1;
    public static final int MIN_SIZE = 1;

    public static final int NO_PAGINATION_PAGE = 0;
    public static final int NO_PAGINATION_SIZE = 0;

    public BaseDataCriteria() {
        // Nothing to do.
    }

    public BaseDataCriteria(BaseDataCriteria criteria) {
        super(criteria);
    }

    public boolean hasPageAndSize() {

        return getPage() >= MIN_PAGE && getSize() >= MIN_SIZE;
    }

    public int getOffset() {

        if (getPage() < MIN_PAGE || getSize() < MIN_SIZE)
            throw new IllegalStateException("Criteria page and size should be greater than zero");

        return (getPage() - MIN_PAGE) * getSize();
    }

    public int getPageCount() {

        int count = (getCount() != null) ? getCount() : 0;
        if (count == 0 || getSize() < MIN_SIZE)
            throw new IllegalStateException("Criteria count and size should be specified for page count");

        return count / getSize() + 1;
    }

    @Override
    public String toString() {
        return "BaseDataCriteria{" +
                "page=" + getPage() +
                ", size=" + getSize() +
                ", count=" + getCount() +
                '}';
    }
}

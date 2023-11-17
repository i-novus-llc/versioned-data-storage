package ru.i_novus.platform.datastorage.temporal.model.criteria;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static ru.i_novus.platform.datastorage.temporal.util.CollectionUtils.isNullOrEmpty;

/**
 * Критерий поиска данных.
 */
public class DataCriteria implements Serializable {

    // Нумерация страниц с 1, поэтому сдвиг = +1.
    @SuppressWarnings("unused")
    public static final int PAGE_SHIFT = 1;

    public static final int MIN_PAGE = 1;
    public static final int MIN_SIZE = 1;
    public static final int DEFAULT_SIZE = 15;

    public static final int NO_PAGINATION_PAGE = 0;
    public static final int NO_PAGINATION_SIZE = 0;

    /** Номер страницы. */
    private int page = MIN_PAGE;

    /** Размер страницы. */
    private int size = DEFAULT_SIZE;

    /** Условия сортировки. */
    private List<DataSorting> sortings;

    /** Количество записей (если известно). */
    private Integer count;

    public DataCriteria() {
        // Nothing to do.
    }

    public DataCriteria(DataCriteria criteria) {

        this.page = criteria.page;
        this.size = criteria.size;
        this.sortings = criteria.sortings;

        this.count = criteria.count;
    }

    //<editor-fold default-state="collapsed" desc="Методы доступа">
    public List<DataSorting> getSortings() {
        return sortings;
    }

    public void setSortings(List<DataSorting> sortings) {
        this.sortings = sortings;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
    //</editor-fold> // Методы доступа

    public DataSorting getSorting() {
        return !isNullOrEmpty(sortings) ? sortings.get(0) : null;
    }

    public void addSorting(DataSorting sorting) {
        if (sortings == null) {
            sortings = new ArrayList<>();
        }
        sortings.add(0, sorting);
    }

    public boolean hasPageAndSize() {

        return page >= MIN_PAGE && size >= MIN_SIZE;
    }

    public boolean hasCount() {

        return count != null && count > 0;
    }

    public void makeUnpaged() {

        setPage(NO_PAGINATION_PAGE);
        setSize(NO_PAGINATION_SIZE);
    }

    public int getOffset() {

        return hasPageAndSize() ? (page - MIN_PAGE) * size : 0;
    }

    public int getPageCount() {

        if (hasPageAndSize() && hasCount()) {
            int result = count / size;
            return (count % size == 0) ? result : result + 1;
        }

        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DataCriteria that = (DataCriteria) o;
        return Objects.equals(page, that.page) &&
                Objects.equals(size, that.size) &&
                Objects.equals(sortings, that.sortings) &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page, size, sortings, count);
    }

    @Override
    public String toString() {
        return "DataCriteria{" +
                "page=" + page +
                ", size=" + size +
                ", sortings=" + sortings +
                ", count=" + count +
                '}';
    }
}

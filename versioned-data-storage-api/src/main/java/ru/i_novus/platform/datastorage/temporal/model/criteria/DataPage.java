package ru.i_novus.platform.datastorage.temporal.model.criteria;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;

/**
 * Страница с данными, найденными по заданному критерию.
 */
public class DataPage<T> implements Serializable {

    /**
     * Критерий поиска.
     */
    protected DataCriteria criteria;
    /**
     * Количество записей.
     */
    protected Integer count;
    /**
     * Коллекция записей.
     */
    protected Collection<T> collection;

    protected DataPage(DataCriteria criteria) {

        this.criteria = criteria;

        if (criteria.getCount() != null) {
            this.count = criteria.getCount();
        }
    }

    public DataPage(int count, Collection<T> collection, DataCriteria criteria) {

        this.criteria = criteria;
        this.count = count;
        this.collection = collection;
    }

    //<editor-fold default-state="collapsed" desc="Методы доступа">
    public int getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Collection<T> getCollection() {
        return collection;
    }

    public void setCollection(Collection<T> collection) {
        this.collection = collection;
    }

    public DataCriteria getCriteria() {
        return criteria;
    }
    //</editor-fold> // Методы доступа

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DataPage<?> dataPage = (DataPage<?>) o;
        return Objects.equals(criteria, dataPage.criteria) &&
                Objects.equals(count, dataPage.count) &&
                Objects.equals(collection, dataPage.collection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(criteria, count, collection);
    }

    @Override
    public String toString() {
        return "DataPage{" +
                "criteria=" + criteria +
                ", count=" + count +
                ", collection=" + collection +
                '}';
    }
}

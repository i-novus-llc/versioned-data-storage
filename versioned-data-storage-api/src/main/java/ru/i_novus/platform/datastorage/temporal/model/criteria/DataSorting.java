package ru.i_novus.platform.datastorage.temporal.model.criteria;

import java.io.Serializable;
import java.util.Objects;

/**
 * Условие сортировки поля.
 */
public class DataSorting implements Serializable {

    /**
     * Наименование поля.
     */
    private String field;

    /**
     * Направление сортировки.
     */
    private DataSortingDirection direction;

    public DataSorting() {
        // Nothing to do.
    }

    public DataSorting(String field, DataSortingDirection direction) {
        this.field = field;
        this.direction = direction;
    }

    //<editor-fold default-state="collapsed" desc="Методы доступа">
    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public DataSortingDirection getDirection() {
        return direction;
    }

    public void setDirection(DataSortingDirection direction) {
        this.direction = direction;
    }
    //</editor-fold> // Методы доступа

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DataSorting that = (DataSorting) o;
        return Objects.equals(field, that.field) &&
                direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, direction);
    }

    @Override
    public String toString() {
        return "DataSorting{" +
                "field='" + field + '\'' +
                ", direction=" + direction +
                '}';
    }
}

package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import static ru.i_novus.platform.datastorage.temporal.util.CollectionUtils.isNullOrEmpty;

/**
 * @author lgalimova
 * @since 10.05.2018
 */
@SuppressWarnings("rawtypes")
public class DiffRowValue implements Serializable {

    private final List<DiffFieldValue> values;
    private final DiffStatusEnum status;

    public DiffRowValue(List<DiffFieldValue> values, DiffStatusEnum status) {
        this.values = values;
        this.status = status;
    }

    public List<DiffFieldValue> getValues() {
        return values;
    }

    public DiffStatusEnum getStatus() {
        return status;
    }

    public DiffFieldValue getDiffFieldValue(String fieldName) {
        if (isNullOrEmpty(values))
            return null;

        return values.stream()
                .filter(value -> fieldName.equals(value.getField().getName()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DiffRowValue)) return false;

        DiffRowValue that = (DiffRowValue) o;
        return Objects.equals(values, that.values) &&
                Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, status);
    }

    @Override
    public String toString() {
        return "DiffRowValue{" +
                "values=" + values +
                ", status=" + status +
                '}';
    }
}

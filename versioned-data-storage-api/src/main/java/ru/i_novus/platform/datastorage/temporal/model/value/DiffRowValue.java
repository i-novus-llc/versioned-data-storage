package ru.i_novus.platform.datastorage.temporal.model.value;

import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;

import java.io.Serializable;
import java.util.List;

import static org.springframework.util.CollectionUtils.isEmpty;

/**
 * @author lgalimova
 * @since 10.05.2018
 */
public class DiffRowValue implements Serializable {
    private List<DiffFieldValue> values;
    private DiffStatusEnum status;

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
        return !isEmpty(values) ?
                values
                        .stream()
                        .filter(value -> fieldName.equals(value.getField().getName()))
                        .findFirst()
                        .orElse(null) :
                null;
    }

}

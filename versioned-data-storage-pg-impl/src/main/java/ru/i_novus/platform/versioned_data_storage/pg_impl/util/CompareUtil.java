package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.criteria.CompareDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.value.DiffFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.DiffRowValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static ru.i_novus.platform.versioned_data_storage.pg_impl.util.QueryUtil.toValueByField;

@SuppressWarnings({"unchecked","rawtypes"})
public final class CompareUtil {

    private CompareUtil() {
        // Nothing to do.
    }

    public static List<DiffRowValue> toDiffRowValues(List<Field> fields, List<Object[]> dataList,
                                                     CompareDataCriteria criteria) {
        List<DiffRowValue> result = new ArrayList<>(dataList.size());
        if (dataList.isEmpty()) {
            return result;
        }

        List<String> primaryFields = criteria.getPrimaryFields();
        for (Object[] row : dataList) {
            int i = 1; // get old/new versions data excluding sys_recordid
            List<DiffFieldValue> fieldValues = new ArrayList<>(fields.size());
            DiffStatusEnum rowStatus = null;
            for (Field field : fields) {
                DiffFieldValue fieldValue = new DiffFieldValue();
                fieldValue.setField(field);

                fieldValue.setOldValue(toValueByField(field, row[i]));
                fieldValue.setNewValue(toValueByField(field, row[row.length / 2 + i]));

                if (primaryFields.contains(field.getName())) {
                    rowStatus = diffFieldValueToStatusEnum(fieldValue, rowStatus);
                }

                fieldValues.add(fieldValue);
                i++;
            }

            for (DiffFieldValue fieldValue : fieldValues) {
                fillDiffFieldValueByStatus(fieldValue, rowStatus);
            }

            result.add(new DiffRowValue(fieldValues, rowStatus));
        }

        return result;
    }

    private static void fillDiffFieldValueByStatus(DiffFieldValue fieldValue, DiffStatusEnum rowStatus) {

        if (DiffStatusEnum.INSERTED.equals(rowStatus))
            fieldValue.setStatus(DiffStatusEnum.INSERTED);

        else if (DiffStatusEnum.DELETED.equals(rowStatus))
            fieldValue.setStatus(DiffStatusEnum.DELETED);

        else {
            Object oldValue = fieldValue.getOldValue();
            Object newValue = fieldValue.getNewValue();
            if (oldValue == null && newValue == null)
                return;

            if (!Objects.equals(oldValue, newValue)) {
                fieldValue.setStatus(DiffStatusEnum.UPDATED);
            } else {
                //if value is not changed store only new value
                fieldValue.setOldValue(null);
            }
        }
    }

    private static DiffStatusEnum diffFieldValueToStatusEnum(DiffFieldValue value, DiffStatusEnum defaultValue) {

        if (value.getOldValue() == null) {
            return DiffStatusEnum.INSERTED;
        }

        if (value.getNewValue() == null) {
            return DiffStatusEnum.DELETED;
        }

        if (value.getOldValue().equals(value.getNewValue())) {
            return DiffStatusEnum.UPDATED;
        }

        return defaultValue;
    }
}

package ru.i_novus.platform.versioned_data_storage.api.model;

import java.util.List;

public class DataStructure {
    private final List<Field> fields;
    private final List<Index> indexes;

    /**
     * @param fields список полей
     * @param indexes   список ключей
     */
    public DataStructure(List<Field> fields, List<Index> indexes) {
        this.fields = fields;
        this.indexes = indexes;
    }

    public List<Field> getFields() {
        return fields;
    }

    public List<Index> getIndexes() {
        return indexes;
    }
}

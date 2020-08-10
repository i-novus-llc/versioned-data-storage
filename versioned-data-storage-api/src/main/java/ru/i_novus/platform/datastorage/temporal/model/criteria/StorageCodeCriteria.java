package ru.i_novus.platform.datastorage.temporal.model.criteria;

import java.io.Serializable;
import java.util.Objects;

/** Критерий определения хранилища по коду. */
public class StorageCodeCriteria implements Serializable {

    /** Исходный код хранилища. */
    private final String storageCode;

    public StorageCodeCriteria(String storageCode) {

        this.storageCode = storageCode;
    }

    public String getStorageCode() {
        return storageCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StorageCodeCriteria that = (StorageCodeCriteria) o;
        return Objects.equals(storageCode, that.storageCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageCode);
    }

    @Override
    public String toString() {
        return "StorageCodeCriteria{" +
                "storageCode='" + storageCode + '\'' +
                '}';
    }
}

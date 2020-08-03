package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageCodeCriteria;
import ru.i_novus.platform.datastorage.temporal.service.StorageCodeService;

import static ru.i_novus.platform.datastorage.temporal.model.StorageConstants.DATA_SCHEMA_NAME;

public class StorageCodeServiceImpl implements StorageCodeService {

    @Override
    public String toStorageCode(StorageCodeCriteria criteria) {
        return criteria.getStorageCode();
    }

    @Override
    public String getSchemaName(StorageCodeCriteria criteria) {
        return DATA_SCHEMA_NAME;
    }
}

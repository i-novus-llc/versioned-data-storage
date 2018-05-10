package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import ru.i_novus.platform.datastorage.temporal.model.DataDifference;
import ru.i_novus.platform.datastorage.temporal.model.MetaDifference;
import ru.i_novus.platform.datastorage.temporal.service.CompareDataService;

import java.util.Date;

/**
 * @author lgalimova
 * @since 10.05.2018
 */
public class CompareDataServiceImpl implements CompareDataService {
    @Override
    public DataDifference getDataDifference(String storageCode, Date baseDataDate, Date targetDataDate) {

        return null;
    }

    @Override
    public MetaDifference getMetaDifference(String baseStorageCode, String targetStorageCode) {
        return null;
    }
}

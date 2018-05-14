package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import ru.i_novus.platform.datastorage.temporal.model.DataDifference;
import ru.i_novus.platform.datastorage.temporal.model.MetaDifference;
import ru.i_novus.platform.datastorage.temporal.model.criteria.CompareDataCriteria;
import ru.i_novus.platform.datastorage.temporal.service.CompareDataService;

/**
 * @author lgalimova
 * @since 10.05.2018
 */
public class CompareDataServiceImpl implements CompareDataService {

    private DataDao dataDao;

    @Override
    public DataDifference getDataDifference(CompareDataCriteria criteria) {
        return dataDao.getDataDifference(criteria);
    }

    @Override
    public MetaDifference getMetaDifference(String baseStorageCode, String targetStorageCode) {
        return null;
    }

    public void setDataDao(DataDao dataDao) {
        this.dataDao = dataDao;
    }
}

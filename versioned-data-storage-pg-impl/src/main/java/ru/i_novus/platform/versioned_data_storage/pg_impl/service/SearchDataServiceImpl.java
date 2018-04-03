package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import net.n2oapp.criteria.api.CollectionPage;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.RowValue;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.service.SearchDataService;

import java.math.BigInteger;
import java.util.*;

/**
 * @author lgalimova
 * @since 28.03.2018
 */
public class SearchDataServiceImpl implements SearchDataService {

    private DataDao dataDao;

    @Override
    public CollectionPage<RowValue> getPagedData(DataCriteria criteria) {
        if (criteria.getCount() == null) {
            BigInteger count = dataDao.getDataCount(criteria);
            criteria.setCount(count.intValue());
        }
        List<RowValue> data = dataDao.getData(criteria);
        return new CollectionPage<>(criteria.getCount(), data, criteria);
    }

    @Override
    public List<RowValue> getData(DataCriteria criteria) {
        criteria.setPage(0);
        criteria.setSize(0);
        return dataDao.getData(criteria);
    }

    @Override
    public RowValue findRow(String storageCode, List<String> fields, Object systemId) {
        return dataDao.getRowData(storageCode, fields, systemId);
    }

    public void setDataDao(DataDao dataDao) {
        this.dataDao = dataDao;
    }
}

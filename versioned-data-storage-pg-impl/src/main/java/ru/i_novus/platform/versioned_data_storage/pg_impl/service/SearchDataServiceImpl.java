package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import ru.i_novus.platform.datastorage.temporal.model.criteria.DataPage;
import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageDataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.SearchDataService;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * @author lgalimova
 * @since 28.03.2018
 */
@SuppressWarnings({"rawtypes", "java:S3740"})
public class SearchDataServiceImpl implements SearchDataService {

    private final DataDao dataDao;

    public SearchDataServiceImpl(DataDao dataDao) {
        this.dataDao = dataDao;
    }

    @Override
    public DataPage<RowValue> getPagedData(StorageDataCriteria criteria) {

        Integer count = criteria.getCount();
        if (count == null) {
            BigInteger dataCount = dataDao.getDataCount(criteria);
            count = dataCount.intValue();
            criteria.setCount(count);
        }

        if (count == 0)
            return new DataPage<>(count, emptyList(), criteria);

        List<RowValue> data = dataDao.getData(criteria);
        return new DataPage<>(criteria.getCount(), data, criteria);
    }

    @Override
    public List<RowValue> getData(StorageDataCriteria criteria) {

        criteria.makeUnpaged();
        return dataDao.getData(criteria);
    }

    @Override
    public boolean hasData(String storageCode) {

        return dataDao.hasData(storageCode);
    }

    @Override
    public RowValue findRow(String storageCode, List<String> fields, Object systemId) {

        return dataDao.getRowData(storageCode, fields, systemId);
    }

    @Override
    public List<RowValue> findRows(String storageCode, List<String> fields, List<Object> systemIds) {

        return dataDao.getRowData(storageCode, fields, systemIds);
    }

    @Override
    public List<String> findExistentHashes(String storageCode,
                                           LocalDateTime bdate,
                                           LocalDateTime edate,
                                           List<String> hashList) {
        return dataDao.findExistentHashes(storageCode, bdate, edate, hashList);
    }
}

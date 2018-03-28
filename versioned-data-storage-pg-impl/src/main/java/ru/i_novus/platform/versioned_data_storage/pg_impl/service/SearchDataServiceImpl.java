package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import net.n2oapp.criteria.api.CollectionPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger logger = LoggerFactory.getLogger(SearchDataServiceImpl.class);
    private DataDao dataDao;

    @Override
    public CollectionPage<RowValue> getPagedData(DataCriteria criteria) {
        String tableName = criteria.getTableName();
        Date versionCreateDate = criteria.getBdate();
        if (criteria.getCount() == null) {
            BigInteger count = dataDao.getDataCount(criteria.getCommonFilter(), criteria.getFieldFilter(), tableName,
                    versionCreateDate, criteria.getEdate());
            criteria.setCount(count.intValue());
        }
        List<RowValue> data = dataDao.getData(criteria);
        return new CollectionPage<>(criteria.getCount(), data, criteria);
    }

    @Override
    public List<RowValue> getData(DataCriteria criteria) {
        return null;
    }

    @Override
    public RowValue findRow(String storageCode, String systemId) {
        return null;
    }

    public void setDataDao(DataDao dataDao) {
        this.dataDao = dataDao;
    }
}

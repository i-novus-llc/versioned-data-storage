package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import net.n2oapp.criteria.api.CollectionPage;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.SearchDataService;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static ru.i_novus.platform.datastorage.temporal.util.CollectionUtils.isNullOrEmpty;

/**
 * @author lgalimova
 * @since 28.03.2018
 */
public class SearchDataServiceImpl implements SearchDataService {

    private DataDao dataDao;

    public SearchDataServiceImpl(DataDao dataDao) {
        this.dataDao = dataDao;
    }

    @Override
    public CollectionPage<RowValue> getPagedData(DataCriteria criteria) {

        if (criteria.getCount() == null) {
            BigInteger count = dataDao.getDataCount(criteria);
            criteria.setCount(count.intValue());

            if (BigInteger.ZERO.equals(count))
                return new CollectionPage<>(criteria.getCount(), emptyList(), criteria);
        }

        List<RowValue> data = dataDao.getData(criteria);
        return new CollectionPage<>(criteria.getCount(), data, criteria);
    }

    @Override
    public List<RowValue> getData(DataCriteria criteria) {

        criteria.setPage(DataCriteria.NO_PAGINATION_PAGE);
        criteria.setSize(DataCriteria.NO_PAGINATION_SIZE);
        return dataDao.getData(criteria);
    }

    @Override
    public boolean hasData(String storageCode) {

        DataCriteria criteria = new DataCriteria(storageCode, null, null,
                emptyList(), emptySet(), null);
        criteria.setCount(1);
        criteria.setPage(DataCriteria.MIN_PAGE);
        criteria.setSize(DataCriteria.MIN_SIZE);

        Collection<RowValue> rowValues = getPagedData(criteria).getCollection();
        return !isNullOrEmpty(rowValues);
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
    public List<String> getNotExists(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<String> hashList) {
        return dataDao.getNotExists(storageCode, bdate, edate, hashList);
    }
}

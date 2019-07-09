package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import ru.i_novus.platform.datastorage.temporal.service.DropDataService;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;

import javax.transaction.Transactional;
import java.util.Set;

/**
 * @author lgalimova
 * @since 04.05.2018
 */
public class DropDataServiceImpl implements DropDataService {

    private DataDao dataDao;

    public DropDataServiceImpl(DataDao dataDao) {
        this.dataDao = dataDao;
    }

    @Override
    @Transactional
    public void drop(Set<String> storageCodes) {
        for (String storageCode : storageCodes) {
            dataDao.dropTable(storageCode);
        }
    }
}

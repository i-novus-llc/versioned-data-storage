package ru.i_novus.platform.versioned_data_storage;

import ru.i_novus.platform.datastorage.temporal.service.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;
import ru.i_novus.platform.versioned_data_storage.pg_impl.service.*;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.io.Serializable;

/**
 * @author lgalimova
 * @since 02.04.2018
 */
@ApplicationScoped
public class ServiceFactory implements Serializable {

    @Inject
    private DataDao dataDao;

    @Produces
    public StorageCodeService getStorageCodeService() {
        return new StorageCodeServiceImpl();
    }

    @Produces
    public SearchDataService getSearchDataService() {
        return new SearchDataServiceImpl(dataDao);
    }

    @Produces
    public DraftDataService getDraftDataService() {
        return new DraftDataServiceImpl(dataDao, getStorageCodeService());
    }

    @Produces
    public DropDataService getDropDataService() {
        return new DropDataServiceImpl(dataDao);
    }

    @Produces
    public CompareDataService getCompareDataService() {
        return new CompareDataServiceImpl(dataDao);
    }

    @Produces
    public FieldFactory getFieldFactory(){
        return new FieldFactoryImpl();
    }
}

package ru.i_novus.platform.versioned_data_storage;

import ru.i_novus.platform.datastorage.temporal.service.CompareDataService;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;
import ru.i_novus.platform.datastorage.temporal.service.DropDataService;
import ru.i_novus.platform.datastorage.temporal.service.SearchDataService;
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
    public SearchDataService getSearchDataService() {
        return new SearchDataServiceImpl(dataDao);
    }

    @Produces
    public DraftDataService getDraftDataService() {
        return new DraftDataServiceImpl(dataDao);
    }

    @Produces
    public DropDataService getDropDataService() {
        return new DropDataServiceImpl(dataDao);
    }

    @Produces
    public CompareDataService getCompareDataService() {
        return new CompareDataServiceImpl(dataDao);
    }
}

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
        SearchDataServiceImpl searchDataService = new SearchDataServiceImpl();
        searchDataService.setDataDao(dataDao);
        return searchDataService;
    }

    @Produces
    public DraftDataService getDraftDataService() {
        DraftDataServiceImpl draftDataService = new DraftDataServiceImpl();
        draftDataService.setDataDao(dataDao);
        return draftDataService;
    }

    @Produces
    public DropDataService getDropDataService() {
        DropDataServiceImpl service = new DropDataServiceImpl();
        service.setDataDao(dataDao);
        return service;
    }

    @Produces
    public CompareDataService getCompareDataService() {
        CompareDataServiceImpl service = new CompareDataServiceImpl();
        service.setDataDao(dataDao);
        return service;
    }
}

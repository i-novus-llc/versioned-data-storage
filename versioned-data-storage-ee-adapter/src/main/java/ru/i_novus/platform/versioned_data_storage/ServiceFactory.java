package ru.i_novus.platform.versioned_data_storage;

import ru.i_novus.platform.datastorage.temporal.service.CompareDataService;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;
import ru.i_novus.platform.datastorage.temporal.service.SearchDataService;
import ru.i_novus.platform.versioned_data_storage.pg_impl.service.DataDao;
import ru.i_novus.platform.versioned_data_storage.pg_impl.service.DraftDataServiceImpl;
import ru.i_novus.platform.versioned_data_storage.pg_impl.service.SearchDataServiceImpl;

import javax.ejb.Startup;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.SessionScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
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
}

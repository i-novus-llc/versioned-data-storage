package ru.i_novus.platform.versioned_data_storage.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.i_novus.platform.datastorage.temporal.service.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.service.*;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

/**
 * Created by tnurdinov on 25.05.2018.
 */

@Configuration
public class VersionedDataStorageConfig {

    @PersistenceContext
    private EntityManager entityManager;


    @Bean
    public DataDao dataDao() {
        return new DataDao(entityManager);
    }

    @Bean
    public SearchDataService getSearchDataService() {
        SearchDataServiceImpl searchDataService = new SearchDataServiceImpl(dataDao());
        return searchDataService;
    }

    @Bean
    public DraftDataService getDraftDataService() {
        DraftDataServiceImpl draftDataService = new DraftDataServiceImpl(dataDao());
        return draftDataService;
    }

    @Bean
    public DropDataService getDropDataService() {
        DropDataServiceImpl service = new DropDataServiceImpl(dataDao());
        return service;
    }

    @Bean
    public CompareDataService getCompareDataService() {
        CompareDataServiceImpl service = new CompareDataServiceImpl(dataDao());
        return service;
    }

    @Bean
    public FieldFactory getFieldFactory(){
        return new FieldFactoryImpl();
    }

}

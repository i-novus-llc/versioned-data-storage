package ru.i_novus.platform.versioned_data_storage.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.i_novus.platform.datastorage.temporal.service.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDaoImpl;
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
        return new DataDaoImpl(entityManager);
    }

    @Bean
    public SearchDataService getSearchDataService() {
        return new SearchDataServiceImpl(dataDao());
    }

    @Bean
    public DraftDataService getDraftDataService() {
        return new DraftDataServiceImpl(dataDao());
    }

    @Bean
    public DropDataService getDropDataService() {
        return new DropDataServiceImpl(dataDao());
    }

    @Bean
    public CompareDataService getCompareDataService() {
        return new CompareDataServiceImpl(dataDao());
    }

    @Bean
    public FieldFactory getFieldFactory(){
        return new FieldFactoryImpl();
    }

}

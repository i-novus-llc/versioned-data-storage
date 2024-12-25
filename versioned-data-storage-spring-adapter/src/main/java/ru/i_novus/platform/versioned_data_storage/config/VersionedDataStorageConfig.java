package ru.i_novus.platform.versioned_data_storage.config;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.i_novus.platform.datastorage.temporal.service.*;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDaoImpl;
import ru.i_novus.platform.versioned_data_storage.pg_impl.service.*;

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
    public SearchDataService searchDataService(DataDao dataDao) {
        return new SearchDataServiceImpl(dataDao);
    }

    @Bean
    public DraftDataService draftDataService(DataDao dataDao) {
        return new DraftDataServiceImpl(dataDao);
    }

    @Bean
    public DropDataService dropDataService(DataDao dataDao) {
        return new DropDataServiceImpl(dataDao);
    }

    @Bean
    public CompareDataService compareDataService(DataDao dataDao) {
        return new CompareDataServiceImpl(dataDao);
    }

    @Bean
    public StorageService storageService(DataDao dataDao, DraftDataService draftDataService) {
        return new StorageServiceImpl(dataDao, draftDataService);
    }

    @Bean
    public FieldFactory fieldFactory(){
        return new FieldFactoryImpl();
    }
}

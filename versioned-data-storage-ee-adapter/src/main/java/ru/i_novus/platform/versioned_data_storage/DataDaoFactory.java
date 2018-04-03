package ru.i_novus.platform.versioned_data_storage;

import ru.i_novus.platform.datastorage.temporal.service.SearchDataService;
import ru.i_novus.platform.versioned_data_storage.pg_impl.service.DataDao;
import ru.i_novus.platform.versioned_data_storage.pg_impl.service.SearchDataServiceImpl;

import javax.annotation.PostConstruct;
import javax.ejb.Startup;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.context.SessionScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.io.Serializable;

/**
 * @author lgalimova
 * @since 02.04.2018
 */
@ApplicationScoped
public class DataDaoFactory implements Serializable {

    @PersistenceContext
    private EntityManager em;

    @Produces
    public DataDao getDataDao() {
        return new DataDao(em);
    }
}

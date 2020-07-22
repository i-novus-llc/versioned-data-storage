package ru.i_novus.platform.versioned_data_storage;

import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDaoImpl;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
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
        return new DataDaoImpl(em);
    }
}

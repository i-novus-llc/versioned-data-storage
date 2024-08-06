package ru.i_novus.platform.versioned_data_storage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDaoImpl;

import java.io.Serializable;

/**
 * @author lgalimova
 * @since 02.04.2018
 */
@ApplicationScoped
public class DataDaoFactory implements Serializable {

    @PersistenceContext
    private EntityManager entityManager;

    @Produces
    public DataDao getDataDao() {
        return new DataDaoImpl(entityManager);
    }
}

package ru.i_novus.platform.versioned_data_storage;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by tnurdinov on 08.06.2018.
 */
@Configuration
@EnableTransactionManagement
public class JpaTestConfig {

    @Bean
    public DataSource dataSource() {
        EmbeddedPostgres pg = null;
        try {
            pg = EmbeddedPostgres.builder().setPort(5444)
                    .setCleanDataDirectory(true)
                    .start();
            return prepareDb(pg.getTemplateDatabase());
        } catch (IOException e) {
            try {
                pg.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
            return null;
        }

    }

    private DataSource prepareDb(DataSource dataSource){

        try (PreparedStatement stmt = dataSource.getConnection().prepareStatement(
                "CREATE SCHEMA IF NOT EXISTS data  ")) {
            stmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return dataSource;
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory() {
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setShowSql(true);
        vendorAdapter.setDatabase(Database.POSTGRESQL);
        //vendorAdapter.setGenerateDdl(true);

        LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();
        factory.setJpaVendorAdapter(vendorAdapter);
        factory.setPackagesToScan("ru.i_novus");
        factory.setDataSource(dataSource());
        //factory.setPersistenceUnitName("test");
        // factory.setPersistenceUnitManager(persistenceUnitManager());

        return factory;
    }

    @Bean
    public JpaTransactionManager transactionManager() {
        JpaTransactionManager txManager = new JpaTransactionManager();
        txManager.setEntityManagerFactory(entityManagerFactory().getObject());

        return txManager;
    }
}

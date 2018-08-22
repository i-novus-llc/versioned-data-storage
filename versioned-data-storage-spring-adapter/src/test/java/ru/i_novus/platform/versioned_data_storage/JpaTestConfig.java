package ru.i_novus.platform.versioned_data_storage;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import liquibase.integration.spring.SpringLiquibase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import ru.inovus.util.pg.embeded.PatchedPgBinaryResolver;

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
            pg = EmbeddedPostgres.builder().setPgBinaryResolver(new PatchedPgBinaryResolver()).setPort(5448)
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
                "CREATE SCHEMA IF NOT EXISTS data;  " +
                        "CREATE TEXT SEARCH DICTIONARY ispell_ru (\n" +
                        "template= ispell,\n" +
                        "dictfile= ru,\n" +
                        "afffile=ru,\n" +
                        "stopwords = russian\n" +
                        ");\n" +
                        "CREATE TEXT SEARCH CONFIGURATION ru ( COPY = russian );\n" +
                        "ALTER TEXT SEARCH CONFIGURATION ru\n" +
                        "ALTER MAPPING\n" +
                        "FOR word, hword, hword_part\n" +
                        "WITH ispell_ru, russian_stem;"
                       )) {
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

    @Bean(name = "liquibase")
    public Object springLiquibase() {
        SpringLiquibase springLiquibase = new SpringLiquibase();
        springLiquibase.setDataSource(dataSource());
        springLiquibase.setChangeLog("classpath:baseChangelog.xml");
        return springLiquibase;
    }
}

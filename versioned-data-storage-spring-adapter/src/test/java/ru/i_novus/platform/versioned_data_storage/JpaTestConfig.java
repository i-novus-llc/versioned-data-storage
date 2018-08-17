package ru.i_novus.platform.versioned_data_storage;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
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
                        "WITH ispell_ru, russian_stem;" +
                        "CREATE OR REPLACE FUNCTION data.closed_now_records(fields text, id BIGINT, tbl text, from_dt TIMESTAMP WITH TIME ZONE,\n" +
                        "                                                   to_dt  TIMESTAMP WITH TIME ZONE, tbl_seq_name text)\n" +
                        "  RETURNS SETOF RECORD AS\n" +
                        "$BODY$\n" +
                        "DECLARE left  RECORD;\n" +
                        "        right RECORD;\n" +
                        "        r     RECORD;\n" +
                        "BEGIN\n" +
                        "  IF (from_dt IS NULL)\n" +
                        "  THEN\n" +
                        "    from_dt := -infinity :: TIMESTAMP WITH TIME ZONE ;\n" +
                        "  END IF;\n" +
                        "\n" +
                        "  IF (to_dt IS NULL)\n" +
                        "  THEN\n" +
                        "    to_dt := infinity :: TIMESTAMP WITH TIME ZONE ;\n" +
                        "  END IF;\n" +
                        "\n" +
                        "  EXECUTE format(\n" +
                        "      'select %1$s from %2$s where \"SYS_RECORDID\" = %3$s',\n" +
                        "      fields, tbl, id)\n" +
                        "  INTO r;\n" +
                        "  -- строка содержится в интервале времени\n" +
                        "  IF (from_dt <= coalesce(r.\"SYS_PUBLISHTIME\", '-infinity') AND coalesce(r.\"SYS_CLOSETIME\", 'infinity') <= to_dt)\n" +
                        "  THEN\n" +
                        "    RETURN;\n" +
                        "  ELSE\n" +
                        "    --отрезаем левый конец\n" +
                        "    IF (coalesce(r.\"SYS_PUBLISHTIME\", '-infinity') < from_dt AND from_dt < coalesce(r.\"SYS_CLOSETIME\", 'infinity'))\n" +
                        "    THEN\n" +
                        "      left := r;\n" +
                        "      left.\"SYS_RECORDID\" := nextval(tbl_seq_name);\n" +
                        "      left.\"SYS_CLOSETIME\" := from_dt;\n" +
                        "      RETURN NEXT left;\n" +
                        "    END IF;\n" +
                        "    --отрезаем правый конец\n" +
                        "    IF (coalesce(r.\"SYS_PUBLISHTIME\", '-infinity') < to_dt AND to_dt < coalesce(r.\"SYS_CLOSETIME\", 'infinity'))\n" +
                        "    THEN\n" +
                        "      right := r;\n" +
                        "      right.\"SYS_RECORDID\" := nextval(tbl_seq_name);\n" +
                        "      right.\"SYS_PUBLISHTIME\" := to_dt;\n" +
                        "      RETURN NEXT right;\n" +
                        "    END IF;\n" +
                        "  END IF;\n" +
                        "\n" +
                        "END;\n" +
                        "$BODY$\n" +
                        "LANGUAGE plpgsql;")) {
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

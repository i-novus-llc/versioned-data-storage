package ru.i_novus.platform.versioned_data_storage;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageCodeCriteria;
import ru.i_novus.platform.datastorage.temporal.service.StorageCodeService;
import ru.i_novus.platform.versioned_data_storage.config.VersionedDataStorageConfig;

import static org.junit.Assert.*;
import static ru.i_novus.platform.datastorage.temporal.model.StorageConstants.DATA_SCHEMA_NAME;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {
        JpaTestConfig.class, VersionedDataStorageConfig.class
})
public class StorageCodeServiceTest {

    private static final String TEST_STORAGE_CODE = "12345678-1234-5678-90ab-cdef56789090";

    @Autowired
    private StorageCodeService storageCodeService;

    @Test
    public void testToStorageCode() {

        StorageCodeCriteria criteria = new StorageCodeCriteria(TEST_STORAGE_CODE);
        String actualCode = storageCodeService.toStorageCode(criteria);
        assertEquals(TEST_STORAGE_CODE, actualCode);
    }

    @Test
    public void testGetSchemaName() {

        StorageCodeCriteria criteria = new StorageCodeCriteria(TEST_STORAGE_CODE);
        String schemaName = storageCodeService.getSchemaName(criteria);
        assertEquals(DATA_SCHEMA_NAME, schemaName);
    }

    @Test
    public void testGenerateStorageName() {

        String firstName = storageCodeService.generateStorageName();
        assertNotNull(firstName);

        String secondName = storageCodeService.generateStorageName();
        assertNotNull(firstName);
        assertNotEquals(firstName, secondName);
    }
}

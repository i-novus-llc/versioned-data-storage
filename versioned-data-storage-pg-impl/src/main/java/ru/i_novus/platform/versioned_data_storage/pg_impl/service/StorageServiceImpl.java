package ru.i_novus.platform.versioned_data_storage.pg_impl.service;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;
import ru.i_novus.platform.datastorage.temporal.service.StorageService;
import ru.i_novus.platform.versioned_data_storage.pg_impl.dao.DataDao;

import javax.transaction.Transactional;
import java.util.List;

@SuppressWarnings({"rawtypes", "java:S3740"})
public class StorageServiceImpl implements StorageService {

    private final DataDao dataDao;

    private final DraftDataService draftDataService;

    public StorageServiceImpl(DataDao dataDao,
                              DraftDataService draftDataService) {

        this.dataDao = dataDao;
        this.draftDataService = draftDataService;
    }

    @Override
    @Transactional
    public String createStorage(List<Field> fields) {

        return createStorage(null, fields);
    }

    @Override
    @Transactional
    public String createStorage(String schemaName, List<Field> fields) {

        String draftCode = draftDataService.createDraft(schemaName, fields);

        List<String> fieldNames = dataDao.getHashUsedFieldNames(draftCode);
        if (!fieldNames.isEmpty()) {
            dataDao.dropTriggers(draftCode);
        }

        dataDao.addVersionedInformation(draftCode);

        if (!fieldNames.isEmpty()) {
            dataDao.createTriggers(draftCode, fieldNames);
        }

        return draftCode;
    }
}

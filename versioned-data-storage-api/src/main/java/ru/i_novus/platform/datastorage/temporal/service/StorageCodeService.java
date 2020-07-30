package ru.i_novus.platform.datastorage.temporal.service;

import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageCodeCriteria;

/** Сервис формирования кода хранилища. */
public interface StorageCodeService {

    String toStorageCode(StorageCodeCriteria criteria);
}

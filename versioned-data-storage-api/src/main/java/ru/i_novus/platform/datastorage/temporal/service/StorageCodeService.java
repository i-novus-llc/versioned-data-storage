package ru.i_novus.platform.datastorage.temporal.service;

import ru.i_novus.platform.datastorage.temporal.model.criteria.StorageCodeCriteria;

/** Сервис формирования кода хранилища. */
public interface StorageCodeService {

    /**
     * Определение кода хранилища по критерию.
     *
     * @param criteria критерий определения хранилища
     * @return Код хранилища
     */
    String toStorageCode(StorageCodeCriteria criteria);

    /**
     * Определение наименования схемы по критерию.
     *
     * @param criteria критерий определения хранилища
     * @return Наименование схемы
     */
    String getSchemaName(StorageCodeCriteria criteria);

    /**
     * Генерация наименования хранилища.
     *
     * @return Наименование хранилища
     */
    String generateStorageName();
}

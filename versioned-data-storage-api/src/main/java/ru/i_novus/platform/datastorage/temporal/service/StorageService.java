package ru.i_novus.platform.datastorage.temporal.service;

import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.util.List;

/**
 * Сервис работы с хранилищем версии.
 * <p>
 * Версия ({@code version}) - это фиксированный набор записей.
 * У версии не меняется ни набор данных, ни их структура.
 */
@SuppressWarnings({"rawtypes", "java:S3740"})
public interface StorageService {

    /**
     * Создание версии без данных.
     *
     * @param fields список полей
     * @return Уникальный код хранилища данных версии
     */
    String createStorage(List<Field> fields);

    /**
     * Создание версии без данных на заданной схеме.
     *
     * @param schemaName схема данных
     * @param fields     список полей
     * @return Уникальный код хранилища данных версии
     */
    String createStorage(String schemaName, List<Field> fields);
}

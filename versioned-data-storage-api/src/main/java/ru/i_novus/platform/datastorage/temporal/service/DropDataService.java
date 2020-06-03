package ru.i_novus.platform.datastorage.temporal.service;

import java.util.Set;

/**
 * Сервис удаления таблиц
 *
 * @author lgalimova
 * @since 04.05.2018
 */
public interface DropDataService {

    void drop(String storageCode);

    void drop(Set<String> storageCodes);
}

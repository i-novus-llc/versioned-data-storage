package ru.i_novus.platform.datastorage.temporal.service;

import ru.i_novus.platform.datastorage.temporal.model.DataDifference;
import ru.i_novus.platform.datastorage.temporal.model.MetaDifference;

import java.util.Date;

/**
 * @author lgalimova
 * @since 31.01.2018
 */
public interface CompareDataService {
    /**
     * Сравнение актуальных на заданные даты данных
     *
     * @param storageCode     код хранилища данных
     * @param baseDataDate    дата актуальности для исходных данных
     * @param targetDataDate  дата актуальности для целевого набора данных
     * @return Результат сравнения
     */
    DataDifference calculateDifference(String storageCode, Date baseDataDate, Date targetDataDate);

    /**
     * Сравненние структур данных в двух хранилищах
     *
     * @param baseStorageCode    код хранилища с исходной структурой данных
     * @param targetStorageCode  код хранилища с целевой структурой данных
     * @return Результат сравнения
     */
    MetaDifference calculateDifference(String baseStorageCode, String targetStorageCode);
}

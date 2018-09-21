package ru.i_novus.platform.datastorage.temporal.service;

import ru.i_novus.platform.datastorage.temporal.model.DataDifference;
import ru.i_novus.platform.datastorage.temporal.model.criteria.CompareDataCriteria;

/**
 * Сервис сравнения наборов данных и структур данных в хранилищах.
 * Набор данных внутри заданного хранилища - это набор записей, актуальных на заданную дату.
 * Структура данных в пределах одного хранилища неизменна, но может быть разной в разных хранилищах.
 *
 * @author lgalimova
 * @since 31.01.2018
 */
public interface CompareDataService {
    /**
     * Сравнение актуальных на заданные периоды дат данных
     *
     * @param criteria параметры запроса
     * @return Результат сравнения
     */
    DataDifference getDataDifferenceForPeriod(CompareDataCriteria criteria);

}

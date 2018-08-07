package ru.i_novus.platform.datastorage.temporal.service;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;

import java.util.Date;
import java.util.List;

/**
 * Сервис работы с черновиками. Черновик ({@code draft}) - это модифицируемый набор записей.
 * У черновика может изменяться как набор данных, так и их структура, вплоть до момента публикации при помощи метода {@code apply}.
 *
 * @author lgalimova
 * @since 31.01.2018
 */
public interface DraftDataService {

    /**
     * Создание черновика версии без данных
     *
     * @param fields список полей
     * @return Уникальный код черновика
     */
    String createDraft(List<Field> fields);

    /**
     * Создание новой версии на основе черновика
     *
     * @param baseStorageCode код хранилища данных
     * @param draftCode       таблица черновика
     * @param publishTime     дата и время публикации черновика
     * @return Уникальный код хранилища данных, созданного в результате слияния данных исходного хранилища и черновика
     */
    String applyDraft(String baseStorageCode, String draftCode, Date publishTime);

    /**
     * Добавление данных в черновик
     *
     * @param draftCode код черновика
     * @param data      данные строки
     */
    void addRows(String draftCode, List<RowValue> data);

    /**
     * Удалить записи из таблицы
     *
     * @param draftCode код черновика
     * @param systemIds системный идентификатор записи
     */
    void deleteRows(String draftCode, List<Object> systemIds);

    /**
     * Удалить все записи из таблицы
     *
     * @param draftCode код черновика
     */
    void deleteAllRows(String draftCode);

    /**
     * Изменение записи таблицы
     *
     * @param draftCode код черновика
     * @param data      новые значения
     */
    void updateRow(String draftCode, RowValue data);

    /**
     * Загрузить данные в черновик из хранилища
     *
     * @param draftCode         код черновика
     * @param sourceStorageCode код хранилища данных, откуда будут загружены данные
     * @param onDate            дата публикации версии
     */
    void loadData(String draftCode, String sourceStorageCode, Date onDate);

    /**
     * Добавление нового поля в таблицу
     *
     * @param draftCode код черновика
     * @param field     данные поля
     */
    void addField(String draftCode, Field field);

    /**
     * Удаления поля из таблицы
     *
     * @param draftCode код черновика
     * @param fieldName наименование удаляемого поля
     */
    void deleteField(String draftCode, String fieldName);

    void updateField(String draftCode, Field field);

    /**
     * Проверка наличия данных в поле хранилища
     *
     * @param storageCode код хранилища данных
     * @param fieldName   наименование поля
     * @return возвращает true, если в столбце есть данные, иначе false. Если столбец заполнен значениями null, считается, что он пустой.
     */
    boolean isFieldNotEmpty(String storageCode, String fieldName);

    /**
     * Проверка наличия пустых данных в поле хранилища
     *
     * @param storageCode код хранилища данных
     * @param fieldName   наименование поля
     * @return возвращает true, если в столбце есть null-значения, иначе false.
     */
    boolean isFieldContainEmptyValues(String storageCode, String fieldName);

    /**
     * Проверка уникальности значений поля хранилища
     *
     * @param storageCode код хранилища данных
     * @param fieldName   наименование поля
     * @param publishTime дата публикации версии
     * @return возврщает true, если значения поля уникальны, иначе false. Null считается уникальным значением.
     */
    boolean isFieldUnique(String storageCode, String fieldName, Date publishTime);

}

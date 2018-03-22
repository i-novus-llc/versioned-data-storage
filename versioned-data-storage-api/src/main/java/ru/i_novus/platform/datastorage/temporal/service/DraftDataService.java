package ru.i_novus.platform.datastorage.temporal.service;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.RowValue;

import java.util.Date;
import java.util.List;

/**
 * @author lgalimova
 * @since 31.01.2018
 */
public interface DraftDataService {
    /**
     * Создание черновика версии с данными
     *
     * @param fields  список полей
     * @param data    данные
     * @return уникальный код черновика
     */
    String createDraft(List<Field> fields, List<RowValue> data);

    /**
     * Создание черновика версии без данных
     *
     * @param fields  список полей
     * @return уникальный код черновика
     */
    String createDraft(List<Field> fields);

    /**
     * Создание новой версии на основе черновика
     *
     * period, range, temporal
     *
     * @param sourceStorageCode  код хранилища данных
     * @param draftCode          таблица черновика
     * @param publishTime        дата и время публикации черновика
     * @return уникальный код хранилища данных, созданного в результате слияния данных исходного хранилища и черновика
     */
    String applyDraft(String sourceStorageCode, String draftCode, Date publishTime);

    /**
     * Добавление данных в черновик
     *
     * @param draftCode код черновика
     * @param data    данные строки
     * @return системные идентификаторы добавленных записей
     */
    List<String> addRows(String draftCode, List<RowValue> data);

    /**
     * Удалить записи из таблицы
     *
     * @param draftCode код черновика
     * @param systemIds системный идентификатор записи
     */
    void deleteRows(String draftCode, List<String> systemIds);

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
     * @param systemId  системный идентификатор записи
     * @param data      новые значения
     */
    void updateRow(String draftCode, String systemId, List<FieldValue> data);

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
}

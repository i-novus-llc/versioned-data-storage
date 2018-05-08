package ru.i_novus.platform.datastorage.temporal.service;

import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.RowValue;

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
     * Создание черновика версии с данными
     *
     * @param fields список полей
     * @param data   данные
     * @return Уникальный код черновика
     */
    String createDraft(List<Field> fields, List<RowValue> data);

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
     * @return Системные идентификаторы добавленных записей
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
     * Загрузить данные в черновик из хранилища
     *
     * @param draftCode код черновика
     * @param sourceStorageCode код хранилища данных, откуда будут загружены данные
     * @param onDate дата публикации версии
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
}

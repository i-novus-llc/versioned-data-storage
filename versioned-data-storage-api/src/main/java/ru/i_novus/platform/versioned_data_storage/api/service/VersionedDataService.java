package ru.i_novus.platform.versioned_data_storage.api.service;

import net.n2oapp.criteria.api.CollectionPage;
import ru.i_novus.platform.versioned_data_storage.api.model.*;

import java.lang.String;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author lgalimova
 * @since 31.01.2018
 */
public interface VersionedDataService {

    /**
     * Получение данных постранично
     *
     * @param criteria     содержит page, size, sorting
     * @return Список записей
     */
    CollectionPage<RowValue> getPagedData(DataCriteria criteria);

    /**
     * Получение данных

     * @return Список записей
     */
    List<List<FieldValue>> getData(DataCriteria criteria);

    /**
     * Получение данных записи по системному идентификатору
     *
     * @param tableName наименование таблицы
     * @param id        системный идентификатор записи
     * @param fields    список полей в ответе
     * @return карта типа <поле, значение>
     */
    List<FieldValue> getRowBySystemId(String tableName, String id, List<Field> fields);


    /**
     * Создание черновика версии с данными
     *
     * @param fields список полей
     * @param indexes   список ключей
     * @param data   данные
     * @return наименование таблицы черновика
     */
    String createDraft(List<Field> fields, List<Index> indexes, List<FieldValue> data);

    /**
     * Создание черновика версии без данных
     *
     *
     * @param dataStructure@return наименование таблицы черновика
     */
    String createDraft(DataStructure dataStructure);

    /**
     * Сохранение данных в черновик
     *
     * @param tableName наименование таблицы
     * @param data      данные
     */
    void addDraftData(String tableName, List<FieldValue> data);

    /**
     * Создание новой версии на основе черновика
     *
     * @param actualVersionTable таблица актуальной версии
     * @param draftTable         таблица черновика
     * @param publishTime        время создания версии
     * @return наименование таблицы созданной версии
     */
    String createVersion(String actualVersionTable, String draftTable, Date publishTime);

    /**
     * Удаление таблицы
     *
     * @param tableName наименование таблицы
     */
    void dropTable(String tableName);

    /**
     * Добавление записи в таблицу
     *
     * @param tableName наименование таблицы
     * @param data      данные
     */
    void addRow(String tableName, List<FieldValue> data);

    /**
     * Удаление записи из таблицы
     *
     * @param tableName наименование таблицы
     * @param systemId  системный идентификатор записи
     */
    void deleteRow(String tableName, String systemId);

    /**
     *
     * @param tableName
     * @param date
     */
    void deleteRows(String tableName, Date date);

    /**
     * Изменение записи таблицы
     *
     * @param tableName наименование таблицы
     * @param id        системный идентификатор записи
     * @param data      новые данные
     */
    void updateRow(String tableName, String id, Map<String, Object> data);

    /**
     * Добавление нового поля в таблицу
     *
     * @param tableName наименование таблицы
     * @param field     данные поля
     */
    void addColumn(String tableName, Field field);

    /**
     * Удаления поля из таблицы
     *
     * @param tableName наименование таблицы
     * @param field     наименование удаляемого поля
     */
    void deleteColumn(String tableName, String field);

    /**
     * Сравненние данные таблиц
     *
     * @param sourceDate дата версии, которую сравнивают
     * @param targetDate дата версии, с которой сравнивают
     * @return результат сравнения
     */
    CompareData compareData(Date sourceDate, Date targetDate);

    /**
     * Сравненние структуры таблиц
     *
     * @param sourceDate дата версии, которую сравнивают
     * @param targetDate дата версии, с которой сравнивают
     * @return результат сравнения
     */
    CompareStructure compareStructure(Date sourceDate, Date targetDate);

    /**
     *
     * @param tableName
     * @param fieldName
     * @param date         дата версии
     * @return
     */
    boolean fieldIsNotEmpty(String tableName, String fieldName, Date date);

    /**
     *
     * @param tableName
     * @param fieldName
     * @param date         дата версии
     * @return
     */
    boolean fieldIsUnique(String tableName, String fieldName, Date date);

    /**
     *
     * @param tableName
     * @param fieldValue
     * @param date
     * @return
     */
    boolean fieldIsUnique(String tableName, FieldValue fieldValue, Date date);

    /**
     *
     * @param tableName
     * @param fields
     */
    void createIndex(String tableName, List<String> fields);

    /**
     *
     * @param tableName
     * @param fields
     */
    void removeIndex(String tableName, List<String> fields);

}

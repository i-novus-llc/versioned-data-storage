package ru.i_novus.platform.versioned_data_storage.api.service;

import net.n2oapp.criteria.api.CollectionPage;
import net.n2oapp.criteria.api.Criteria;
import net.n2oapp.criteria.api.Sorting;
import ru.i_novus.platform.versioned_data_storage.api.model.Field;
import ru.i_novus.platform.versioned_data_storage.api.model.FieldValue;
import ru.i_novus.platform.versioned_data_storage.api.model.RowValue;
import ru.i_novus.platform.versioned_data_storage.api.criteria.TextSearchCriteria;
import ru.i_novus.platform.versioned_data_storage.api.model.CompareData;
import ru.i_novus.platform.versioned_data_storage.api.model.Field;
import ru.i_novus.platform.versioned_data_storage.api.model.Key;

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
     * Получение данных
     *
     * @param tableName    наименование таблицы
     * @param fields       список полей в ответе
     * @param fieldFilter  фильтр по отдельным полям
     * @param commonFilter фильтр по всем полям
     * @param criteria     содержит page, size, sorting
     *
     * @return Список записей, где запись есть карта типа <поле, значение>
     */
    CollectionPage<RowValue> getPagedData(String tableName, Date beginDt, Date endDt, List<Field> fields, List<TextSearchCriteria> fieldFilter,
                                          String commonFilter, Criteria criteria);

    /**
     * Получение данных постранично
     *
     * @param tableName    наименование таблицы
     * @param fields       список полей в ответе
     * @param fieldFilter  фильтр по отдельным полям
     * @param commonFilter фильтр по всем полям
     * @param sorting      сортировка
     * @return Список записей, где запись есть карта типа <поле, значение>
     */
    List<List<FieldValue>> getData(String tableName, List<Field> fields, List<TextSearchCriteria> fieldFilter,
                                   String commonFilter, Sorting sorting);

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
     * Создание черновика версии
     *
     * @param fields            список полей
     * @param keys              список ключей
     * @param data              данные
     * @param existingTableName наименование существующей таблицы, куда будет создан черновик
     * @return наименование таблицы черновика
     */
    String createDraft(List<Field> fields, List<Key> keys, List<FieldValue> data);

    String createDraft(List<Field> fields, List<Key> keys);

    void addDraftData(List<FieldValue> data, String tableName)


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
    void addRowToTable(String tableName, List<FieldValue> data);

    /**
     * Удаление записи из таблицы
     *
     * @param tableName наименование таблицы
     * @param id        системный идентификатор записи
     */
    void deleteRowFromTable(String tableName, String systenId);

    /**
     * Изменение записи таблицы
     *
     * @param tableName наименование таблицы
     * @param id        системный идентификатор записи
     * @param data      новые данные
     */
    void updateRowFromTable(String tableName, String id, Map<String, Object> data);

    /**
     * Добавление нового поля в таблицу
     *
     * @param tableName наименование таблицы
     * @param field     данные поля
     */
    void addColumnToTable(String tableName, Field field);

    /**
     * Удаления поля из таблицы
     *
     * @param tableName наименование таблицы
     * @param field     наименование удаляемого поля
     */
    void deleteColumnFromTable(String tableName, String field);

    /**
     * Сравненние данные таблиц
     *
     * @param sourceTableName таблица, которую сравнивают
     * @param targetTableName таблица, с которой сравнивают
     * @return результат сравнения
     */
    CompareData compareData(String sourceTableName, String targetTableName);

    /**
     * Сравненние структуры таблиц
     *
     * @param sourceTableName таблица, которую сравнивают
     * @param targetTableName таблица, с которой сравнивают
     * @return результат сравнения
     */
    CompareData compareStructure(String sourceTableName, String targetTableName);
}

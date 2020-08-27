package ru.i_novus.platform.datastorage.temporal.service;

import ru.i_novus.platform.datastorage.temporal.exception.NotUniqueException;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Сервис работы с черновиками.
 * <p>
 * Черновик ({@code draft}) - это модифицируемый набор записей.
 * У черновика может изменяться как набор данных, так и их структура,
 * вплоть до момента публикации при помощи метода {@code apply}.
 *
 * @author lgalimova
 * @since 31.01.2018
 */
public interface DraftDataService {

    /**
     * Создание схемы для хранилищ данных.
     *
     * @param schemaName наименование схемы
     */
    void createSchema(String schemaName);

    /**
     * Создание черновика версии без данных.
     *
     * @param fields список полей
     * @return Уникальный код черновика
     */
    String createDraft(List<Field> fields);

    /**
     * Создание черновика версии без данных.
     *
     * @param fields список полей
     * @return Уникальный код черновика
     */
    String createDraft(String schemaName, List<Field> fields);

    /**
     * Создание новой версии на основе черновика.
     *
     * @param baseStorageCode код базового хранилища данных
     * @param draftCode       код черновика
     * @param publishTime     дата и время публикации черновика
     * @return Уникальный код хранилища данных, созданного в результате слияния данных исходного хранилища и черновика
     */
    String applyDraft(String baseStorageCode, String draftCode, LocalDateTime publishTime);

    /**
     * Создание новой версии на основе черновика с указанием даты закрытия.
     *
     * @param baseStorageCode код базового хранилища данных
     * @param draftCode       код черновика
     * @param publishTime     дата и время публикации черновика
     * @param closeTime       дата и время завершения действия версии
     * @return Уникальный код хранилища данных, созданного в результате слияния данных исходного хранилища и черновика
     */
    String applyDraft(String baseStorageCode, String draftCode,
                      LocalDateTime publishTime, LocalDateTime closeTime);

    /**
     * Проверка существования схемы.
     *
     * @param schemaName наименование схемы
     * @return Признак существования
     */
    boolean schemaExists(String schemaName);

    /**
     * Проверка существования хранилища.
     *
     * @param storageCode код хранилища данных
     * @return Признак существования
     */
    boolean storageExists(String storageCode);

    /**
     * Добавление записей в таблицу.
     *
     * @param draftCode код черновика
     * @param rowValues данные записей
     * @throws NotUniqueException если строки дублируются (нарушение уникальности SYS_HASH)
     */
    void addRows(String draftCode, List<RowValue> rowValues);

    /**
     * Изменение записей таблицы.
     *
     * @param draftCode код черновика
     * @param rowValues новые данные записей
     */
    void updateRows(String draftCode, List<RowValue> rowValues);

    /**
     * Удаление записей из таблицы.
     *
     * @param draftCode код черновика
     * @param systemIds системные идентификаторы записей
     */
    void deleteRows(String draftCode, List<Object> systemIds);

    /**
     * Удаление всех записей из таблицы.
     *
     * @param draftCode код черновика
     */
    void deleteAllRows(String draftCode);

    /**
     * Загрузка данных на указанную дату из хранилища в черновик.
     *
     * @param draftCode  код черновика
     * @param sourceCode код хранилища, откуда будут загружены данные
     * @param onDate     дата публикации версии
     */
    void loadData(String draftCode, String sourceCode, LocalDateTime onDate);

    /**
     * Загрузка данных на указанном интервале из хранилища в черновик.
     *
     * @param draftCode  код черновика
     * @param sourceCode код хранилища, откуда будут загружены данные
     * @param fromDate   дата начала действия версии
     * @param toDate     дата окончания действия версии
     */
    void loadData(String draftCode, String sourceCode, LocalDateTime fromDate, LocalDateTime toDate);

    /**
     * Копирование всех записей из исходной таблицы в целевую, эквивалентную по структуре.
     * <p>
     * Проверка совпадения структур не выполняется.
     *
     * @param sourceCode код исходного хранилища данных
     * @param targetCode код целевого хранилища данных
     */
    void copyAllData(String sourceCode, String targetCode);

    /**
     * Обновление отображаемого значения ссылки в записях таблицы.
     *
     * @param storageCode код хранилища данных
     * @param fieldValue  данные поля
     * @param systemIds   системные идентификаторы записей
     */
    void updateReferenceInRows(String storageCode, ReferenceFieldValue fieldValue, List<Object> systemIds);

    /**
     * Обновление отображаемого значения ссылки во всех записях таблицы с заполненным значением ссылки.
     *
     * @param storageCode код хранилища данных
     * @param fieldValue  данные поля
     * @param publishTime дата и время публикации черновика
     * @param closeTime   дата и время завершения действия версии
     */
    void updateReferenceInRefRows(String storageCode, ReferenceFieldValue fieldValue,
                                  LocalDateTime publishTime, LocalDateTime closeTime);

    /**
     * Добавление нового поля в таблицу.
     *
     * @param draftCode код черновика
     * @param field     данные поля
     */
    void addField(String draftCode, Field field);

    /**
     * Изменение типа поля в таблице.
     *
     * @param draftCode код черновика
     * @param field     данные поля
     */
    void updateField(String draftCode, Field field);

    /**
     * Удаления поля из таблицы.
     *
     * @param draftCode код черновика
     * @param fieldName наименование удаляемого поля
     * @throws NotUniqueException
     */
    void deleteField(String draftCode, String fieldName);

    /**
     * Проверка наличия данных в поле хранилища.
     *
     * @param storageCode код хранилища данных
     * @param fieldName   наименование поля
     * @return возвращает true, если в столбце есть данные, иначе false.
     * Если столбец заполнен значениями null, считается, что он пустой.
     */
    boolean isFieldNotEmpty(String storageCode, String fieldName);

    /**
     * Проверка наличия пустых данных в поле хранилища.
     *
     * @param storageCode код хранилища данных
     * @param fieldName   наименование поля
     * @return Возвращает true, если в столбце есть null-значения, иначе false.
     */
    boolean isFieldContainEmptyValues(String storageCode, String fieldName);

    /**
     * Проверка уникальности значений поля хранилища.
     *
     * @param storageCode код хранилища данных
     * @param fieldName   наименование поля
     * @param publishTime дата публикации версии
     * @return Возвращает true, если значения поля уникальны, иначе false. Null считается уникальным значением.
     */
    boolean isFieldUnique(String storageCode, String fieldName, LocalDateTime publishTime);

    /**
     * Проверка уникальности списка значений полей хранилища.
     *
     * @param storageCode код хранилища данных
     * @param fieldNames  список наименований полей
     * @return Возвращает true, если значения полей уникальны, иначе false. Null считается уникальным значением.
     */
    boolean isUnique(String storageCode, List<String> fieldNames);
}

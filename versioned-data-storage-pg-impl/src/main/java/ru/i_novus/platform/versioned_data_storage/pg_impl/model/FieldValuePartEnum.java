package ru.i_novus.platform.versioned_data_storage.pg_impl.model;

/**
 * Часть значения поля.
 * <p/>
 * Используется для уточнения частей составных полей (типа json и т.п.),
 * возвращаемых запросом SELECT и используемых для заполнения соответствующих объектов.
 */
public enum FieldValuePartEnum {
    REFERENCE_HASH,             // Хеш записи, на который ведёт ссылка
    REFERENCE_DISPLAY_VALUE,    // Отображаемое значение ссылки
}

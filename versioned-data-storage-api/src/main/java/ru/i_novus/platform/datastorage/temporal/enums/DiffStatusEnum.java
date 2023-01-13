package ru.i_novus.platform.datastorage.temporal.enums;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lgalimova
 * @since 10.05.2018
 */
public enum DiffStatusEnum {

    INSERTED,
    UPDATED,
    DELETED
    ;

    private static final Map<String, DiffStatusEnum> TYPE_MAP = new HashMap<>();
    static {
        for (DiffStatusEnum type : DiffStatusEnum.values()) {
            TYPE_MAP.put(type.name(), type);
        }
    }

    /**
     * Получение типа по строковому значению типа.
     * Обычный {@link DiffStatusEnum#valueOf} не подходит, т.к. кидает исключение.
     *
     * @param value Строковое значение
     * @return Тип справочника
     */
    public static DiffStatusEnum fromValue(String value) {

        return value != null ? TYPE_MAP.get(value) : null;
    }
}

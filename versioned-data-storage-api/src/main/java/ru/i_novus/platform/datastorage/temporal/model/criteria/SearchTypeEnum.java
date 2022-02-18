package ru.i_novus.platform.datastorage.temporal.model.criteria;

import java.util.HashMap;
import java.util.Map;

/**
 * Тип поиска значений поля.
 *
 * @author lgalimova
 * @since 21.03.2017
 */
public enum SearchTypeEnum {

    // Сравнение значений:
    EXACT,          // на совпадение или на вхождение в список

    // Сравнение по вхождению подстроки:
    LIKE,           // обычное

    // Специальное сравнение:
    IS_NULL,        // на совпадение с null
    IS_NOT_NULL,    // на несовпадение с null

    // Сравнение для ссылок:
    REFERENCE,      // по значению ключа записи, на который ведёт ссылка
                    // (иначе - по отображаемому значению ссылки)

    // Сравнение для дерева:
    MORE,           // поиск по всем потомкам
    LESS            // поиск по всем предкам

    ;

    private static final Map<String, SearchTypeEnum> TYPE_MAP = new HashMap<>();
    static {
        for (SearchTypeEnum type : SearchTypeEnum.values()) {
           TYPE_MAP.put(type.name(), type);
        }
    }

    public static SearchTypeEnum fromValue(String value) {

        return value != null ? TYPE_MAP.get(value) : SearchTypeEnum.EXACT;
    }
}

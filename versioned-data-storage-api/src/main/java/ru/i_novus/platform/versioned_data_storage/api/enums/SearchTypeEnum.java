package ru.i_novus.platform.versioned_data_storage.api.enums;

/**
 * @author lgalimova
 * @since 21.03.2017
 */
public enum SearchTypeEnum {
    EXACT, LIKE;

    public static SearchTypeEnum getByText(String value) {
        for (SearchTypeEnum searchTypeEnum : values()) {
            if (searchTypeEnum.name().equals(value))
                return searchTypeEnum;
        }
        return null;
    }
}

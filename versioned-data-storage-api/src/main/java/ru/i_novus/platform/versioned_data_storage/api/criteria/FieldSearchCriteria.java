package ru.i_novus.platform.versioned_data_storage.api.criteria;

import ru.i_novus.platform.versioned_data_storage.api.enums.SearchTypeEnum;
import ru.i_novus.platform.versioned_data_storage.api.model.FieldValue;


/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class FieldSearchCriteria {
    private FieldValue value;
    private SearchTypeEnum type = SearchTypeEnum.EXACT;
}

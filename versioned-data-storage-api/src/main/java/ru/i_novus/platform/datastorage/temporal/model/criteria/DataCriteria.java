package ru.i_novus.platform.datastorage.temporal.model.criteria;

import net.n2oapp.criteria.api.Criteria;
import ru.i_novus.platform.datastorage.temporal.model.Field;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

// add StorageDataCriteria with storageCode, fields, fieldFilter and statics
public class DataCriteria extends Criteria {

    public static final int MIN_PAGE = 1;
    public static final int MIN_SIZE = 1;
    public static final int NO_PAGINATION_PAGE = 0;
    public static final int NO_PAGINATION_SIZE = 0;

    private final String tableName;
    private final LocalDateTime bdate;
    private final LocalDateTime edate;
    private final List<Field> fields;
    private Set<List<FieldSearchCriteria>> fieldFilter;
    private List<Long> systemIds;
    private String commonFilter;
    private List<String> hashList;

    /**
     * @param storageCode  наименование таблицы
     * @param bdate        дата публикации версии
     * @param edate        дата создания версии
     * @param fields       список полей в ответе
     * @param fieldFilter  фильтр по отдельным полям
     * @param commonFilter фильтр по всем полям
     */
    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                        List<FieldSearchCriteria> fieldFilter, String commonFilter) {
        this(storageCode, bdate, edate, fields,
                new HashSet<List<FieldSearchCriteria>>() {{
                    add(fieldFilter);
                }},
                commonFilter);
    }

    /**
     * @param storageCode   наименование таблицы
     * @param bdate         дата публикации версии
     * @param edate         дата создания версии
     * @param fields        список полей в ответе
     * @param hashList      хеши записей
     */
    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                        List<String> hashList) {
        this.tableName = storageCode;
        this.bdate = bdate;
        this.edate = edate;
        this.fields = fields;
        this.hashList = hashList;
    }

    /**
     * @param storageCode   наименование таблицы
     * @param bdate         дата публикации версии
     * @param edate         дата создания версии
     * @param fields        список полей в ответе
     * @param fieldFilter множество фильтров по отдельным полям
     * @param commonFilter  фильтр по всем полям
     */
    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                        Set<List<FieldSearchCriteria>> fieldFilter, String commonFilter) {
        this.tableName = storageCode;
        this.bdate = bdate;
        this.edate = edate;
        this.fields = fields;
        this.fieldFilter = fieldFilter;
        this.commonFilter = commonFilter;
    }

    /**
     * @param storageCode   наименование таблицы
     * @param bdate         дата публикации версии
     * @param edate         дата создания версии
     * @param fields        список полей в ответе
     * @param fieldFilter   множество фильтров по отдельным полям
     * @param systemIds     множество фильтров по систменым идентификаторам строк
     * @param commonFilter  фильтр по всем полям
     */
    public DataCriteria(String storageCode, LocalDateTime bdate, LocalDateTime edate, List<Field> fields,
                        Set<List<FieldSearchCriteria>> fieldFilter, List<Long> systemIds, String commonFilter) {
        this(storageCode, bdate, edate, fields, fieldFilter, commonFilter);
        this.systemIds = systemIds;
    }

    public String getTableName() {
        return tableName;
    }

    public LocalDateTime getBdate() {
        return bdate;
    }

    public LocalDateTime getEdate() {
        return edate;
    }

    public List<Field> getFields() {
        return fields;
    }

    public Set<List<FieldSearchCriteria>> getFieldFilter() {
        return fieldFilter;
    }

    public List<Long> getSystemIds() {
        return systemIds;
    }

    public String getCommonFilter() {
        return commonFilter;
    }

    public List<String> getHashList() {
        return hashList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataCriteria criteria = (DataCriteria) o;

        if (!Objects.equals(tableName, criteria.tableName)) return false;
        if (!Objects.equals(bdate, criteria.bdate)) return false;
        if (!Objects.equals(edate, criteria.edate)) return false;
        if (!Objects.equals(fields, criteria.fields)) return false;
        if (!Objects.equals(fieldFilter, criteria.fieldFilter)) return false;
        if (!Objects.equals(systemIds, criteria.systemIds)) return false;
        if (!Objects.equals(commonFilter, criteria.commonFilter)) return false;
        return Objects.equals(hashList, criteria.hashList);
    }

    @Override
    public int hashCode() {
        int result = tableName != null ? tableName.hashCode() : 0;
        result = 31 * result + (bdate != null ? bdate.hashCode() : 0);
        result = 31 * result + (edate != null ? edate.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        result = 31 * result + (fieldFilter != null ? fieldFilter.hashCode() : 0);
        result = 31 * result + (systemIds != null ? systemIds.hashCode() : 0);
        result = 31 * result + (commonFilter != null ? commonFilter.hashCode() : 0);
        result = 31 * result + (hashList != null ? hashList.hashCode() : 0);
        return result;
    }
}

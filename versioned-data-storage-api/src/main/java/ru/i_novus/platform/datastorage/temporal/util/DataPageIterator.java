package ru.i_novus.platform.datastorage.temporal.util;

import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataPage;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Итератор для постраничной обработки данных, получаемых из БД.
 *
 * @param <T> тип получаемых данных
 * @param <C> тип критерия поиска данных
 */
public class DataPageIterator<T, C extends DataCriteria> implements Iterator<DataPage<? extends T>> {

    /** Источник данных. */
    private final Function<? super C, DataPage<? extends T>> pageSource;

    /** Критерий выборки. */
    private final C criteria;

    /** Номер текущей страницы. */
    private int currentPage;

    /** Следующая страница с данными. */
    private DataPage<? extends T> nextPage;

    public DataPageIterator(Function<? super C, DataPage<? extends T>> pageSource, C criteria) {

        this.pageSource = pageSource;
        this.criteria = criteria;
        this.currentPage = criteria.getPage() - 1;
    }

    @Override
    public boolean hasNext() {

        criteria.setPage(currentPage + 1);

        nextPage = pageSource.apply(criteria);
        Collection<? extends T> content = nextPage.getCollection();

        return !content.isEmpty();
    }

    @Override
    @SuppressWarnings("squid:S2272")
    public DataPage<? extends T> next() {

        DataPage<? extends T> result;

        if (nextPage != null) {
            result = nextPage;
            nextPage = null;

        } else {
            criteria.setPage(currentPage + 1);
            result = pageSource.apply(criteria);
        }
        currentPage++;

        return result;
    }
}

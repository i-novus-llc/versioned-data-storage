package ru.i_novus.platform.datastorage.temporal.util;

import net.n2oapp.criteria.api.CollectionPage;
import net.n2oapp.criteria.api.Criteria;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

public class CollectionPageIterator<T, C extends Criteria> implements Iterator<CollectionPage<? extends T>> {

    private final Function<? super C, CollectionPage<? extends T>> pageSource;

    private final C criteria;

    private int currentPage;

    private CollectionPage<? extends T> nextPage;

    public CollectionPageIterator(Function<? super C, CollectionPage<? extends T>> pageSource, C criteria) {

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
    public CollectionPage<? extends T> next() {

        CollectionPage<? extends T> result;

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

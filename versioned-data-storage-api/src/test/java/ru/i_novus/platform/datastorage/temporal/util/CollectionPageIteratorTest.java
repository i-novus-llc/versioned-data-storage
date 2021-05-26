package ru.i_novus.platform.datastorage.temporal.util;

import net.n2oapp.criteria.api.*;
import org.junit.Assert;
import org.junit.Test;
import ru.i_novus.platform.datastorage.temporal.model.criteria.BaseDataCriteria;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CollectionPageIteratorTest {

    private static final List<String> allContent = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8");

    @Test
    public void testIteration() {

        TestCriteria criteria = new TestCriteria();
        criteria.setSize(3);
        criteria.setSorting(new Sorting("id", Direction.ASC));

        CollectionPageIterator<String, TestCriteria> pageIterator = new CollectionPageIterator<>(c -> {
            int total = allContent.size();
            int offset = (c.getPage() - BaseDataCriteria.PAGE_SHIFT) * c.getSize();
            List<String> content = allContent.subList(Math.min(offset, total), Math.min(offset + c.getSize(), total));
            return new CollectionPage<>(total, content, c);
        }, criteria);

        List<List<String>> expectedPages = new ArrayList<>();
        expectedPages.add(Arrays.asList("1", "2", "3"));
        expectedPages.add(Arrays.asList("4", "5", "6"));
        expectedPages.add(Arrays.asList("7", "8"));

        for (int i = 0; i<3; i++) {
            Assert.assertTrue((i+1) + " - page number of 3 not found", pageIterator.hasNext());
            CollectionPage<? extends String> page = pageIterator.next();
            String displayContent = String.join(",", page.getCollection());
            Assert.assertTrue(displayContent + " - unexpected content", expectedPages.remove(page.getCollection()));
        }
    }

    private static class TestCriteria extends Criteria {

    }
}
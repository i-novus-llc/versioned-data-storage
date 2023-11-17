package ru.i_novus.platform.datastorage.temporal.util;

import org.junit.Assert;
import org.junit.Test;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataPage;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataSorting;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataSortingDirection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataPageIteratorTest {

    private static final List<String> allContent = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8");

    @Test
    public void testIteration() {

        final TestCriteria criteria = new TestCriteria();
        criteria.setSize(3);
        criteria.addSorting(new DataSorting("id", DataSortingDirection.ASC));

        DataPageIterator<String, TestCriteria> pageIterator = new DataPageIterator<>(c -> {
            int total = allContent.size();
            int offset = c.getOffset();
            List<String> content = allContent.subList(Math.min(offset, total), Math.min(offset + c.getSize(), total));
            return new DataPage<>(total, content, c);
        }, criteria);

        final List<List<String>> expectedPages = new ArrayList<>();
        expectedPages.add(Arrays.asList("1", "2", "3"));
        expectedPages.add(Arrays.asList("4", "5", "6"));
        expectedPages.add(Arrays.asList("7", "8"));

        for (int i = 0; i<3; i++) {
            Assert.assertTrue((i+1) + " - page number of 3 not found", pageIterator.hasNext());
            DataPage<? extends String> page = pageIterator.next();

            final String displayContent = String.join(",", page.getCollection());
            Assert.assertTrue(displayContent + " - unexpected content", expectedPages.remove(page.getCollection()));
        }
    }

    private static class TestCriteria extends DataCriteria {
    }
}
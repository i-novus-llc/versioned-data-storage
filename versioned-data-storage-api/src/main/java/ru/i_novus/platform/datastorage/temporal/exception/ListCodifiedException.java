package ru.i_novus.platform.datastorage.temporal.exception;

import ru.kirkazan.common.exception.CodifiedException;

import java.util.List;

public class ListCodifiedException extends CodifiedException {

    private List<CodifiedException> codifiedExceptions;

    public ListCodifiedException(List<CodifiedException> codifiedExceptions) {
        super(null, null);
        this.codifiedExceptions = codifiedExceptions;
    }

    public List<CodifiedException> getCodifiedExceptions() {
        return codifiedExceptions;
    }
}

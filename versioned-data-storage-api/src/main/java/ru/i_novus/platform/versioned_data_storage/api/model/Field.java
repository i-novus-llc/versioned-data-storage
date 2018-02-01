package ru.i_novus.platform.versioned_data_storage.api.model;

/**
 * @author lgalimova
 * @since 01.02.2018
 */
public abstract class Field {
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Database field type
     */
    public abstract String getType();
}

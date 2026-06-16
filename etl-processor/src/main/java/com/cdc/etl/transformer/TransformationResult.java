package com.cdc.etl.transformer;

import java.util.List;

/**
 * Holds the result of transforming raw CDC records into Star Schema rows.
 *
 * @param customers dimension rows for customers
 * @param products  dimension rows for products
 * @param dates     dimension rows for dates
 * @param facts     fact rows for orders
 */
public record TransformationResult(
        List<StarSchemaModels.CustomerRow> customers,
        List<StarSchemaModels.ProductRow> products,
        List<StarSchemaModels.DateRow> dates,
        List<StarSchemaModels.OrderFactRow> facts
) {

    /**
     * @return true if all lists are empty (no data to write)
     */
    public boolean isEmpty() {
        return customers.isEmpty() && products.isEmpty() && dates.isEmpty() && facts.isEmpty();
    }

    /**
     * @return total number of rows across all tables
     */
    public int totalRows() {
        return customers.size() + products.size() + dates.size() + facts.size();
    }
}

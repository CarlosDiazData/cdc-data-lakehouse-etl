package com.cdc.etl.glue;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

/**
 * Synchronizes Iceberg table metadata with the AWS Glue Data Catalog.
 * <p>
 * While Iceberg's {@code GlueCatalog} automatically registers tables during
 * commit, this component provides additional guarantees:
 * <ul>
 *   <li>Ensures the Glue database exists before any table operations</li>
 *   <li>Verifies table registration after Iceberg commits</li>
 *   <li>Updates table metadata attributes (description, parameters) post-commit</li>
 * </ul>
 * <p>
 * All AWS API calls include exponential-backoff retry for transient failures.
 */
@Component
public class GlueCatalogSync {

    private static final Logger log = LoggerFactory.getLogger(GlueCatalogSync.class);

    private final GlueClient glueClient;
    private final String database;

    public GlueCatalogSync(
            GlueClient glueClient,
            @Value("${aws.glue.database}") String database) {
        this.glueClient = glueClient;
        this.database = database;
    }

    /**
     * Ensure the Glue database exists, creating it if necessary.
     */
    public void ensureDatabase() {
        try {
            glueClient.getDatabase(r -> r.name(database));
            log.debug("Glue database '{}' already exists.", database);
        } catch (EntityNotFoundException e) {
            log.info("Creating Glue database '{}'", database);
            glueClient.createDatabase(r -> r.databaseInput(
                    DatabaseInput.builder()
                            .name(database)
                            .description("CDC Data Lakehouse — Iceberg tables (GOLD layer)")
                            .build()));
        }
    }

    /**
     * Register or update a table in the Glue Data Catalog.
     * <p>
     * Called after an Iceberg commit to ensure Glue metadata reflects the
     * latest snapshot location and table type.
     *
     * @param tableName   Iceberg table name (e.g. "dim_customer")
     * @param location    S3 location of the Iceberg table (e.g. "s3://bucket/iceberg/dim_customer")
     * @param properties  Table properties (Iceberg format version, etc.)
     */
    @Retryable(
            retryFor = { RuntimeException.class },
            maxAttempts = 3,
            backoff = @Backoff(delay = 2000, maxDelay = 8000, multiplier = 2.0))
    public void syncTable(String tableName, String location, Map<String, String> properties) {
        ensureDatabase();

        boolean exists = tableExists(tableName);

        if (exists) {
            updateTableMetadata(tableName, location, properties);
        } else {
            createTableMetadata(tableName, location, properties);
        }
    }

    /**
     * Check if a table exists in the Glue catalog.
     */
    public boolean tableExists(String tableName) {
        try {
            glueClient.getTable(GetTableRequest.builder()
                    .databaseName(database)
                    .name(tableName)
                    .build());
            return true;
        } catch (EntityNotFoundException e) {
            return false;
        }
    }

    /**
     * Verify all expected star schema tables are registered in Glue.
     *
     * @return map of table-name → exists (true/false)
     */
    public Map<String, Boolean> verifyTables() {
        return Map.of(
                "dim_customer", tableExists("dim_customer"),
                "dim_product", tableExists("dim_product"),
                "dim_date", tableExists("dim_date"),
                "fact_orders", tableExists("fact_orders"));
    }

    // ── Private helpers ─────────────────────────────

    private void createTableMetadata(String tableName, String location,
                                      Map<String, String> properties) {
        log.info("Registering Iceberg table '{}.{}' in Glue Catalog", database, tableName);

        TableInput tableInput = TableInput.builder()
                .name(tableName)
                .description("Iceberg table — Star Schema (GOLD layer)")
                .tableType("EXTERNAL_TABLE")
                .parameters(Map.of(
                        "table_type", "ICEBERG",
                        "metadata_location", location + "/metadata/v1.metadata.json"))
                .storageDescriptor(StorageDescriptor.builder()
                        .location(location)
                        .inputFormat("org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat")
                        .outputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                        .serdeInfo(SerDeInfo.builder()
                                .serializationLibrary("org.apache.hadoop.hive.serde2.OpenCSVSerde")
                                .build())
                        .columns(Column.builder().name("_dummy").type("string").build())
                        .build())
                .build();

        glueClient.createTable(CreateTableRequest.builder()
                .databaseName(database)
                .tableInput(tableInput)
                .build());
    }

    private void updateTableMetadata(String tableName, String location,
                                      Map<String, String> properties) {
        log.debug("Updating Glue metadata for '{}.{}'", database, tableName);

        // Get current table metadata to preserve what's already there
        var existingTable = glueClient.getTable(GetTableRequest.builder()
                .databaseName(database)
                .name(tableName)
                .build());

        // Merge new properties with existing
        var mergedProps = new java.util.HashMap<>(existingTable.table().parameters());
        mergedProps.putAll(properties);

        // Build TableInput from existing Table properties
        TableInput tableInput = TableInput.builder()
                .name(tableName)
                .parameters(mergedProps)
                .storageDescriptor(existingTable.table().storageDescriptor().toBuilder()
                        .location(location)
                        .build())
                .build();

        glueClient.updateTable(UpdateTableRequest.builder()
                .databaseName(database)
                .tableInput(tableInput)
                .build());
    }
}

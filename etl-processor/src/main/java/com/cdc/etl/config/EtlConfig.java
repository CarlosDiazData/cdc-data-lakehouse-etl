package com.cdc.etl.config;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

/**
 * Spring configuration for AWS SDK clients and Apache Iceberg catalog.
 * <p>
 * All clients use the default credential provider chain (environment variables,
 * ~/.aws/credentials, IAM instance profile). Region is configurable via
 * {@code aws.region} property (defaults to us-east-1).
 */
@Configuration
public class EtlConfig {

    @Value("${aws.region:us-east-1}")
    private String region;

    // ── AWS SDK Clients ─────────────────────────────

    @Bean
    public S3Client s3Client() {
        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    @Bean
    public GlueClient glueClient() {
        return GlueClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    @Bean
    public StsClient stsClient() {
        return StsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    // ── Apache Iceberg Catalog (Glue-backed) ────────

    @Bean
    public Catalog icebergCatalog(
            @Value("${iceberg.warehouse}") String warehousePath) {

        GlueCatalog catalog = new GlueCatalog();
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
        properties.put(CatalogProperties.CATALOG_IMPL, GlueCatalog.class.getName());
        properties.put("client.region", region);

        catalog.initialize("glue_catalog", properties);
        return catalog;
    }
}

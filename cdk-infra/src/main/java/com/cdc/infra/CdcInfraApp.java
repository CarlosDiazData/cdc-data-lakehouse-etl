package com.cdc.infra;

import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;

/**
 * CDC Lakehouse — CDK Application entry point.
 * <p>
 * Synthesizes the LakehouseStack, which provisions all AWS resources:
 * RDS PostgreSQL, DMS CDC, S3 buckets, Glue Data Catalog, Athena workgroup,
 * and IAM roles with least-privilege policies.
 * <p>
 * Usage:
 * <pre>
 * cdk synth    — generates CloudFormation template
 * cdk deploy   — provisions resources in AWS
 * cdk destroy  — tears down everything
 * </pre>
 */
public class CdcInfraApp {

    public static void main(String[] args) {
        App app = new App();

        String account = System.getenv("CDK_DEFAULT_ACCOUNT");
        String region = System.getenv().getOrDefault("CDK_DEFAULT_REGION", "us-east-1");

        Environment env = Environment.builder()
                .account(account)
                .region(region)
                .build();

        StackProps props = StackProps.builder()
                .env(env)
                .description("CDC Data Lakehouse - RDS + DMS + S3 + Glue + Athena (Free Tier)")
                .build();

        new LakehouseStack(app, "CdcLakehouseStack", props);

        app.synth();
    }
}

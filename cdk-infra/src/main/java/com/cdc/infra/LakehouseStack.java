package com.cdc.infra;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.athena.CfnWorkGroup;
import software.amazon.awscdk.services.dms.CfnEndpoint;
import software.amazon.awscdk.services.dms.CfnReplicationInstance;
import software.amazon.awscdk.services.dms.CfnReplicationTask;
import software.amazon.awscdk.services.ec2.InstanceClass;
import software.amazon.awscdk.services.ec2.InstanceSize;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.Peer;
import software.amazon.awscdk.services.ec2.Port;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.SubnetType;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.glue.CfnDatabase;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.rds.Credentials;
import software.amazon.awscdk.services.rds.DatabaseInstanceEngine;
import software.amazon.awscdk.services.rds.ParameterGroup;
import software.amazon.awscdk.services.rds.PostgresEngineVersion;
import software.amazon.awscdk.services.rds.StorageType;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.rds.DatabaseInstance;

import java.util.List;
import java.util.Map;

/**
 * CDK Stack defining all AWS resources for the CDC Data Lakehouse.
 * <p>
 * Resources:
 * <ul>
 *   <li>VPC + Security Groups (network isolation)</li>
 *   <li>RDS PostgreSQL db.t3.micro (source database)</li>
 *   <li>S3 buckets: raw (BRONZE) + lakehouse (GOLD)</li>
 *   <li>Glue Data Catalog database: lakehouse_db</li>
 *   <li>Athena workgroup: etl-workgroup</li>
 *   <li>DMS replication instance + endpoints + CDC task</li>
 *   <li>IAM roles: DMS, Glue, ETL processor (least privilege)</li>
 * </ul>
 * <p>
 * All resources target AWS Free Tier where possible.
 */
public class LakehouseStack extends Stack {

    // ── Configuration constants ───────────────────────
    private static final String DB_NAME = "ecommerce";
    private static final String DB_USER = "etl_user";
    private static final String GLUE_DB_NAME = "lakehouse_db";
    private static final String ATHENA_WORKGROUP = "etl-workgroup";

    // Resource name prefixes for consistent naming
    private final String rawBucketName;
    private final String lakehouseBucketName;

    public LakehouseStack(final software.constructs.Construct scope, final String id) {
        this(scope, id, null);
    }

    public LakehouseStack(final software.constructs.Construct scope, final String id,
                          final StackProps props) {
        super(scope, id, props);

        String stackSuffix = getStackName().toLowerCase().replaceAll("[^a-z0-9-]", "-");
        this.rawBucketName = "cdc-raw-" + stackSuffix;
        this.lakehouseBucketName = "cdc-lakehouse-" + stackSuffix;

        // ── 1. Network ────────────────────────────────
        Vpc vpc = createVpc();
        SecurityGroup rdsSecurityGroup = createRdsSecurityGroup(vpc);
        SecurityGroup dmsSecurityGroup = createDmsSecurityGroup(vpc, rdsSecurityGroup);

        // ── 2. RDS PostgreSQL ─────────────────────────
        DatabaseInstance rdsInstance = createRdsInstance(vpc, rdsSecurityGroup);

        // ── 3. S3 Buckets ─────────────────────────────
        Bucket rawBucket = createRawBucket();
        Bucket lakehouseBucket = createLakehouseBucket();

        // ── 4. Glue Data Catalog ──────────────────────
        CfnDatabase glueDatabase = createGlueDatabase();

        // ── 5. Athena Workgroup ───────────────────────
        CfnWorkGroup athenaWorkgroup = createAthenaWorkgroup(lakehouseBucket);

        // ── 6. IAM Roles ──────────────────────────────
        Role dmsRole = createDmsRole(rawBucket, lakehouseBucket);
        Role glueRole = createGlueRole(lakehouseBucket);
        Role etlProcessorRole = createEtlProcessorRole(rawBucket, lakehouseBucket);

        // ── 7. DMS ────────────────────────────────────
        createDmsResources(vpc, dmsSecurityGroup, rdsInstance, rawBucket, dmsRole);
    }

    // ═══════════════════════════════════════════════════════════
    // Network
    // ═══════════════════════════════════════════════════════════

    private Vpc createVpc() {
        return Vpc.Builder.create(this, "CdcVpc")
                .maxAzs(2)
                .natGateways(0)       // Free Tier: no NAT
                .subnetConfiguration(List.of(
                        software.amazon.awscdk.services.ec2.SubnetConfiguration.builder()
                                .name("Public")
                                .subnetType(SubnetType.PUBLIC)
                                .cidrMask(24)
                                .build()
                ))
                .build();
    }

    private SecurityGroup createRdsSecurityGroup(Vpc vpc) {
        SecurityGroup sg = SecurityGroup.Builder.create(this, "RdsSecurityGroup")
                .vpc(vpc)
                .description("RDS PostgreSQL - allows DMS and ETL processor access")
                .allowAllOutbound(false)
                .build();

        // Allow PostgreSQL from anywhere (dev/sandbox) — restrict in production
        sg.addIngressRule(Peer.anyIpv4(), Port.tcp(5432), "PostgreSQL access");
        return sg;
    }

    private SecurityGroup createDmsSecurityGroup(Vpc vpc, SecurityGroup rdsSg) {
        return SecurityGroup.Builder.create(this, "DmsSecurityGroup")
                .vpc(vpc)
                .description("DMS replication instance")
                .allowAllOutbound(true)
                .build();
    }

    // ═══════════════════════════════════════════════════════════
    // RDS PostgreSQL
    // ═══════════════════════════════════════════════════════════

    private DatabaseInstance createRdsInstance(Vpc vpc, SecurityGroup securityGroup) {
        // Parameter group: enable logical replication for DMS CDC
        ParameterGroup paramGroup = ParameterGroup.Builder.create(this, "RdsParamGroup")
                .engine(DatabaseInstanceEngine.postgres(
                        software.amazon.awscdk.services.rds.PostgresInstanceEngineProps.builder()
                                .version(PostgresEngineVersion.VER_14)
                                .build()))
                .parameters(Map.of(
                        "rds.logical_replication", "1",
                        "max_replication_slots", "5",
                        "max_wal_senders", "5",
                        "wal_keep_size", "1024"  // MB, replaces wal_keep_segments in PG 13+
                ))
                .description("CDC-enabled: logical replication for DMS")
                .build();

        DatabaseInstance instance = DatabaseInstance.Builder.create(this, "SourceDatabase")
                .engine(DatabaseInstanceEngine.postgres(
                        software.amazon.awscdk.services.rds.PostgresInstanceEngineProps.builder()
                                .version(PostgresEngineVersion.VER_14)
                                .build()))
                .instanceType(InstanceType.of(InstanceClass.BURSTABLE3, InstanceSize.MICRO))
                .vpc(vpc)
                .vpcSubnets(software.amazon.awscdk.services.ec2.SubnetSelection.builder()
                        .subnetType(SubnetType.PUBLIC)
                        .build())
                .securityGroups(List.of(securityGroup))
                .allocatedStorage(20)            // 20 GB gp2 (Free Tier)
                .storageType(StorageType.GP2)
                .databaseName(DB_NAME)
                .credentials(Credentials.fromGeneratedSecret(DB_USER))
                .parameterGroup(paramGroup)
                .publiclyAccessible(true)        // Required for DMS + local dev access
                .removalPolicy(RemovalPolicy.DESTROY)
                .deletionProtection(false)
                .build();

        // Output: RDS endpoint so consumers can discover it
        software.amazon.awscdk.CfnOutput.Builder.create(this, "RdsEndpoint")
                .description("RDS PostgreSQL endpoint")
                .value(instance.getDbInstanceEndpointAddress())
                .build();

        software.amazon.awscdk.CfnOutput.Builder.create(this, "RdsPort")
                .description("RDS PostgreSQL port")
                .value(instance.getDbInstanceEndpointPort())
                .build();

        return instance;
    }

    // ═══════════════════════════════════════════════════════════
    // S3 Buckets
    // ═══════════════════════════════════════════════════════════

    private Bucket createRawBucket() {
        Bucket bucket = Bucket.Builder.create(this, "RawBucket")
                .bucketName(rawBucketName)
                .encryption(BucketEncryption.S3_MANAGED)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .removalPolicy(RemovalPolicy.DESTROY)
                .autoDeleteObjects(true)          // Clean up on cdk destroy
                .versioned(false)                 // No versioning for raw CDC data
                .build();

        software.amazon.awscdk.CfnOutput.Builder.create(this, "RawBucketName")
                .description("S3 bucket for DMS raw Parquet output (BRONZE)")
                .value(bucket.getBucketName())
                .build();

        return bucket;
    }

    private Bucket createLakehouseBucket() {
        Bucket bucket = Bucket.Builder.create(this, "LakehouseBucket")
                .bucketName(lakehouseBucketName)
                .encryption(BucketEncryption.S3_MANAGED)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .removalPolicy(RemovalPolicy.DESTROY)
                .autoDeleteObjects(true)
                .versioned(false)
                .build();

        software.amazon.awscdk.CfnOutput.Builder.create(this, "LakehouseBucketName")
                .description("S3 bucket for Iceberg tables (GOLD)")
                .value(bucket.getBucketName())
                .build();

        return bucket;
    }

    // ═══════════════════════════════════════════════════════════
    // Glue Data Catalog
    // ═══════════════════════════════════════════════════════════

    private CfnDatabase createGlueDatabase() {
        CfnDatabase database = CfnDatabase.Builder.create(this, "LakehouseGlueDb")
                .catalogId(software.amazon.awscdk.Aws.ACCOUNT_ID)
                .databaseInput(CfnDatabase.DatabaseInputProperty.builder()
                        .name(GLUE_DB_NAME)
                        .description("CDC Data Lakehouse - Iceberg tables in GOLD zone")
                        .build())
                .build();

        software.amazon.awscdk.CfnOutput.Builder.create(this, "GlueDatabaseName")
                .description("Glue Data Catalog database")
                .value(GLUE_DB_NAME)
                .build();

        return database;
    }

    // ═══════════════════════════════════════════════════════════
    // Athena Workgroup
    // ═══════════════════════════════════════════════════════════

    private CfnWorkGroup createAthenaWorkgroup(Bucket lakehouseBucket) {
        CfnWorkGroup workgroup = CfnWorkGroup.Builder.create(this, "EtlWorkgroup")
                .name(ATHENA_WORKGROUP)
                .description("CDC Lakehouse ETL analytics workgroup")
                .recursiveDeleteOption(true)
                .workGroupConfiguration(CfnWorkGroup.WorkGroupConfigurationProperty.builder()
                        .resultConfiguration(CfnWorkGroup.ResultConfigurationProperty.builder()
                                .outputLocation("s3://" + lakehouseBucket.getBucketName() + "/athena-results/")
                                .build())
                        .enforceWorkGroupConfiguration(true)
                        .publishCloudWatchMetricsEnabled(true)
                        .build())
                .build();

        software.amazon.awscdk.CfnOutput.Builder.create(this, "AthenaWorkgroupName")
                .description("Athena workgroup for analytics queries")
                .value(ATHENA_WORKGROUP)
                .build();

        return workgroup;
    }

    // ═══════════════════════════════════════════════════════════
    // IAM Roles (Least Privilege)
    // ═══════════════════════════════════════════════════════════

    private Role createDmsRole(Bucket rawBucket, Bucket lakehouseBucket) {
        Role role = Role.Builder.create(this, "DmsRole")
                .assumedBy(new ServicePrincipal("dms.amazonaws.com"))
                .description("DMS replication instance role - read RDS, write S3 raw")
                .build();

        // Allow DMS to read from the source RDS
        role.addToPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "rds:DescribeDBInstances",
                        "rds:DescribeDBClusters",
                        "rds:DescribeDBLogFiles"
                ))
                .resources(List.of("*"))
                .build());

        // Allow DMS to write Parquet to raw bucket
        rawBucket.grantReadWrite(role);

        // DMS needs to assume this role for S3 endpoint
        role.addToPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("s3:ListBucket"))
                .resources(List.of(rawBucket.getBucketArn()))
                .build());

        return role;
    }

    private Role createGlueRole(Bucket lakehouseBucket) {
        Role role = Role.Builder.create(this, "GlueServiceRole")
                .assumedBy(new ServicePrincipal("glue.amazonaws.com"))
                .description("Glue Data Catalog role - manage lakehouse_db tables")
                .managedPolicies(List.of(
                        ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole")
                ))
                .build();

        // Glue needs access to the lakehouse bucket for Iceberg metadata
        lakehouseBucket.grantReadWrite(role);

        return role;
    }

    private Role createEtlProcessorRole(Bucket rawBucket, Bucket lakehouseBucket) {
        Role role = Role.Builder.create(this, "EtlProcessorRole")
                .assumedBy(new ServicePrincipal("ecs-tasks.amazonaws.com"))
                .description("ETL Processor role - read raw, write lakehouse, manage Glue catalog")
                .build();

        // Read-only on raw bucket
        rawBucket.grantRead(role);

        // Read-write on lakehouse bucket
        lakehouseBucket.grantReadWrite(role);

        // Glue Data Catalog: full access to lakehouse_db only
        role.addToPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "glue:GetTable", "glue:CreateTable", "glue:UpdateTable",
                        "glue:GetDatabase", "glue:GetTables", "glue:GetPartitions",
                        "glue:BatchCreatePartition", "glue:UpdatePartition"
                ))
                .resources(List.of(
                        "arn:aws:glue:" + getRegion() + ":" + getAccount() + ":catalog",
                        "arn:aws:glue:" + getRegion() + ":" + getAccount() + ":database/" + GLUE_DB_NAME,
                        "arn:aws:glue:" + getRegion() + ":" + getAccount() + ":table/" + GLUE_DB_NAME + "/*"
                ))
                .build());

        // STS: AssumeRole for cross-account (future) + caller identity
        role.addToPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("sts:GetCallerIdentity"))
                .resources(List.of("*"))
                .build());

        return role;
    }

    // ═══════════════════════════════════════════════════════════
    // DMS (Database Migration Service)
    // ═══════════════════════════════════════════════════════════

    private void createDmsResources(Vpc vpc, SecurityGroup dmsSg, DatabaseInstance rdsInstance,
                                     Bucket rawBucket, Role dmsRole) {
        // DMS Replication Instance (dms.t2.micro — Free Tier)
        CfnReplicationInstance replicationInstance = CfnReplicationInstance.Builder.create(this, "DmsReplicationInstance")
                .replicationInstanceClass("dms.t3.small")
                .allocatedStorage(50)
                .engineVersion("3.5.4")
                .publiclyAccessible(true)  // Required for S3 access without NAT gateway
                .vpcSecurityGroupIds(List.of(dmsSg.getSecurityGroupId()))
                .replicationSubnetGroupIdentifier(createDmsSubnetGroup(vpc).getRef())
                .multiAz(false)
                .autoMinorVersionUpgrade(true)
                .build();

        // Source endpoint: RDS PostgreSQL
        CfnEndpoint sourceEndpoint = createDmsSourceEndpoint(rdsInstance);
        String sourceEndpointArn = sourceEndpoint.getRef();

        // Target endpoint: S3 raw bucket (Parquet output)
        CfnEndpoint targetEndpoint = createDmsTargetEndpoint(rawBucket, dmsRole);
        String targetEndpointArn = targetEndpoint.getRef();

        // CDC Task: full-load + ongoing replication
        createDmsCdcTask(replicationInstance, sourceEndpointArn, targetEndpointArn);

        software.amazon.awscdk.CfnOutput.Builder.create(this, "DmsReplicationInstanceArn")
                .description("DMS replication instance ARN")
                .value(replicationInstance.getRef())
                .build();
    }

    private software.amazon.awscdk.services.dms.CfnReplicationSubnetGroup createDmsSubnetGroup(Vpc vpc) {
        List<String> subnetIds = vpc.getPublicSubnets().stream()
                .map(subnet -> subnet.getSubnetId())
                .toList();

        return software.amazon.awscdk.services.dms.CfnReplicationSubnetGroup.Builder.create(this, "DmsSubnetGroup")
                .replicationSubnetGroupDescription("DMS subnet group - public subnets")
                .subnetIds(subnetIds)
                .build();
    }

    private CfnEndpoint createDmsSourceEndpoint(DatabaseInstance rdsInstance) {
        return CfnEndpoint.Builder.create(this, "DmsSourceEndpoint")
                .endpointType("source")
                .engineName("postgres")
                .endpointIdentifier("cdc-source-postgres")
                .serverName(rdsInstance.getDbInstanceEndpointAddress())
                .port(5432)
                .databaseName(DB_NAME)
                .username(DB_USER)
                .password(rdsInstance.getSecret().secretValueFromJson("password").toString())
                .build();
    }

    private CfnEndpoint createDmsTargetEndpoint(Bucket rawBucket, Role dmsRole) {
        return CfnEndpoint.Builder.create(this, "DmsTargetEndpoint")
                .endpointType("target")
                .engineName("s3")
                .endpointIdentifier("cdc-target-s3")
                .s3Settings(CfnEndpoint.S3SettingsProperty.builder()
                        .bucketName(rawBucket.getBucketName())
                        .bucketFolder("dms-source")
                        .serviceAccessRoleArn(dmsRole.getRoleArn())
                        .dataFormat("parquet")
                        .parquetVersion("PARQUET_2_0")
                        .compressionType("GZIP")       // DMS does not support Snappy; GZIP is used, processor handles decompression
                        .enableStatistics(true)
                        .timestampColumnName("dms_timestamp")
                        .build())
                .build();
    }

    private CfnReplicationTask createDmsCdcTask(CfnReplicationInstance replicationInstance,
                                                  String sourceEndpointArn,
                                                  String targetEndpointArn) {
        return CfnReplicationTask.Builder.create(this, "DmsCdcTask")
                .replicationInstanceArn(replicationInstance.getRef())
                .sourceEndpointArn(sourceEndpointArn)
                .targetEndpointArn(targetEndpointArn)
                .migrationType("full-load-and-cdc")
                .tableMappings("""
                    {
                      "rules": [
                        {
                          "rule-type": "selection",
                          "rule-id": "1",
                          "rule-name": "select-all-tables",
                          "object-locator": {
                            "schema-name": "public",
                            "table-name": "%"
                          },
                          "rule-action": "include",
                          "filters": []
                        }
                      ]
                    }
                    """)
                .replicationTaskSettings("""
                    {
                      "Logging": {
                        "EnableLogging": true
                      },
                      "FullLoadSettings": {
                        "TargetTablePrepMode": "DROP_AND_CREATE",
                        "MaxFullLoadSubTasks": 8
                      },
                      "ChangeProcessingTuning": {
                        "BatchApplyMemoryLimit": 500
                      }
                    }
                    """)
                .build();
    }
}

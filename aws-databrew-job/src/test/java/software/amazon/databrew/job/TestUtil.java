package software.amazon.databrew.job;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.databrew.model.*;
import software.amazon.awssdk.services.databrew.model.ColumnSelector;
import software.amazon.awssdk.services.databrew.model.ColumnStatisticsConfiguration;
import software.amazon.awssdk.services.databrew.model.CsvOutputOptions;
import software.amazon.awssdk.services.databrew.model.DataCatalogOutput;
import software.amazon.awssdk.services.databrew.model.DatabaseOutput;
import software.amazon.awssdk.services.databrew.model.DatabaseTableOutputOptions;
import software.amazon.awssdk.services.databrew.model.JobSample;
import software.amazon.awssdk.services.databrew.model.Output;
import software.amazon.awssdk.services.databrew.model.OutputFormatOptions;
import software.amazon.awssdk.services.databrew.model.ProfileConfiguration;
import software.amazon.awssdk.services.databrew.model.S3Location;
import software.amazon.awssdk.services.databrew.model.S3TableOutputOptions;
import software.amazon.awssdk.services.databrew.model.StatisticOverride;
import software.amazon.awssdk.services.databrew.model.StatisticsConfiguration;
import software.amazon.awssdk.services.databrew.model.ValidationConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtil {

    public static final String JOB_NAME = "job-name";
    public static final String INVALID_JOB_NAME = "invalid-job-name";
    public static final String JOB_TYPE_PROFILE = "PROFILE";
    public static final String JOB_TYPE_RECIPE = "RECIPE";
    public static final String S3_BUCKET = "s3://test-dataset-input-bucket";
    public static final Integer TIMEOUT = 2880;
    public static final Integer UPDATED_TIMEOUT = 2880;
    public static final String PIPE_CSV_DELIMITER = "|";
    public static final String INVALID_CSV_DELIMITER = "*";
    public static final String VALID_BUCKET_OWNER = "123456789012";
    public static final String INVALID_BUCKET_OWNER = "000000000000";
    public static final String RULESET_ARN_1 = "arn:aws:databrew:us-east-1:123456789012:ruleset/test-ruleset-ds-1";
    public static final String RULESET_ARN_2 = "arn:aws:databrew:us-east-1:123456789012:ruleset/test-ruleset-ds-2";
    public static final String RULESET_ARN_3 = "arn:aws:databrew:us-east-1:123456789012:ruleset/test-ruleset-ds-3";
    public static final String INVALID_RULESET_ARN = "arn:aws:databrew:us-east-1:123456789012:ruleset/test-ruleset-ds-3";

    public static final S3Location S3_LOCATION = S3Location.builder()
            .bucket(S3_BUCKET)
            .build();
    public static final S3Location S3_LOCATION_VALID_BUCKET_OWNER = S3Location.builder()
            .bucket(S3_BUCKET)
            .bucketOwner(VALID_BUCKET_OWNER)
            .build();
    public static final S3Location S3_LOCATION_INVALID_BUCKET_OWNER = S3Location.builder()
            .bucket(S3_BUCKET)
            .bucketOwner(INVALID_BUCKET_OWNER)
            .build();
    public static final List<Output> OUTPUTS = ImmutableList.of(Output.builder()
            .location(S3_LOCATION).build());
    public static final List<Output> CSV_OUTPUT_VALID_DELIMITER= ImmutableList.of(Output.builder()
            .location(S3_LOCATION)
            .formatOptions(OutputFormatOptions.builder()
                    .csv(CsvOutputOptions.builder()
                            .delimiter(PIPE_CSV_DELIMITER)
                            .build())
                    .build())
            .build());
    public static final List<Output> TABLEAUHYPER_OUTPUTS = ImmutableList.of(Output.builder()
            .location(S3_LOCATION)
            .format(OutputFormat.TABLEAUHYPER)
            .build());
    public static final List<Output> CSV_OUTPUT_INVALID_DELIMITER= ImmutableList.of(Output.builder()
            .location(S3_LOCATION)
            .formatOptions(OutputFormatOptions.builder()
                    .csv(CsvOutputOptions.builder()
                            .delimiter(INVALID_CSV_DELIMITER)
                            .build())
                    .build())
            .build());
    public static final List<Output> VALID_BUCKET_OWNER_OUTPUT = ImmutableList.of(Output.builder()
            .location(S3_LOCATION_VALID_BUCKET_OWNER).build());
    public static final List<Output> INVALID_BUCKET_OWNER_OUTPUT = ImmutableList.of(Output.builder()
            .location(S3_LOCATION_INVALID_BUCKET_OWNER).build());
    public static final List<DataCatalogOutput> DATA_CATALOG_OUTPUT_LIST = ImmutableList.of(
            DataCatalogOutput.builder()
                    .databaseName("database-name")
                    .tableName("table-name")
                    .s3Options(S3TableOutputOptions.builder().location(S3_LOCATION).build())
                    .build(),
            DataCatalogOutput.builder()
                    .databaseName("database-name")
                    .tableName("table-name")
                    .databaseOptions(DatabaseTableOutputOptions.builder().tempDirectory(S3_LOCATION).tableName("table-name").build())
                    .build()
            );
    public static final List<DatabaseOutput> DATABASE_OUTPUT_LIST = ImmutableList.of(
            DatabaseOutput.builder()
                    .glueConnectionName("database-name")
                    .databaseOutputMode("NEW_TABLE")
                    .databaseOptions(DatabaseTableOutputOptions.builder().tempDirectory(S3_LOCATION).tableName("table-name").build())
                    .build()
    );
    public static final List<DataCatalogOutput> INVALID_DATA_CATALOG_OUTPUT_LIST = ImmutableList.of(
            DataCatalogOutput.builder()
                    .databaseName("database-name")
                    .tableName("table-name")
                    .s3Options(S3TableOutputOptions.builder().build())
                    .build()
    );
    public static final Map<String, String> sampleTags() {
        Map<String, String> tagMap = new HashMap<>();
        tagMap.put("test1Key", "test1Value");
        tagMap.put("test2Key", "test12Value");
        return tagMap;
    }
    public static JobSample fullDatasetModeJobSample() {
        return JobSample.builder()
                .mode(SampleMode.FULL_DATASET.toString())
                .build();
    }
    public static JobSample customRowsModeJobSample() {
        return JobSample.builder()
                .mode(SampleMode.CUSTOM_ROWS.toString())
                .size(Long.valueOf(500))
                .build();
    }
    public static JobSample defaultCustomRowsModeJobSample() {
        return JobSample.builder()
                .mode(SampleMode.CUSTOM_ROWS.toString())
                .size(Long.valueOf(20000))
                .build();
    }
    public static JobSample invalidJobSample1() {
        return JobSample.builder()
                .mode(SampleMode.CUSTOM_ROWS.toString())
                .size(Long.valueOf(Long.MAX_VALUE + 1))
                .build();
    }
    public static JobSample invalidJobSample2() {
        return JobSample.builder()
                .mode("")
                .size(Long.valueOf(Long.MAX_VALUE))
                .build();
    }
    public static JobSample invalidJobSample3() {
        return JobSample.builder()
                .mode(SampleMode.CUSTOM_ROWS.toString())
                .size(Long.valueOf(-500))
                .build();
    }
    public static JobSample invalidJobSample4() {
        return JobSample.builder()
                .mode(SampleMode.FULL_DATASET.toString())
                .size(Long.valueOf(500))
                .build();
    }

    public static final StatisticOverride COLUMN_STATISTIC_OVERRIDE = StatisticOverride.builder()
            .statistic("OUTLIER_DETECTION")
            .parameters(ImmutableMap.of(
                    "threshold", "2",
                    "sampleSize", "20"
            ))
            .build();

    public static final StatisticsConfiguration COLUMN_STATISTICS_CONFIGURATION = StatisticsConfiguration.builder()
            .includedStatistics(ImmutableList.of("OUTLIER_DETECTION"))
            .overrides(ImmutableList.of(COLUMN_STATISTIC_OVERRIDE))
            .build();

    public static final List<ColumnStatisticsConfiguration> COLUMN_STATISTICS_CONFIGURATIONS = ImmutableList.of(
            ColumnStatisticsConfiguration.builder()
                    .statistics(COLUMN_STATISTICS_CONFIGURATION)
                    .selectors(ImmutableList.of(ColumnSelector.builder()
                            .regex(".*")
                            .build()))
                    .build()
    );

    public static final StatisticOverride DATASET_STATISTIC_OVERRIDE = StatisticOverride.builder()
            .statistic("CORRELATION")
            .parameters(ImmutableMap.of(
                    "columnNumber", "2"
            ))
            .build();

    public static final StatisticsConfiguration DATASET_STATISTICS_CONFIGURATION = StatisticsConfiguration.builder()
            .includedStatistics(ImmutableList.of("CORRELATION"))
            .overrides(ImmutableList.of(DATASET_STATISTIC_OVERRIDE))
            .build();

    public  static final List<ColumnSelector> PROFILE_COLUMNS = ImmutableList.of(
            ColumnSelector.builder()
                    .regex(".*")
                    .build(),
            ColumnSelector.builder()
                    .name("columnName")
                    .build()
    );


    public static void assertThatJobModelsAreEqual(final Object rawModel,
                                                final Job sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getName()).isEqualTo(sdkModel.name());
    }

    public static List<ValidationConfiguration> createValidationConfigurations() {
        final software.amazon.awssdk.services.databrew.model.ValidationConfiguration validationConfiguration1 =
                software.amazon.awssdk.services.databrew.model.ValidationConfiguration.builder()
                .rulesetArn(RULESET_ARN_1)
                .validationMode(ValidationMode.CHECK_ALL)
                .build();
        final software.amazon.awssdk.services.databrew.model.ValidationConfiguration validationConfiguration2 =
                software.amazon.awssdk.services.databrew.model.ValidationConfiguration.builder()
                        .rulesetArn(RULESET_ARN_2)
                        .validationMode(ValidationMode.CHECK_ALL)
                        .build();

        final software.amazon.awssdk.services.databrew.model.ValidationConfiguration validationConfiguration3 =
                software.amazon.awssdk.services.databrew.model.ValidationConfiguration.builder()
                        .rulesetArn(RULESET_ARN_3)
                        .validationMode(ValidationMode.CHECK_ALL)
                        .build();
        List<software.amazon.awssdk.services.databrew.model.ValidationConfiguration> validationConfigurationList = new ArrayList<>();
        validationConfigurationList.add(validationConfiguration1);
        validationConfigurationList.add(validationConfiguration2);
        validationConfigurationList.add(validationConfiguration3);
        return validationConfigurationList;
    }

    public static List<ValidationConfiguration> createInvalidValidationConfigurations() {
        final software.amazon.awssdk.services.databrew.model.ValidationConfiguration validationConfiguration1 =
                software.amazon.awssdk.services.databrew.model.ValidationConfiguration.builder()
                        .rulesetArn(INVALID_RULESET_ARN)
                        .validationMode(ValidationMode.CHECK_ALL)
                        .build();

        List<software.amazon.awssdk.services.databrew.model.ValidationConfiguration> validationConfigurationList = new ArrayList<>();
        validationConfigurationList.add(validationConfiguration1);
        return validationConfigurationList;
    }
}

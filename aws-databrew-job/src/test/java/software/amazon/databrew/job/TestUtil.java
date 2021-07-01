package software.amazon.databrew.job;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.databrew.model.CsvOutputOptions;
import software.amazon.awssdk.services.databrew.model.Job;
import software.amazon.awssdk.services.databrew.model.OutputFormatOptions;
import software.amazon.awssdk.services.databrew.model.SampleMode;
import software.amazon.awssdk.services.databrew.model.S3Location;
import software.amazon.awssdk.services.databrew.model.JobSample;
import software.amazon.awssdk.services.databrew.model.Output;
import software.amazon.awssdk.services.databrew.model.DataCatalogOutput;
import software.amazon.awssdk.services.databrew.model.S3TableOutputOptions;
import software.amazon.awssdk.services.databrew.model.DatabaseTableOutputOptions;

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

    public static final S3Location S3_LOCATION = S3Location.builder()
            .bucket(S3_BUCKET)
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
    public static final List<Output> CSV_OUTPUT_INVALID_DELIMITER= ImmutableList.of(Output.builder()
            .location(S3_LOCATION)
            .formatOptions(OutputFormatOptions.builder()
                    .csv(CsvOutputOptions.builder()
                            .delimiter(INVALID_CSV_DELIMITER)
                            .build())
                    .build())
            .build());
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
    public static void assertThatJobModelsAreEqual(final Object rawModel,
                                                final Job sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getName()).isEqualTo(sdkModel.name());
    }
}

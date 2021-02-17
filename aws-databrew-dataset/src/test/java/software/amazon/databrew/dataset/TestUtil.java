package software.amazon.databrew.dataset;

import software.amazon.awssdk.services.databrew.model.Dataset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtil {

    public static final String DATASET_NAME = "dataset-test";
    public static final String INVALID_DATASET_NAME = "invalid-dataset-test";
    public static final String S3_BUCKET = "s3://test-dataset-input-bucket";
    public static final String S3_KEY = "input.json";
    public static final String UPDATED_S3_KEY = "input.xlsx";
    public static final String CSV_S3_KEY = "input.csv";
    public static final String TSV_S3_KEY = "input.tsv";
    public static final String PIPE_CSV_DELIMITER = "|";
    public static final String TAB_CSV_DELIMITER = "\t";
    public static final String INVALID_CSV_DELIMITER = "*";
    public static final S3Location s3InputDefinition = S3Location.builder()
            .bucket(S3_BUCKET)
            .key(S3_KEY)
            .build();
    public static final Input S3_INPUT = Input.builder()
            .s3InputDefinition(s3InputDefinition)
            .build();
    public static final S3Location updatedS3InputDefinition = S3Location.builder()
            .bucket(S3_BUCKET)
            .key(UPDATED_S3_KEY)
            .build();
    public static final Input UPDATED_S3_INPUT = Input.builder()
            .s3InputDefinition(updatedS3InputDefinition)
            .build();
    public static final S3Location csvS3InputDefinition = S3Location.builder()
            .bucket(S3_BUCKET)
            .key(CSV_S3_KEY)
            .build();
    public static final Input CSV_S3_INPUT = Input.builder()
            .s3InputDefinition(csvS3InputDefinition)
            .build();
    public static final S3Location tsvS3InputDefinition = S3Location.builder()
            .bucket(S3_BUCKET)
            .key(TSV_S3_KEY)
            .build();
    public static final Input TSV_S3_INPUT = Input.builder()
            .s3InputDefinition(tsvS3InputDefinition)
            .build();
    public static final JsonOptions JSON_OPTIONS = JsonOptions.builder()
            .multiLine(true)
            .build();
    public static final FormatOptions JSON_FORMAT_OPTIONS = FormatOptions.builder()
            .json(JSON_OPTIONS)
            .build();
    public static final ExcelOptions EXCEL_OPTIONS_INDEXES = ExcelOptions.builder()
            .sheetIndexes(new ArrayList<Integer>(){{ add(1); }})
            .build();
    public static final FormatOptions EXCEL_FORMAT_OPTIONS_INDEXES = FormatOptions.builder()
            .excel(EXCEL_OPTIONS_INDEXES)
            .build();
    public static final ExcelOptions EXCEL_OPTIONS_NAMES = ExcelOptions.builder()
            .sheetNames(new ArrayList<String>(){{ add("test"); }})
            .build();
    public static final FormatOptions EXCEL_FORMAT_OPTIONS_NAMES = FormatOptions.builder()
            .excel(EXCEL_OPTIONS_NAMES)
            .build();
    public static final CsvOptions CSV_OPTIONS = CsvOptions.builder()
            .delimiter(PIPE_CSV_DELIMITER)
            .build();
    public static final FormatOptions CSV_FORMAT_OPTIONS = FormatOptions.builder()
            .csv(CSV_OPTIONS)
            .build();
    public static final CsvOptions INVALID_CSV_OPTIONS = CsvOptions.builder()
            .delimiter(INVALID_CSV_DELIMITER)
            .build();
    public static final FormatOptions INVALID_CSV_FORMAT_OPTIONS = FormatOptions.builder()
            .csv(INVALID_CSV_OPTIONS)
            .build();
    public static final CsvOptions TSV_OPTIONS = CsvOptions.builder()
            .delimiter(TAB_CSV_DELIMITER)
            .build();
    public static final FormatOptions TSV_FORMAT_OPTIONS = FormatOptions.builder()
            .csv(TSV_OPTIONS)
            .build();
    public static final Map<String, String> sampleTags() {
        Map<String, String> tagMap = new HashMap<>();
        tagMap.put("test1Key", "test1Value");
        tagMap.put("test2Key", "test12Value");
        return tagMap;
    }
    public static void assertThatDatasetModelsAreEqual(final Object rawModel,
                                                final Dataset sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getName()).isEqualTo(sdkModel.name());
    }
}

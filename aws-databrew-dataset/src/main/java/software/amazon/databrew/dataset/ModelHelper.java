package software.amazon.databrew.dataset;

import software.amazon.awssdk.services.databrew.model.DescribeDatasetResponse;
import software.amazon.awssdk.services.databrew.model.Dataset;
import software.amazon.awssdk.services.databrew.model.ExcelOptions;
import software.amazon.awssdk.services.databrew.model.FormatOptions;
import software.amazon.awssdk.services.databrew.model.JsonOptions;
import software.amazon.awssdk.services.databrew.model.CsvOptions;
import software.amazon.awssdk.services.databrew.model.S3Location;
import software.amazon.awssdk.services.databrew.model.DataCatalogInputDefinition;
import software.amazon.awssdk.services.databrew.model.Input;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModelHelper {
    public static ResourceModel constructModel(final DescribeDatasetResponse dataset) {
        Map<String, String> tags = dataset.tags();
        return ResourceModel.builder()
                .name(dataset.name())
                .input(buildModelInput(dataset.input()))
                .formatOptions(buildModelFormatOptions(dataset.formatOptions()))
                .tags(tags != null ? buildModelTags(tags) : null)
                .build();
    }

    public static ResourceModel constructModel(final Dataset dataset) {
        Map<String, String> tags = dataset.tags();
        return ResourceModel.builder()
                .name(dataset.name())
                .input(buildModelInput(dataset.input()))
                .formatOptions(buildModelFormatOptions(dataset.formatOptions()))
                .tags(tags != null ? buildModelTags(tags) : null)
                .build();
    }

    public static S3Location buildRequestS3Location(final software.amazon.databrew.dataset.S3Location modelS3Location) {
        return modelS3Location == null ? null : S3Location.builder()
                .bucket(modelS3Location.getBucket())
                .key(modelS3Location.getKey()).build();
    }

    public static DataCatalogInputDefinition buildRequestDataCatalogInputDefinition(final software.amazon.databrew.dataset.DataCatalogInputDefinition modelDataCatalogInputDefinition) {
        return modelDataCatalogInputDefinition == null ? null : DataCatalogInputDefinition.builder()
                .catalogId(modelDataCatalogInputDefinition.getCatalogId())
                .databaseName(modelDataCatalogInputDefinition.getDatabaseName())
                .tableName(modelDataCatalogInputDefinition.getTableName())
                .tempDirectory(buildRequestS3Location(modelDataCatalogInputDefinition.getTempDirectory()))
                .build();
    }

    public static Input buildRequestInput(final software.amazon.databrew.dataset.Input modelInput) {
        if (modelInput == null) return null;
        return Input.builder()
                .s3InputDefinition(buildRequestS3Location(modelInput.getS3InputDefinition()))
                .dataCatalogInputDefinition(buildRequestDataCatalogInputDefinition(modelInput.getDataCatalogInputDefinition()))
                .build();
    }

    public static software.amazon.databrew.dataset.S3Location buildModelS3Location(final S3Location requestS3Location) {
        return requestS3Location == null ? null : software.amazon.databrew.dataset.S3Location.builder()
                .bucket(requestS3Location.bucket())
                .key(requestS3Location.key())
                .build();
    }

    public static software.amazon.databrew.dataset.DataCatalogInputDefinition buildModelDataCatalogInputDefinition(final DataCatalogInputDefinition requestDataCatalogInputDefinition) {
        return requestDataCatalogInputDefinition == null ? null : software.amazon.databrew.dataset.DataCatalogInputDefinition.builder()
                .catalogId(requestDataCatalogInputDefinition.catalogId())
                .databaseName(requestDataCatalogInputDefinition.databaseName())
                .tableName(requestDataCatalogInputDefinition.tableName())
                .tempDirectory(buildModelS3Location(requestDataCatalogInputDefinition.tempDirectory()))
                .build();
    }

    public static software.amazon.databrew.dataset.Input buildModelInput(final Input requestInput) {
        if (requestInput == null) return null;
        return software.amazon.databrew.dataset.Input.builder()
                .s3InputDefinition(buildModelS3Location(requestInput.s3InputDefinition()))
                .dataCatalogInputDefinition(buildModelDataCatalogInputDefinition(requestInput.dataCatalogInputDefinition()))
                .build();
    }

    public static FormatOptions buildRequestFormatOptions(final software.amazon.databrew.dataset.FormatOptions modelFormatOptions) {
        if (modelFormatOptions == null) return null;
        software.amazon.awssdk.services.databrew.model.FormatOptions.Builder requestFormatOptionsBuilder = software.amazon.awssdk.services.databrew.model.FormatOptions.builder();
        software.amazon.databrew.dataset.ExcelOptions modelExcelOptions = modelFormatOptions.getExcel();
        software.amazon.databrew.dataset.JsonOptions modelJsonOptions = modelFormatOptions.getJson();
        software.amazon.databrew.dataset.CsvOptions modelCsvOptions = modelFormatOptions.getCsv();
        if (modelExcelOptions != null) {
            if (modelExcelOptions.getSheetIndexes() != null) {
                software.amazon.awssdk.services.databrew.model.ExcelOptions requestExcelOptions = software.amazon.awssdk.services.databrew.model.ExcelOptions.builder()
                        .sheetIndexes(modelExcelOptions.getSheetIndexes())
                        .build();
                requestFormatOptionsBuilder
                        .excel(requestExcelOptions);
            } else if (modelExcelOptions.getSheetNames() != null) {
                ExcelOptions requestExcelOptions = ExcelOptions.builder()
                        .sheetNames(modelExcelOptions.getSheetNames())
                        .build();
                requestFormatOptionsBuilder
                        .excel(requestExcelOptions);
            }
        }
        if (modelJsonOptions != null) {
            JsonOptions requestJsonOptions = JsonOptions.builder()
                    .multiLine(modelJsonOptions.getMultiLine())
                    .build();
            requestFormatOptionsBuilder
                    .json(requestJsonOptions);
        }
        if (modelCsvOptions != null) {
            CsvOptions requestCsvOptions = CsvOptions.builder()
                    .delimiter(modelCsvOptions.getDelimiter())
                    .build();
            requestFormatOptionsBuilder
                    .csv(requestCsvOptions);
        }
        return requestFormatOptionsBuilder.build();
    }

    public static Map<String, String> buildTagInputMap(final List<Tag> tagList) {
        Map<String, String> tagMap = new HashMap<>();
        // return null if no Tag specified.
        if (tagList == null) return null;

        for (Tag tag : tagList) {
            tagMap.put(tag.getKey(), tag.getValue());
        }
        return tagMap;
    }

    public static software.amazon.databrew.dataset.FormatOptions buildModelFormatOptions(final FormatOptions requestFormatOptions) {
        if (requestFormatOptions == null) return null;
        software.amazon.databrew.dataset.FormatOptions.FormatOptionsBuilder modelFormatOptionsBuilder = new software.amazon.databrew.dataset.FormatOptions().builder();
        software.amazon.databrew.dataset.JsonOptions modelJsonOptions = new software.amazon.databrew.dataset.JsonOptions();
        software.amazon.databrew.dataset.ExcelOptions modelExcelOptions = new software.amazon.databrew.dataset.ExcelOptions();
        software.amazon.databrew.dataset.CsvOptions modelCsvOptions = new software.amazon.databrew.dataset.CsvOptions();
        if (requestFormatOptions.json() != null) {
            modelFormatOptionsBuilder
                        .json(modelJsonOptions.builder()
                                .multiLine(requestFormatOptions.json().multiLine())
                                .build());
        }
        if (requestFormatOptions.excel() != null){
            if (requestFormatOptions.excel().sheetIndexes() != null && requestFormatOptions.excel().sheetIndexes().size() >= 1) {
                modelFormatOptionsBuilder
                        .excel(modelExcelOptions.builder()
                                .sheetIndexes(requestFormatOptions.excel().sheetIndexes())
                                .build());
            }
            else {
                modelFormatOptionsBuilder.
                        excel(modelExcelOptions.builder()
                                .sheetNames(requestFormatOptions.excel().sheetNames())
                                .build());
                }
        }
        if (requestFormatOptions.csv() != null) {
            modelFormatOptionsBuilder
                    .csv(modelCsvOptions.builder()
                                .delimiter(requestFormatOptions.csv().delimiter())
                                .build());
        }
        return modelFormatOptionsBuilder.build();
    }

    public static List<Tag> buildModelTags(final Map<String, String> tags) {
        List<Tag> tagArrayList = new ArrayList<>();
        if (tags == null) return null;
        tags.forEach((k, v) -> tagArrayList.add(Tag.builder().key(k).value(v).build()));
        return tagArrayList;
    }
}

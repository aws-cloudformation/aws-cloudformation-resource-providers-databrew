package software.amazon.databrew.dataset;

import software.amazon.awssdk.services.databrew.model.CsvOptions;
import software.amazon.awssdk.services.databrew.model.DataCatalogInputDefinition;
import software.amazon.awssdk.services.databrew.model.Dataset;
import software.amazon.awssdk.services.databrew.model.DatasetParameter;
import software.amazon.awssdk.services.databrew.model.DatetimeOptions;
import software.amazon.awssdk.services.databrew.model.DescribeDatasetResponse;
import software.amazon.awssdk.services.databrew.model.ExcelOptions;
import software.amazon.awssdk.services.databrew.model.FilesLimit;
import software.amazon.awssdk.services.databrew.model.FilterExpression;
import software.amazon.awssdk.services.databrew.model.FormatOptions;
import software.amazon.awssdk.services.databrew.model.Input;
import software.amazon.awssdk.services.databrew.model.JsonOptions;
import software.amazon.awssdk.services.databrew.model.Metadata;
import software.amazon.awssdk.services.databrew.model.PathOptions;
import software.amazon.awssdk.services.databrew.model.S3Location;
import software.amazon.awssdk.services.databrew.model.DataCatalogInputDefinition;
import software.amazon.awssdk.services.databrew.model.DatabaseInputDefinition;
import software.amazon.awssdk.services.databrew.model.Input;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ModelHelper {
    public static ResourceModel constructModel(final DescribeDatasetResponse dataset) {
        Map<String, String> tags = dataset.tags();
        return ResourceModel.builder()
                .name(dataset.name())
                .format(dataset.format() != null ? dataset.format().toString() : null)
                .input(buildModelInput(dataset.input()))
                .formatOptions(buildModelFormatOptions(dataset.formatOptions()))
                .pathOptions(buildModelPathOptions(dataset.pathOptions()))
                .tags(tags != null ? buildModelTags(tags) : null)
                .build();
    }

    public static ResourceModel constructModel(final Dataset dataset) {
        Map<String, String> tags = dataset.tags();
        return ResourceModel.builder()
                .name(dataset.name())
                .format(dataset.format() != null ? dataset.format().toString() : null)
                .input(buildModelInput(dataset.input()))
                .formatOptions(buildModelFormatOptions(dataset.formatOptions()))
                .pathOptions(buildModelPathOptions(dataset.pathOptions()))
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

    public static DatabaseInputDefinition buildRequestDatabaseInputDefinition(final software.amazon.databrew.dataset.DatabaseInputDefinition modelDatabaseInputDefinition) {
        return modelDatabaseInputDefinition == null ? null : DatabaseInputDefinition.builder()
                .glueConnectionName(modelDatabaseInputDefinition.getGlueConnectionName())
                .databaseTableName(modelDatabaseInputDefinition.getDatabaseTableName())
                .tempDirectory(buildRequestS3Location(modelDatabaseInputDefinition.getTempDirectory()))
                .queryString(modelDatabaseInputDefinition.getQueryString())
                .build();
    }

    public static Metadata buildRequestMetadata(final software.amazon.databrew.dataset.Metadata modelMetadata) {
        return modelMetadata == null ? null : Metadata.builder()
                .sourceArn(modelMetadata.getSourceArn())
                .build();
    }

    public static Input buildRequestInput(final software.amazon.databrew.dataset.Input modelInput) {
        if (modelInput == null) return null;
        return Input.builder()
                .s3InputDefinition(buildRequestS3Location(modelInput.getS3InputDefinition()))
                .dataCatalogInputDefinition(buildRequestDataCatalogInputDefinition(modelInput.getDataCatalogInputDefinition()))
                .databaseInputDefinition(buildRequestDatabaseInputDefinition(modelInput.getDatabaseInputDefinition()))
                .metadata(buildRequestMetadata(modelInput.getMetadata()))
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

    public static software.amazon.databrew.dataset.DatabaseInputDefinition buildModelDatabaseInputDefinition(final DatabaseInputDefinition requestDatabaseInputDefinition) {
        return requestDatabaseInputDefinition == null ? null : software.amazon.databrew.dataset.DatabaseInputDefinition.builder()
                .glueConnectionName(requestDatabaseInputDefinition.glueConnectionName())
                .databaseTableName(requestDatabaseInputDefinition.databaseTableName())
                .tempDirectory(buildModelS3Location(requestDatabaseInputDefinition.tempDirectory()))
                .queryString(requestDatabaseInputDefinition.queryString())
                .build();
    }

    public static software.amazon.databrew.dataset.Metadata buildModelMetadata(final Metadata requestMetadata) {
        return requestMetadata == null ? null : software.amazon.databrew.dataset.Metadata.builder()
                .sourceArn(requestMetadata.sourceArn())
                .build();
    }

    public static software.amazon.databrew.dataset.Input buildModelInput(final Input requestInput) {
        if (requestInput == null) return null;
        return software.amazon.databrew.dataset.Input.builder()
                .s3InputDefinition(buildModelS3Location(requestInput.s3InputDefinition()))
                .dataCatalogInputDefinition(buildModelDataCatalogInputDefinition(requestInput.dataCatalogInputDefinition()))
                .databaseInputDefinition(buildModelDatabaseInputDefinition(requestInput.databaseInputDefinition()))
                .metadata(buildModelMetadata(requestInput.metadata()))
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
                        .headerRow(modelExcelOptions.getHeaderRow())
                        .build();
                requestFormatOptionsBuilder
                        .excel(requestExcelOptions);
            } else if (modelExcelOptions.getSheetNames() != null) {
                ExcelOptions requestExcelOptions = ExcelOptions.builder()
                        .sheetNames(modelExcelOptions.getSheetNames())
                        .headerRow(modelExcelOptions.getHeaderRow())
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
                    .headerRow(modelCsvOptions.getHeaderRow())
                    .build();
            requestFormatOptionsBuilder
                    .csv(requestCsvOptions);
        }
        return requestFormatOptionsBuilder.build();
    }

    public static <T, K, V> Map<K, V> buildMapFromList(final List<T> tagList, Function<T, K> keyProvider, Function<T, V> valueProvider) {
        Map<K, V> tagMap = new HashMap<K, V>();
        // return null if no Tag specified.
        if (tagList == null) return null;

        for (T tag : tagList) {
            tagMap.put(keyProvider.apply(tag), valueProvider.apply(tag));
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
        if (requestFormatOptions.excel() != null) {
            if (requestFormatOptions.excel().sheetIndexes() != null && requestFormatOptions.excel().sheetIndexes().size() >= 1) {
                modelFormatOptionsBuilder
                        .excel(modelExcelOptions.builder()
                                .sheetIndexes(requestFormatOptions.excel().sheetIndexes())
                                .headerRow(requestFormatOptions.excel().headerRow())
                                .build());
            } else {
                modelFormatOptionsBuilder.
                        excel(modelExcelOptions.builder()
                                .sheetNames(requestFormatOptions.excel().sheetNames())
                                .headerRow(requestFormatOptions.excel().headerRow())
                                .build());
            }
        }
        if (requestFormatOptions.csv() != null) {
            modelFormatOptionsBuilder
                    .csv(modelCsvOptions.builder()
                            .delimiter(requestFormatOptions.csv().delimiter())
                            .headerRow(requestFormatOptions.csv().headerRow())
                            .build());
        }
        return modelFormatOptionsBuilder.build();
    }

    public static software.amazon.databrew.dataset.PathOptions buildModelPathOptions(final PathOptions requestPathOptions) {
        if (requestPathOptions == null) return null;
        software.amazon.databrew.dataset.PathOptions.PathOptionsBuilder modelPathOptionsBuilder = new software.amazon.databrew.dataset.PathOptions().builder();
        software.amazon.databrew.dataset.FilesLimit.FilesLimitBuilder filesLimitBuilder = new software.amazon.databrew.dataset.FilesLimit().builder();
        FilesLimit filesLimit = requestPathOptions.filesLimit();
        if (filesLimit != null) {
            modelPathOptionsBuilder
                    .filesLimit(filesLimitBuilder
                            .maxFiles(filesLimit.maxFiles())
                            .orderedBy(filesLimit.orderedByAsString())
                            .order(filesLimit.orderAsString())
                            .build());
        }
        FilterExpression filterExpression = requestPathOptions.lastModifiedDateCondition();
        if (filterExpression != null) {
            modelPathOptionsBuilder
                    .lastModifiedDateCondition(getModelFilterExpression(filterExpression));
        }
        if (requestPathOptions.hasParameters()) {
            modelPathOptionsBuilder
                    .parameters(requestPathOptions.parameters().entrySet().stream()
                            .map(requestParameter ->
                                    PathParameter.builder()
                                            .pathParameterName(requestParameter.getKey())
                                            .datasetParameter(getDatasetParameter(requestParameter.getValue()))
                                            .build()).collect(Collectors.toList())
                    ).build();
        }
        return modelPathOptionsBuilder.build();
    }

    private static software.amazon.databrew.dataset.FilterExpression getModelFilterExpression(FilterExpression filterExpression) {
        return new software.amazon.databrew.dataset.FilterExpression().builder()
                .expression(filterExpression.expression())
                .valuesMap(filterExpression.valuesMap().entrySet().stream()
                        .map(entry -> FilterValue.builder()
                                .valueReference(entry.getKey())
                                .value(entry.getValue())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }


    public static PathOptions buildRequestPathOptions(final software.amazon.databrew.dataset.PathOptions pathOptions) {
        if (pathOptions == null) return null;
        PathOptions.Builder pathOptionsBuilder = PathOptions.builder();
        software.amazon.databrew.dataset.FilesLimit filesLimit = pathOptions.getFilesLimit();
        if (filesLimit != null) {
            FilesLimit.Builder filesLimitBuilder = FilesLimit.builder();
            pathOptionsBuilder
                    .filesLimit(filesLimitBuilder
                            .maxFiles(filesLimit.getMaxFiles())
                            .orderedBy(filesLimit.getOrderedBy())
                            .order(filesLimit.getOrder())
                            .build());
        }
        software.amazon.databrew.dataset.FilterExpression lastModifiedDateCondition = pathOptions.getLastModifiedDateCondition();
        if (lastModifiedDateCondition != null) {
            pathOptionsBuilder.lastModifiedDateCondition(getFilterExpression(lastModifiedDateCondition));
        }
        List<PathParameter> parameters = pathOptions.getParameters();
        if (parameters != null) {
            pathOptionsBuilder
                    .parameters(buildMapFromList(pathOptions.getParameters(),
                            param -> param.getPathParameterName(),
                            param -> getModelDatasetParameter(param.getDatasetParameter())))
                    .build();
        }
        return pathOptionsBuilder.build();
    }

    private static DatasetParameter getModelDatasetParameter(software.amazon.databrew.dataset.DatasetParameter datasetParameter) {
        DatasetParameter.Builder datasetParameterBuilder = DatasetParameter.builder();
        datasetParameterBuilder.name(datasetParameter.getName());
        datasetParameterBuilder.type(datasetParameter.getType());
        datasetParameterBuilder.createColumn(datasetParameter.getCreateColumn());
        software.amazon.databrew.dataset.FilterExpression filterExpression = datasetParameter.getFilter();
        if (filterExpression != null) {
            datasetParameterBuilder.filter(getFilterExpression(filterExpression));
        }
        software.amazon.databrew.dataset.DatetimeOptions datetimeOptions = datasetParameter.getDatetimeOptions();
        if (datetimeOptions != null) {

            datasetParameterBuilder.datetimeOptions(DatetimeOptions.builder()
                    .format(datetimeOptions.getFormat())
                    .localeCode(datetimeOptions.getLocaleCode())
                    .timezoneOffset(datetimeOptions.getTimezoneOffset())
                    .build());
        }
        return datasetParameterBuilder.build();
    }

    private static FilterExpression getFilterExpression(software.amazon.databrew.dataset.FilterExpression filterExpression) {
        return FilterExpression.builder()
                .expression(filterExpression.getExpression())
                .valuesMap(buildMapFromList(filterExpression.getValuesMap(),
                        filterValue -> filterValue.getValueReference(),
                        filterValue -> filterValue.getValue()))
                .build();
    }


    private static software.amazon.databrew.dataset.DatasetParameter getDatasetParameter(DatasetParameter requestDatasetParameter) {
        software.amazon.databrew.dataset.DatasetParameter.DatasetParameterBuilder modelDatasetBuilder = new software.amazon.databrew.dataset.DatasetParameter().builder();
        modelDatasetBuilder.name(requestDatasetParameter.name());
        modelDatasetBuilder.type(requestDatasetParameter.typeAsString());
        modelDatasetBuilder.createColumn(requestDatasetParameter.createColumn());
        DatetimeOptions datetimeOptions = requestDatasetParameter.datetimeOptions();
        if (datetimeOptions != null) {
            software.amazon.databrew.dataset.DatetimeOptions.DatetimeOptionsBuilder datetimeOptionsBuilder = new software.amazon.databrew.dataset.DatetimeOptions().builder();
            modelDatasetBuilder.datetimeOptions(
                    datetimeOptionsBuilder
                            .format(datetimeOptions.format())
                            .localeCode(datetimeOptions.localeCode())
                            .timezoneOffset(datetimeOptions.timezoneOffset())
                            .build());
        }
        FilterExpression filterExpression = requestDatasetParameter.filter();
        if (filterExpression != null) {
            modelDatasetBuilder.filter(getModelFilterExpression(filterExpression));
        }
        return modelDatasetBuilder.build();
    }

    public static List<Tag> buildModelTags(final Map<String, String> tags) {
        if (tags == null) return null;
        List<Tag> tagArrayList = new ArrayList<>();
        tags.forEach((k, v) -> tagArrayList.add(Tag.builder().key(k).value(v).build()));
        return tagArrayList;
    }
}

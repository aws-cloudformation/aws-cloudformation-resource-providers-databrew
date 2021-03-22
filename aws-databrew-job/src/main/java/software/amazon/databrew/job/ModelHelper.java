package software.amazon.databrew.job;

import software.amazon.awssdk.services.databrew.model.DescribeJobResponse;
import software.amazon.awssdk.services.databrew.model.Job;
import software.amazon.awssdk.services.databrew.model.S3Location;
import software.amazon.awssdk.services.databrew.model.Output;
import software.amazon.awssdk.services.databrew.model.DataCatalogOutput;
import software.amazon.awssdk.services.databrew.model.S3TableOutputOptions;
import software.amazon.awssdk.services.databrew.model.DatabaseTableOutputOptions;
import software.amazon.awssdk.services.databrew.model.RecipeReference;
import software.amazon.awssdk.services.databrew.model.OutputFormatOptions;
import software.amazon.awssdk.services.databrew.model.CsvOutputOptions;
import software.amazon.awssdk.services.databrew.model.SampleMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ModelHelper {
    public enum Type {
        PROFILE,
        RECIPE
    }

    public static ResourceModel constructModel(final DescribeJobResponse job) {
        Map<String, String> tags = job.tags();
        ResourceModel model = ResourceModel.builder()
                .datasetName(job.datasetName())
                .name(job.name())
                .type(job.typeAsString())
                .encryptionKeyArn(job.encryptionKeyArn())
                .encryptionMode(job.encryptionModeAsString())
                .logSubscription(job.logSubscriptionAsString())
                .maxCapacity(job.maxCapacity())
                .maxRetries(job.maxRetries())
                .recipe(buildModelRecipe(job.recipeReference()))
                .roleArn(job.roleArn())
                .tags(tags != null ? buildModelTags(tags) : null)
                .timeout(job.timeout())
                .build();
        if (job.typeAsString().equals(Type.RECIPE.toString())) {
            model.setOutputs(buildModelOutputs(job.outputs()));
            model.setDataCatalogOutputs(buildModelDataCatalogOutputs(job.dataCatalogOutputs()));
        } else if (job.typeAsString().equals(Type.PROFILE.toString())) {
            model.setOutputLocation(buildModelOutputLocation(job.outputs()));
            model.setJobSample(buildRequestJobSample(job.jobSample()));
        }

        return model;
    }

    public static ResourceModel constructModel(final Job job) {
        Map<String, String> tags = job.tags();
        ResourceModel model = ResourceModel.builder()
                .datasetName(job.datasetName())
                .name(job.name())
                .type(job.typeAsString())
                .encryptionKeyArn(job.encryptionKeyArn())
                .encryptionMode(job.encryptionModeAsString())
                .logSubscription(job.logSubscriptionAsString())
                .maxCapacity(job.maxCapacity())
                .maxRetries(job.maxRetries())
                .recipe(buildModelRecipe(job.recipeReference()))
                .roleArn(job.roleArn())
                .tags(tags != null ? buildModelTags(tags) : null)
                .timeout(job.timeout())
                .build();
        if (job.typeAsString().equals(Type.RECIPE.toString())) {
            model.setOutputs(buildModelOutputs(job.outputs()));
            model.setDataCatalogOutputs(buildModelDataCatalogOutputs(job.dataCatalogOutputs()));
        } else if (job.typeAsString().equals(Type.PROFILE.toString())) {
            model.setOutputLocation(buildModelOutputLocation(job.outputs()));
            model.setJobSample(buildRequestJobSample(job.jobSample()));
        }

        return model;
    }

    public static JobSample buildRequestJobSample(final software.amazon.awssdk.services.databrew.model.JobSample jobSample) {
        if (jobSample == null) {
            return null;
        } else if (jobSample.mode().equals(SampleMode.FULL_DATASET)) {
            return JobSample.builder()
                    .mode(jobSample.modeAsString())
                    .build();
        } else {
            return JobSample.builder()
                    .mode(jobSample.modeAsString())
                    .size(jobSample.size())
                    .build();
        }
    }

    public static software.amazon.awssdk.services.databrew.model.JobSample buildModelJobSample(final JobSample jobSample) {
        if (jobSample == null) {
            return null;
        } else if (jobSample.getMode().equals(SampleMode.FULL_DATASET)) {
            return software.amazon.awssdk.services.databrew.model.JobSample.builder()
                    .mode(jobSample.getMode())
                    .build();
        } else {
            return software.amazon.awssdk.services.databrew.model.JobSample.builder()
                    .mode(jobSample.getMode())
                    .size(jobSample.getSize())
                    .build();
        }
    }

    public static S3Location buildRequestS3Location(final software.amazon.databrew.job.S3Location modelS3Location) {
        return modelS3Location == null ? null : S3Location.builder()
                .bucket(modelS3Location.getBucket())
                .key(modelS3Location.getKey())
                .build();
    }

    public static S3Location buildRequestS3Location(final software.amazon.databrew.job.OutputLocation modelS3Location) {
        return modelS3Location == null ? null : S3Location.builder()
                .bucket(modelS3Location.getBucket())
                .key(modelS3Location.getKey())
                .build();
    }

    public static software.amazon.databrew.job.OutputLocation buildModelOutputLocation(final List<Output> requestOutputs) {
        if (requestOutputs == null || requestOutputs.get(0) == null) return null;;
        return requestOutputs.get(0).location() == null ? null : software.amazon.databrew.job.OutputLocation.builder()
                .bucket(requestOutputs.get(0).location().bucket())
                .key(requestOutputs.get(0).location().key())
                .build();
    }

    public static software.amazon.databrew.job.OutputFormatOptions buildModelFormatOptions(final OutputFormatOptions requestFormatOptions) {
        if (requestFormatOptions == null) return null;
        software.amazon.databrew.job.OutputFormatOptions.OutputFormatOptionsBuilder modelFormatOptionsBuilder = new software.amazon.databrew.job.OutputFormatOptions().builder();
        if (requestFormatOptions.csv() != null) {
            software.amazon.databrew.job.CsvOutputOptions modelCsvOutputOptions = new software.amazon.databrew.job.CsvOutputOptions();
            modelFormatOptionsBuilder
                    .csv(modelCsvOutputOptions.builder()
                            .delimiter(requestFormatOptions.csv().delimiter())
                            .build());
        }
        return modelFormatOptionsBuilder.build();
    }

    public static OutputFormatOptions buildRequestFormatOptions(final software.amazon.databrew.job.OutputFormatOptions modelFormatOptions) {
        if (modelFormatOptions == null) return null;
        OutputFormatOptions.Builder requestOutputFormatOptionsBuilder = OutputFormatOptions.builder();
        if (modelFormatOptions.getCsv() != null) {
            CsvOutputOptions requestCsvOutputOptions = CsvOutputOptions.builder()
                    .delimiter(modelFormatOptions.getCsv().getDelimiter())
                    .build();
            requestOutputFormatOptionsBuilder.csv(requestCsvOutputOptions);
        }
        return requestOutputFormatOptionsBuilder.build();
    }

    public static software.amazon.databrew.job.S3Location buildModelS3Location(final S3Location requestS3Location) {
        return requestS3Location == null ? null : software.amazon.databrew.job.S3Location.builder()
                .bucket(requestS3Location.bucket())
                .key(requestS3Location.key())
                .build();
    }

    public static List<Output> buildRequestOutputs(final List<software.amazon.databrew.job.Output> outputs) {
        List<Output> requestOutputs = new ArrayList<>();
        if (outputs == null) return null;
        outputs.forEach(output -> {
            Output requestOutput = Output.builder()
                    .compressionFormat(output.getCompressionFormat())
                    .format(output.getFormat())
                    .formatOptions(buildRequestFormatOptions(output.getFormatOptions()))
                    .partitionColumns(output.getPartitionColumns())
                    .location(buildRequestS3Location(output.getLocation()))
                    .overwrite(output.getOverwrite())
                    .build();
           requestOutputs.add(requestOutput);
        });
        return requestOutputs;
    }

    public static List<software.amazon.databrew.job.Output> buildModelOutputs(final List<Output> outputs) {
        List<software.amazon.databrew.job.Output> modelOutputs = new ArrayList<>();
        if (outputs == null) return null;
        outputs.forEach(output -> {
            software.amazon.databrew.job.Output modelOutput = new software.amazon.databrew.job.Output().builder()
                    .compressionFormat(output.compressionFormatAsString())
                    .format(output.formatAsString())
                    .formatOptions(buildModelFormatOptions(output.formatOptions()))
                    .partitionColumns(output.partitionColumns())
                    .location(buildModelS3Location(output.location()))
                    .overwrite(output.overwrite())
                    .build();
            modelOutputs.add(modelOutput);
        });
        return modelOutputs;
    }

    public static List<DataCatalogOutput> buildRequestDataCatalogOutputs(final List<software.amazon.databrew.job.DataCatalogOutput> dataCatalogOutputs) {
        List<DataCatalogOutput> requestDataCatalogOutputs = new ArrayList<>();
        if (dataCatalogOutputs == null) return null;
        dataCatalogOutputs.forEach(dataCatalogOutput -> {
            DataCatalogOutput requestDataCatalogOutput = DataCatalogOutput.builder()
                    .catalogId(dataCatalogOutput.getCatalogId())
                    .databaseName(dataCatalogOutput.getDatabaseName())
                    .tableName(dataCatalogOutput.getTableName())
                    .s3Options(buildRequestS3TableOutputOptions(dataCatalogOutput.getS3Options()))
                    .databaseOptions(buildRequestDatabaseTableOutputOptions(dataCatalogOutput.getDatabaseOptions()))
                    .overwrite(dataCatalogOutput.getOverwrite())
                    .build();
            requestDataCatalogOutputs.add(requestDataCatalogOutput);
        });
        return requestDataCatalogOutputs;
    }

    public static List<software.amazon.databrew.job.DataCatalogOutput> buildModelDataCatalogOutputs(final List<DataCatalogOutput> dataCatalogOutputs) {
        List<software.amazon.databrew.job.DataCatalogOutput> modelDataCatalogOutputs = new ArrayList<>();
        if (dataCatalogOutputs == null) return null;
        dataCatalogOutputs.forEach(dataCatalogOutput -> {
            software.amazon.databrew.job.DataCatalogOutput modelDataCatalogOutput = new software.amazon.databrew.job.DataCatalogOutput().builder()
                    .catalogId(dataCatalogOutput.catalogId())
                    .databaseName(dataCatalogOutput.databaseName())
                    .tableName(dataCatalogOutput.tableName())
                    .s3Options(buildModelS3TableOutputOptions(dataCatalogOutput.s3Options()))
                    .databaseOptions(buildModelDatabaseTableOutputOptions(dataCatalogOutput.databaseOptions()))
                    .overwrite(dataCatalogOutput.overwrite())
                    .build();
            modelDataCatalogOutputs.add(modelDataCatalogOutput);
        });
        return modelDataCatalogOutputs;
    }

    public static S3TableOutputOptions buildRequestS3TableOutputOptions(final software.amazon.databrew.job.S3TableOutputOptions modelS3TableOutputOptions) {
        return modelS3TableOutputOptions == null ? null : S3TableOutputOptions.builder()
                .location(buildRequestS3Location(modelS3TableOutputOptions.getLocation()))
                .build();
    }

    public static software.amazon.databrew.job.S3TableOutputOptions buildModelS3TableOutputOptions(final S3TableOutputOptions requestS3TableOutputOptions) {
        return requestS3TableOutputOptions == null ? null : software.amazon.databrew.job.S3TableOutputOptions.builder()
                .location(buildModelS3Location(requestS3TableOutputOptions.location()))
                .build();
    }

    public static DatabaseTableOutputOptions buildRequestDatabaseTableOutputOptions(final software.amazon.databrew.job.DatabaseTableOutputOptions modelDatabaseTableOutputOptions) {
        return modelDatabaseTableOutputOptions == null ? null : DatabaseTableOutputOptions.builder()
                .tempDirectory(buildRequestS3Location(modelDatabaseTableOutputOptions.getTempDirectory()))
                .tableName(modelDatabaseTableOutputOptions.getTableName())
                .build();
    }

    public static software.amazon.databrew.job.DatabaseTableOutputOptions buildModelDatabaseTableOutputOptions(final DatabaseTableOutputOptions requestDatabaseTableOutputOptions) {
        return requestDatabaseTableOutputOptions == null ? null : software.amazon.databrew.job.DatabaseTableOutputOptions.builder()
                .tempDirectory(buildModelS3Location(requestDatabaseTableOutputOptions.tempDirectory()))
                .tableName(requestDatabaseTableOutputOptions.tableName())
                .build();
    }

    public static RecipeReference buildRequestRecipe(final Recipe modelRecipe) {
        return modelRecipe == null ? null : RecipeReference.builder()
                .name(modelRecipe.getName())
                .recipeVersion(modelRecipe.getVersion())
                .build();
    }

    public static Recipe buildModelRecipe(final RecipeReference requestRecipe) {
        return requestRecipe == null ? null : Recipe.builder()
                .name(requestRecipe.name())
                .version(requestRecipe.recipeVersion())
                .build();
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

    public static List<Tag> buildModelTags(final Map<String, String> tags) {
        List<Tag> tagArrayList = new ArrayList<Tag>();
        if (tags == null) return null;
        tags.forEach((k, v) -> tagArrayList.add(Tag.builder().key(k).value(v).build()));
        return tagArrayList;
    }
}

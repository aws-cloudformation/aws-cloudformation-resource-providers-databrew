package software.amazon.databrew.dataset;

import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.DatabaseInputDefinition;
import software.amazon.awssdk.services.databrew.model.Dataset;
import software.amazon.awssdk.services.databrew.model.DescribeDatasetResponse;
import software.amazon.awssdk.services.databrew.model.Input;
import software.amazon.awssdk.services.databrew.model.Metadata;
import software.amazon.awssdk.services.databrew.model.ResourceNotFoundException;
import software.amazon.awssdk.services.databrew.model.S3Location;
import software.amazon.awssdk.services.databrew.model.ValidationException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    @BeforeEach
    public void setup() {
        proxy = mock(AmazonWebServicesClientProxy.class);
        logger = mock(Logger.class);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final ReadHandler handler = new ReadHandler();

        Dataset dataset = Dataset.builder()
                .name(TestUtil.DATASET_NAME)
                .formatOptions(ModelHelper.buildRequestFormatOptions(TestUtil.JSON_FORMAT_OPTIONS))
                .pathOptions(ModelHelper.buildRequestPathOptions(TestUtil.PATH_OPTIONS_WITH_VALID_PARAM))
                .tags(TestUtil.sampleTags())
                .build();

        final DescribeDatasetResponse describeResult = DescribeDatasetResponse.builder()
                .name(dataset.name())
                .build();

        doReturn(describeResult)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        TestUtil.assertThatDatasetModelsAreEqual(response.getResourceModel(), dataset);
    }

    @Test
    public void handleRequest_FailedRead_MissingRequiredParameterException() {
        doThrow(DataBrewException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ReadHandler handler = new ReadHandler();
        final ResourceModel model = ResourceModel.builder()
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
    }

    @Test
    public void handleRequest_FailedRead_InvalidParameterException() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ReadHandler handler = new ReadHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.INVALID_DATASET_NAME)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
    }

    @Test
    public void handleRequest_FailedRead_ResourceNotFoundException() {
        doThrow(ResourceNotFoundException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ReadHandler handler = new ReadHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.INVALID_DATASET_NAME)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
    }


    @Test
    public void handleRequest_SuccessfulRead_DatabaseSQLDataset() {
        final ReadHandler handler = new ReadHandler();

        Input datasetInput = Input.builder().databaseInputDefinition(
                DatabaseInputDefinition.builder()
                        .queryString(TestUtil.DATABASE_INPUT_SQL_STR)
                        .glueConnectionName(TestUtil.GLUE_CONNECTION_NAME)
                        .build()
        ).build();

        Dataset dataset = Dataset.builder()
                .name(TestUtil.DATASET_NAME)
                .input(datasetInput)
                .build();

        final DescribeDatasetResponse describeResult = DescribeDatasetResponse.builder()
                .name(TestUtil.DATASET_NAME)
                .input(datasetInput)
                .build();

        doReturn(describeResult)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());


        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getResourceModel().getInput().getDatabaseInputDefinition().getQueryString()).isNotEmpty();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getResourceModel().getTags()).isNotNull();
        assertThat(response.getErrorCode()).isNull();
        TestUtil.assertThatDatasetModelsAreEqual(response.getResourceModel(), dataset);
    }


    @Test
    public void handleRequest_SuccessfulRead_MetadataInputDataset() {
        final ReadHandler handler = new ReadHandler();

        Input datasetInput = Input.builder()
                .s3InputDefinition(S3Location.builder().bucket(TestUtil.S3_BUCKET).key(TestUtil.S3_KEY).build())
                .metadata(Metadata.builder().sourceArn(TestUtil.METADATA_SOURCE_ARN).build())
                .build();

        Dataset dataset = Dataset.builder()
                .name(TestUtil.DATASET_NAME)
                .input(datasetInput)
                .build();

        final DescribeDatasetResponse describeResult = DescribeDatasetResponse.builder()
                .name(TestUtil.DATASET_NAME)
                .input(datasetInput)
                .build();

        doReturn(describeResult)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());


        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getResourceModel().getInput().getMetadata().getSourceArn()).isNotEmpty();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        TestUtil.assertThatDatasetModelsAreEqual(response.getResourceModel(), dataset);
    }
}

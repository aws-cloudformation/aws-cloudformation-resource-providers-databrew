package software.amazon.databrew.dataset;

import software.amazon.awssdk.services.databrew.model.ConflictException;
import software.amazon.awssdk.services.databrew.model.CreateDatasetResponse;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.ServiceQuotaExceededException;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest {

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
        final CreateHandler handler = new CreateHandler();
        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();
        doReturn(createDatasetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .input(TestUtil.S3_INPUT)
                .formatOptions(TestUtil.JSON_FORMAT_OPTIONS)
                .tags(ModelHelper.buildModelTags(TestUtil.sampleTags()))
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailedCreate_ResourceAlreadyExistsException() {
        doThrow(ConflictException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.INVALID_DATASET_NAME)
                .input(TestUtil.S3_INPUT)
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
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AlreadyExists);
    }

    @Test
    public void handleRequest_FailedCreate_MissingRequiredParameterException() {
        doThrow(DataBrewException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .input(TestUtil.S3_INPUT)
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
    public void handleRequest_FailedCreate_InvalidParameterException() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.INVALID_DATASET_NAME)
                .input(TestUtil.S3_INPUT)
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
    public void handleRequest_FailedCreate_LimitExceededException() {
        doThrow(ServiceQuotaExceededException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .input(TestUtil.S3_INPUT)
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
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ServiceLimitExceeded);
    }

    @Test
    public void handleRequest_SuccessfulCreate_ValidCsvDelimiter() {
        final CreateHandler handler = new CreateHandler();
        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();
        doReturn(createDatasetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .input(TestUtil.CSV_S3_INPUT)
                .formatOptions(TestUtil.CSV_FORMAT_OPTIONS)
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModel().getFormatOptions()).isNotNull();
        assertThat(response.getResourceModel().getFormatOptions().getCsv()).isNotNull();
        assertThat(response.getResourceModel().getFormatOptions().getCsv().getDelimiter()).isEqualTo(TestUtil.PIPE_CSV_DELIMITER);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailedCreate_InvalidCsvDelimiter() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .input(TestUtil.S3_INPUT)
                .formatOptions(TestUtil.INVALID_CSV_FORMAT_OPTIONS)
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
    public void handleRequest_SuccessfulCreate_TsvDataset() {
        final CreateHandler handler = new CreateHandler();
        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();
        doReturn(createDatasetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .input(TestUtil.TSV_S3_INPUT)
                .formatOptions(TestUtil.TSV_FORMAT_OPTIONS)
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModel().getFormatOptions()).isNotNull();
        assertThat(response.getResourceModel().getFormatOptions().getCsv()).isNotNull();
        assertThat(response.getResourceModel().getFormatOptions().getCsv().getDelimiter()).isEqualTo(TestUtil.TAB_CSV_DELIMITER);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessfulCreate_ValidFormatWithHeaderlessCsv() {
        final CreateHandler handler = new CreateHandler();
        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();
        doReturn(createDatasetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .format(TestUtil.CSV_FORMAT)
                .input(TestUtil.CSV_S3_INPUT)
                .formatOptions(TestUtil.CSV_FORMAT_OPTIONS_HEADERLESS)
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModel().getFormatOptions()).isNotNull();
        assertThat(response.getResourceModel().getFormatOptions().getCsv()).isNotNull();
        assertThat(response.getResourceModel().getFormatOptions().getCsv().getDelimiter()).isEqualTo(TestUtil.PIPE_CSV_DELIMITER);
        assertThat(response.getResourceModel().getFormatOptions().getCsv().getHeaderRow()).isEqualTo(false);
        assertThat(response.getResourceModel().getFormat()).isEqualTo(TestUtil.CSV_FORMAT);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessfulCreate_ValidFormatWithHeaderlessExcel() {
        final CreateHandler handler = new CreateHandler();
        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();
        doReturn(createDatasetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .format(TestUtil.EXCEL_FORMAT)
                .input(TestUtil.EXCEL_S3_INPUT)
                .formatOptions(TestUtil.EXCEL_FORMAT_OPTIONS_HEADERLESS)
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModel().getFormatOptions()).isNotNull();
        assertThat(response.getResourceModel().getFormatOptions().getExcel()).isNotNull();
        assertThat(response.getResourceModel().getFormatOptions().getExcel().getHeaderRow()).isEqualTo(false);
        assertThat(response.getResourceModel().getFormat()).isEqualTo(TestUtil.EXCEL_FORMAT);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessfulCreate_ValidFormatWithExtensionlessJson() {
        final CreateHandler handler = new CreateHandler();
        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();
        doReturn(createDatasetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .format(TestUtil.JSON_FORMAT)
                .input(TestUtil.JSON_S3_INPUT_EXTENSIONLESS)
                .formatOptions(TestUtil.JSON_FORMAT_OPTIONS)
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModel().getFormatOptions()).isNotNull();
        assertThat(response.getResourceModel().getFormatOptions().getJson()).isNotNull();
        assertThat(response.getResourceModel().getFormat()).isEqualTo(TestUtil.JSON_FORMAT);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailedCreate_MismatchedParquetFormatAndJsonFormatOptions() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .format(TestUtil.PARQUET_FORMAT)
                .input(TestUtil.JSON_S3_INPUT_EXTENSIONLESS)
                .formatOptions(TestUtil.JSON_FORMAT_OPTIONS)
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
    public void handleRequest_SuccessfulCreate_ValidFilesLimit() {
        final CreateHandler handler = new CreateHandler();
        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();
        doReturn(createDatasetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .format(TestUtil.JSON_FORMAT)
                .input(TestUtil.S3_FOLDER_INPUT)
                .formatOptions(TestUtil.JSON_FORMAT_OPTIONS)
                .pathOptions(TestUtil.PATH_OPTIONS_WITH_VALID_FILES_LIMIT)
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModel().getPathOptions()).isNotNull();
        assertThat(response.getResourceModel().getPathOptions().getFilesLimit()).isNotNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailedCreate_InvalidFilesLimit() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .input(TestUtil.S3_FOLDER_INPUT)
                .pathOptions(TestUtil.PATH_OPTIONS_WITH_INVALID_FILES_LIMIT)
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
    public void handleRequest_SuccessfulCreate_ValidLastModified() {
        final CreateHandler handler = new CreateHandler();
        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();
        doReturn(createDatasetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .format(TestUtil.JSON_FORMAT)
                .input(TestUtil.S3_REGEX_INPUT)
                .formatOptions(TestUtil.JSON_FORMAT_OPTIONS)
                .pathOptions(TestUtil.PATH_OPTIONS_WITH_VALID_LAST_MODIFIED)
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModel().getPathOptions()).isNotNull();
        assertThat(response.getResourceModel().getPathOptions().getLastModifiedDateCondition()).isNotNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailedCreate_InvalidLastModified() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .input(TestUtil.S3_REGEX_INPUT)
                .pathOptions(TestUtil.PATH_OPTIONS_WITH_INVALID_LAST_MODIFIED)
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
    public void handleRequest_SuccessfulCreate_ValidPathParam() {
        final CreateHandler handler = new CreateHandler();
        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();
        doReturn(createDatasetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .format(TestUtil.JSON_FORMAT)
                .input(TestUtil.S3_PARAM_INPUT)
                .formatOptions(TestUtil.JSON_FORMAT_OPTIONS)
                .pathOptions(TestUtil.PATH_OPTIONS_WITH_VALID_PARAM)
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModel().getPathOptions()).isNotNull();
        assertThat(response.getResourceModel().getPathOptions().getParameters()).isNotNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailedCreate_InvalidPathParam() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .input(TestUtil.S3_PARAM_INPUT)
                .pathOptions(TestUtil.PATH_OPTIONS_WITH_INVALID_PARAM)
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
    public void handleRequest_SuccessfulCreate_DatabaseDataset() {
        final CreateHandler handler = new CreateHandler();
        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();
        doReturn(createDatasetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        Map<String, String> tags = TestUtil.sampleTags();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .input(TestUtil.DATABASE_INPUT)
                .tags(ModelHelper.buildModelTags(tags))
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getResourceModel().getTags()).isNotNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessfulCreate_DatabaseSQLDataset() {
        final CreateHandler handler = new CreateHandler();

        Map<String, String> tags = TestUtil.sampleTags();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .input(TestUtil.DATABASE_SQL_INPUT)
                .tags(ModelHelper.buildModelTags(tags))
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getResourceModel().getInput().getDatabaseInputDefinition().getQueryString()).isNotEmpty();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getResourceModel().getTags()).isNotNull();
        assertThat(response.getErrorCode()).isNull();
    }


    @Test
    public void handleRequest_SuccessfulCreate_MetadataInputDataset() {
        final CreateHandler handler = new CreateHandler();

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .input(TestUtil.METADATA_INPUT_DATASET)
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getResourceModel().getInput().getMetadata().getSourceArn()).isNotEmpty();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessfulCreate_ValidFormatWithORC() {
        final CreateHandler handler = new CreateHandler();
        final CreateDatasetResponse createDatasetResponse = CreateDatasetResponse.builder().build();
        doReturn(createDatasetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.DATASET_NAME)
                .format(TestUtil.ORC_FORMAT)
                .input(TestUtil.ORC_S3_INPUT)
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
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModel().getFormat()).isEqualTo(TestUtil.ORC_FORMAT);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }
}

package software.amazon.databrew.job;

import software.amazon.awssdk.services.databrew.model.ConflictException;
import software.amazon.awssdk.services.databrew.model.CreateProfileJobResponse;
import software.amazon.awssdk.services.databrew.model.CreateRecipeJobResponse;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.OutputFormat;
import software.amazon.awssdk.services.databrew.model.ProfileConfiguration;
import software.amazon.awssdk.services.databrew.model.ResourceNotFoundException;
import software.amazon.awssdk.services.databrew.model.StatisticsConfiguration;
import software.amazon.awssdk.services.databrew.model.ValidationException;
import software.amazon.awssdk.services.databrew.model.AccessDeniedException;
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

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static software.amazon.databrew.job.TestUtil.CSV_OUTPUT_WITH_MAX_OUTPUT_FILES;
import static software.amazon.databrew.job.TestUtil.DATA_CATALOG_OUTPUT_LIST;
import static software.amazon.databrew.job.TestUtil.INVALID_JOB_NAME;
import static software.amazon.databrew.job.TestUtil.JOB_NAME;
import static software.amazon.databrew.job.TestUtil.JOB_TYPE_PROFILE;
import static software.amazon.databrew.job.TestUtil.JOB_TYPE_RECIPE;
import static software.amazon.databrew.job.TestUtil.TIMEOUT;
import static software.amazon.databrew.job.TestUtil.TABLEAUHYPER_OUTPUTS;
import static software.amazon.databrew.job.TestUtil.CSV_OUTPUT_VALID_DELIMITER;
import static software.amazon.databrew.job.TestUtil.CSV_OUTPUT_INVALID_DELIMITER;
import static software.amazon.databrew.job.TestUtil.DATASET_STATISTICS_CONFIGURATION;
import static software.amazon.databrew.job.TestUtil.PROFILE_COLUMNS;
import static software.amazon.databrew.job.TestUtil.COLUMN_STATISTICS_CONFIGURATIONS;
import static software.amazon.databrew.job.TestUtil.VALID_BUCKET_OWNER_OUTPUT;
import static software.amazon.databrew.job.TestUtil.INVALID_BUCKET_OWNER_OUTPUT;
import static software.amazon.databrew.job.TestUtil.VALID_BUCKET_OWNER;

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
    public void handleRequest_SimpleSuccess_ProfileJob() {
        final CreateHandler handler = new CreateHandler();
        final CreateProfileJobResponse createProfileJobResponse = CreateProfileJobResponse.builder().build();
        doReturn(createProfileJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
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
    public void handleRequest_SimpleSuccess_ProfileJobWithValidationConfiguration() {
        final CreateHandler handler = new CreateHandler();
        final CreateProfileJobResponse createProfileJobResponse = CreateProfileJobResponse.builder().build();
        doReturn(createProfileJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .tags(ModelHelper.buildModelTags(TestUtil.sampleTags()))
                .validationConfigurations(ModelHelper.buildModelValidationConfigurations(TestUtil.createValidationConfigurations()))
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
    public void handleRequest_FailedCreate_ValidationException_ProfileJobWithInvalidValidationConfiguration() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .timeout(TIMEOUT)
                .validationConfigurations(ModelHelper.buildModelValidationConfigurations(TestUtil.createInvalidValidationConfigurations()))
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
    public void handleRequest_FailedCreate_ResourceNotException_ProfileJobWithInvalidValidationConfiguration() {
        doThrow(ResourceNotFoundException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .timeout(TIMEOUT)
                .validationConfigurations(ModelHelper.buildModelValidationConfigurations(TestUtil.createInvalidValidationConfigurations()))
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
    public void handleRequest_FailedCreate_InvalidParameterException_ProfileJob() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(INVALID_JOB_NAME)
                .timeout(TIMEOUT)
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
    public void handleRequest_FailedCreate_CreateFailedException_ProfileJob() {
        doThrow(DataBrewException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
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
    public void handleRequest_FailedCreate_ResourceNotFoundException_ProfileJob() {
        doThrow(ResourceNotFoundException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(TestUtil.INVALID_JOB_NAME)
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
    public void handleRequest_FailedCreate_ConflictException_ProfileJob() {
        doThrow(ConflictException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(TestUtil.INVALID_JOB_NAME)
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
    public void handleRequest_WithFullDatasetMode_SimpleSuccess_ProfileJob() {
        final CreateHandler handler = new CreateHandler();
        final CreateProfileJobResponse createProfileJobResponse = CreateProfileJobResponse.builder().build();
        doReturn(createProfileJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .tags(ModelHelper.buildModelTags(TestUtil.sampleTags()))
                .jobSample(ModelHelper.buildRequestJobSample(TestUtil.fullDatasetModeJobSample()))
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
    public void handleRequest_WithCustomRowsMode_SimpleSuccess_ProfileJob() {
        final CreateHandler handler = new CreateHandler();
        final CreateProfileJobResponse createProfileJobResponse = CreateProfileJobResponse.builder().build();
        doReturn(createProfileJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .tags(ModelHelper.buildModelTags(TestUtil.sampleTags()))
                .jobSample(ModelHelper.buildRequestJobSample(TestUtil.customRowsModeJobSample()))
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
    public void handleRequest_WithInvalidJobSample1_CreateFailed_ProfileJob() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .tags(ModelHelper.buildModelTags(TestUtil.sampleTags()))
                .jobSample(ModelHelper.buildRequestJobSample(TestUtil.invalidJobSample1()))
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
    public void handleRequest_WithInvalidJobSample2_CreateFailed_ProfileJob() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .tags(ModelHelper.buildModelTags(TestUtil.sampleTags()))
                .jobSample(ModelHelper.buildRequestJobSample(TestUtil.invalidJobSample2()))
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
    public void handleRequest_WithInvalidJobSample3_CreateFailed_ProfileJob() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .tags(ModelHelper.buildModelTags(TestUtil.sampleTags()))
                .jobSample(ModelHelper.buildRequestJobSample(TestUtil.invalidJobSample3()))
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
    public void handleRequest_WithInvalidJobSample4_CreateFailed_ProfileJob() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .tags(ModelHelper.buildModelTags(TestUtil.sampleTags()))
                .jobSample(ModelHelper.buildRequestJobSample(TestUtil.invalidJobSample4()))
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
    public void handleRequest_SimpleSuccess_RecipeJob() {
        final CreateHandler handler = new CreateHandler();
        final CreateRecipeJobResponse createRecipeJobResponse = CreateRecipeJobResponse.builder().build();
        doReturn(createRecipeJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(JOB_NAME)
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
    public void handleRequest_FailedCreate_InvalidParameterException_RecipeJob() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(INVALID_JOB_NAME)
                .timeout(TIMEOUT)
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
    public void handleRequest_FailedCreate_CreateFailedException_RecipeJob() {
        doThrow(DataBrewException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
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
    public void handleRequest_FailedCreate_ResourceNotFoundException_RecipeJob() {
        doThrow(ResourceNotFoundException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(TestUtil.INVALID_JOB_NAME)
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
    public void handleRequest_FailedCreate_ConflictException_RecipeJob() {
        doThrow(ConflictException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(TestUtil.INVALID_JOB_NAME)
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
    public void handleRequest_SuccessfulCreate_RecipeJob_ValidCsvOutputDelimiter() {
        final CreateHandler handler = new CreateHandler();
        final CreateRecipeJobResponse createRecipeJobResponse = CreateRecipeJobResponse.builder().build();
        doReturn(createRecipeJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(JOB_NAME)
                .outputs(ModelHelper.buildModelOutputs(CSV_OUTPUT_VALID_DELIMITER))
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
        assertThat(response.getResourceModel().getOutputs().size()).isEqualTo(1);
        assertThat(response.getResourceModel().getOutputs().get(0).getFormatOptions()).isNotNull();
        assertThat(response.getResourceModel().getOutputs().get(0).getMaxOutputFiles()).isNull();
        assertThat(response.getResourceModel().getOutputs().get(0).getFormatOptions().getCsv()).isNotNull();
        assertThat(response.getResourceModel().getOutputs().get(0).getFormatOptions().getCsv().getDelimiter())
                .isEqualTo(TestUtil.PIPE_CSV_DELIMITER);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessfulCreate_RecipeJob_WithMaxOutputFiles() {
        final CreateHandler handler = new CreateHandler();
        final CreateRecipeJobResponse createRecipeJobResponse = CreateRecipeJobResponse.builder().build();
        doReturn(createRecipeJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(JOB_NAME)
                .outputs(ModelHelper.buildModelOutputs(CSV_OUTPUT_WITH_MAX_OUTPUT_FILES))
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
        assertThat(response.getResourceModel().getOutputs().size()).isEqualTo(1);
        assertThat(response.getResourceModel().getOutputs().get(0).getMaxOutputFiles()).isEqualTo(1);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailedCreate_RecipeJob_InvalidCsvOutputDelimiter() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(JOB_NAME)
                .outputs(ModelHelper.buildModelOutputs(CSV_OUTPUT_INVALID_DELIMITER))
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
    public void handleRequest_SuccessfulCreate_RecipeJob_ValidDataCatalogOutput() {
        final CreateHandler handler = new CreateHandler();
        final CreateRecipeJobResponse createRecipeJobResponse = CreateRecipeJobResponse.builder().build();
        doReturn(createRecipeJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(JOB_NAME)
                .dataCatalogOutputs(ModelHelper.buildModelDataCatalogOutputs(DATA_CATALOG_OUTPUT_LIST))
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
        assertThat(response.getResourceModel().getDataCatalogOutputs().size()).isEqualTo(2);
        assertThat(response.getResourceModel().getDataCatalogOutputs().get(0).getS3Options()).isNotNull();
        assertThat(response.getResourceModel().getDataCatalogOutputs().get(1).getDatabaseOptions()).isNotNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessfulCreate_RecipeJob_ValidDatabaseOutput() {
        final CreateHandler handler = new CreateHandler();
        final CreateRecipeJobResponse createRecipeJobResponse = CreateRecipeJobResponse.builder().build();
        doReturn(createRecipeJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(JOB_NAME)
                .databaseOutputs(ModelHelper.buildModelDatabaseOutputs(TestUtil.DATABASE_OUTPUT_LIST))
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
        assertThat(response.getResourceModel().getDatabaseOutputs().size()).isEqualTo(1);
        assertThat(response.getResourceModel().getDatabaseOutputs().get(0).getDatabaseOptions()).isNotNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessfulCreate_ProfileJob_ValidConfiguration() {
        final CreateHandler handler = new CreateHandler();
        final CreateProfileJobResponse createProfileJobResponse = CreateProfileJobResponse.builder().build();
        doReturn(createProfileJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        ProfileConfiguration configuration = ProfileConfiguration.builder()
                .datasetStatisticsConfiguration(DATASET_STATISTICS_CONFIGURATION)
                .profileColumns(PROFILE_COLUMNS)
                .columnStatisticsConfigurations(COLUMN_STATISTICS_CONFIGURATIONS)
                .build();

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .profileConfiguration(ModelHelper.buildModelProfileConfiguration(configuration))
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
        assertThat(response.getResourceModel().getProfileConfiguration()).isNotNull();
        assertThat(ModelHelper.buildRequestProfileConfiguration(response.getResourceModel().getProfileConfiguration())).isEqualTo(configuration);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailedCreate_ProfileJob_InvalidConfiguration() {
        final CreateHandler handler = new CreateHandler();

        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        ProfileConfiguration configuration = ProfileConfiguration.builder()
                .profileColumns(new ArrayList<>())
                .datasetStatisticsConfiguration(StatisticsConfiguration.builder().build())
                .build();

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .profileConfiguration(ModelHelper.buildModelProfileConfiguration(configuration))
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
    public void handleRequest_Success_CreateRecipeJob_ValidTABLEAUHYPEROutput() {
        final CreateHandler handler = new CreateHandler();
        final CreateRecipeJobResponse createRecipeJobResponse = CreateRecipeJobResponse.builder().build();
        doReturn(createRecipeJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(JOB_NAME)
                .outputs(ModelHelper.buildModelOutputs(TABLEAUHYPER_OUTPUTS))
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
        assertThat(response.getResourceModel().getOutputs().size()).isEqualTo(1);
        assertThat(response.getResourceModel().getOutputs().get(0).getFormat()).isEqualTo(OutputFormat.TABLEAUHYPER.name());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessfulCreate_RecipeJob_ValidBucketOwner() {
        final CreateHandler handler = new CreateHandler();
        final CreateRecipeJobResponse createRecipeJobResponse = CreateRecipeJobResponse.builder().build();
        doReturn(createRecipeJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(JOB_NAME)
                .outputs(ModelHelper.buildModelOutputs(VALID_BUCKET_OWNER_OUTPUT))
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
        assertThat(response.getResourceModel().getOutputs().size()).isEqualTo(1);
        assertThat(response.getResourceModel().getOutputs().get(0).getLocation().getBucketOwner()).isEqualTo(VALID_BUCKET_OWNER);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessfulCreate_ProfileJob_ValidBucketOwner() {
        final CreateHandler handler = new CreateHandler();
        final CreateProfileJobResponse createProfileJobResponse = CreateProfileJobResponse.builder().build();
        doReturn(createProfileJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .outputLocation(ModelHelper.buildModelOutputLocation(VALID_BUCKET_OWNER_OUTPUT))
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
        assertThat(response.getResourceModel().getOutputLocation().getBucketOwner()).isEqualTo(VALID_BUCKET_OWNER);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailedCreate_RecipeJob_InvalidBucketOwner() {
        doThrow(AccessDeniedException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_RECIPE)
                .name(JOB_NAME)
                .outputs(ModelHelper.buildModelOutputs(INVALID_BUCKET_OWNER_OUTPUT))
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
    public void handleRequest_FailedCreate_ProfileJob_InvalidBucketOwner() {
        doThrow(AccessDeniedException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final CreateHandler handler = new CreateHandler();
        final ResourceModel model = ResourceModel.builder()
                .type(JOB_TYPE_PROFILE)
                .name(JOB_NAME)
                .outputLocation(ModelHelper.buildModelOutputLocation(INVALID_BUCKET_OWNER_OUTPUT))
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

}

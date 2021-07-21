package software.amazon.databrew.job;

import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.DescribeJobResponse;
import software.amazon.awssdk.services.databrew.model.Job;
import software.amazon.awssdk.services.databrew.model.ProfileConfiguration;
import software.amazon.awssdk.services.databrew.model.ResourceNotFoundException;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

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

        Job job = Job.builder()
                .type(TestUtil.JOB_TYPE_PROFILE)
                .name(TestUtil.JOB_NAME)
                .outputs(TestUtil.OUTPUTS)
                .dataCatalogOutputs(TestUtil.DATA_CATALOG_OUTPUT_LIST)
                .jobSample(TestUtil.customRowsModeJobSample())
                .timeout(TestUtil.TIMEOUT)
                .build();

        final DescribeJobResponse describeJobResponse = DescribeJobResponse.builder()
                .type(job.type())
                .name(job.name())
                .jobSample(job.jobSample())
                .outputs(TestUtil.OUTPUTS)
                .dataCatalogOutputs(TestUtil.DATA_CATALOG_OUTPUT_LIST)
                .timeout(job.timeout())
                .build();

        doReturn(describeJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder().build();

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
        TestUtil.assertThatJobModelsAreEqual(response.getResourceModel(), job);
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
    public void handleRequest_FailedRead_ResourceNotFoundException() {
        doThrow(ResourceNotFoundException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ReadHandler handler = new ReadHandler();
        final ResourceModel model = ResourceModel.builder()
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
    public void handleRequest_SuccessfulRead_OutputFormatOptions_Shown() {
        final ReadHandler handler = new ReadHandler();

        Job job = Job.builder()
                .type(TestUtil.JOB_TYPE_RECIPE)
                .name(TestUtil.JOB_NAME)
                .outputs(TestUtil.CSV_OUTPUT_VALID_DELIMITER)
                .build();

        final DescribeJobResponse describeJobResponse = DescribeJobResponse.builder()
                .type(job.type())
                .name(job.name())
                .outputs(TestUtil.CSV_OUTPUT_VALID_DELIMITER)
                .build();

        doReturn(describeJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder().build();

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
        TestUtil.assertThatJobModelsAreEqual(response.getResourceModel(), job);
        assertThat(response.getResourceModel().getOutputs()).isNotNull();
        assertThat(response.getResourceModel().getOutputs().size()).isEqualTo(1);
        assertThat(response.getResourceModel().getOutputs().get(0).getFormatOptions()).isNotNull();
        assertThat(response.getResourceModel().getOutputs().get(0).getFormatOptions().getCsv()).isNotNull();
        assertThat(response.getResourceModel().getOutputs().get(0).getFormatOptions().getCsv().getDelimiter())
                .isEqualTo(TestUtil.PIPE_CSV_DELIMITER);
    }

    @Test
    public void handleRequest_SuccessfulRead_ProfileJobConfiguration() {
        final ReadHandler handler = new ReadHandler();

        ProfileConfiguration configuration = ProfileConfiguration.builder()
                .datasetStatisticsConfiguration(TestUtil.DATASET_STATISTICS_CONFIGURATION)
                .profileColumns(TestUtil.PROFILE_COLUMNS)
                .columnStatisticsConfigurations(TestUtil.COLUMN_STATISTICS_CONFIGURATIONS)
                .build();

        final DescribeJobResponse describeJobResponse = DescribeJobResponse.builder()
                .type(TestUtil.JOB_TYPE_PROFILE)
                .name(TestUtil.JOB_NAME)
                .profileConfiguration(configuration)
                .outputs(TestUtil.CSV_OUTPUT_VALID_DELIMITER)
                .build();

        doReturn(describeJobResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder().build();

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
        assertThat(response.getResourceModel().getProfileConfiguration()).isNotNull();
    }
}

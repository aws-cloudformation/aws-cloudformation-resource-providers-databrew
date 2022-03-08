package software.amazon.databrew.job;

import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.Job;
import software.amazon.awssdk.services.databrew.model.ListJobsResponse;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static software.amazon.databrew.job.TestUtil.assertThatJobModelsAreEqual;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest {

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
        final ListHandler handler = new ListHandler();

        final List<Job> jobs = new ArrayList<>();
        Job job1 = Job.builder()
                .type(TestUtil.JOB_TYPE_PROFILE)
                .name("job1")
                .jobSample(TestUtil.fullDatasetModeJobSample())
                .outputs(TestUtil.OUTPUTS)
                .validationConfigurations(TestUtil.createValidationConfigurations())
                .build();
        Job job2 = Job.builder()
                .type(TestUtil.JOB_TYPE_PROFILE)
                .name("job2")
                .jobSample(TestUtil.customRowsModeJobSample())
                .outputs(TestUtil.OUTPUTS)
                .validationConfigurations(TestUtil.createValidationConfigurations())
                .build();
        // When job sample is empty then it defaults to Mode : CUSTOM_ROWS and Size : 20000
        Job job3 = Job.builder()
                .type(TestUtil.JOB_TYPE_PROFILE)
                .name("job2")
                .outputs(TestUtil.OUTPUTS)
                .validationConfigurations(TestUtil.createValidationConfigurations())
                .build();
        jobs.add(job1);
        jobs.add(job2);
        jobs.add(job3);

        final ListJobsResponse listJobsResponse = ListJobsResponse.builder()
                .jobs(jobs)
                .build();

        doReturn(listJobsResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels().size()).isEqualTo(3);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertThatJobModelsAreEqual(response.getResourceModels().get(0), job1);
        assertThatJobModelsAreEqual(response.getResourceModels().get(1), job2);
        assertThatJobModelsAreEqual(response.getResourceModels().get(2), job2);
    }

    @Test
    public void handleRequest_FailedList_MaxResultsOver() {
        doThrow(DataBrewException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ListHandler handler = new ListHandler();
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
    public void handleRequest_SuccessfulList_WithOutputFormatOptions() {
        final ListHandler handler = new ListHandler();

        final List<Job> jobs = new ArrayList<>();
        Job job = Job.builder()
                .type(TestUtil.JOB_TYPE_RECIPE)
                .name(TestUtil.JOB_NAME)
                .outputs(TestUtil.CSV_OUTPUT_VALID_DELIMITER)
                .dataCatalogOutputs(TestUtil.DATA_CATALOG_OUTPUT_LIST)
                .build();
        Job job2 = Job.builder()
                .type(TestUtil.JOB_TYPE_RECIPE)
                .name("Job2")
                .outputs(TestUtil.CSV_OUTPUT_WITH_MAX_OUTPUT_FILES)
                .build();
        jobs.add(job);
        jobs.add(job2);

        final ListJobsResponse listJobsResponse = ListJobsResponse.builder()
                .jobs(jobs)
                .build();

        doReturn(listJobsResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels().size()).isEqualTo(2);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertThatJobModelsAreEqual(response.getResourceModels().get(0), job);
        assertThatJobModelsAreEqual(response.getResourceModels().get(1), job2);
    }

    @Test
    public void handleRequest_SuccessfulList_WithBucketOwner() {
        final ListHandler handler = new ListHandler();

        final List<Job> jobs = new ArrayList<>();
        Job job = Job.builder()
                .type(TestUtil.JOB_TYPE_RECIPE)
                .name(TestUtil.JOB_NAME)
                .outputs(TestUtil.VALID_BUCKET_OWNER_OUTPUT)
                .build();
        jobs.add(job);

        final ListJobsResponse listJobsResponse = ListJobsResponse.builder()
                .jobs(jobs)
                .build();

        doReturn(listJobsResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels().size()).isEqualTo(1);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertThatJobModelsAreEqual(response.getResourceModels().get(0), job);
    }
}

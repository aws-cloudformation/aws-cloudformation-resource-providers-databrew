package software.amazon.databrew.ruleset;

import software.amazon.awssdk.services.databrew.model.*;
import software.amazon.cloudformation.proxy.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest {

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
        final DeleteHandler handler = new DeleteHandler();
        final DeleteRulesetResponse deleteRulesetResponse = DeleteRulesetResponse.builder()
                .name(TestUtil.RULESET_NAME)
                .build();
        doReturn(deleteRulesetResponse)
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
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailedDelete_ResourceNotFoundException() {
        doThrow(ResourceNotFoundException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final DeleteHandler handler = new DeleteHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.INVALID_RULESET_NAME)
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
    public void handleRequest_FailedDelete_ValidationException() {
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final DeleteHandler handler = new DeleteHandler();
        final ResourceModel model = ResourceModel.builder().build();
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
    public void handleRequest_FailedDelete_ConflictException() {
        doThrow(ConflictException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final DeleteHandler handler = new DeleteHandler();
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
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
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ResourceConflict);
    }

    @Test
    public void handleRequest_FailedDelete_DatabrewException() {
        doThrow(DataBrewException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final DeleteHandler handler = new DeleteHandler();
        final ResourceModel model = ResourceModel.builder().build();
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

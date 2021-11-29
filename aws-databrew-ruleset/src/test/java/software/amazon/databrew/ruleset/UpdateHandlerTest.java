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
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest {

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
        final UpdateHandler handler = new UpdateHandler();
        final UpdateRulesetResponse updateRulesetResponse = UpdateRulesetResponse.builder().name(TestUtil.RULESET_NAME)
                .build();
        doReturn(updateRulesetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .rules(ModelHelper.buildModelRules(TestUtil.createRulesList()))
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
    public void handleRequest_FailedCreateForValidationException() {
        final UpdateHandler handler = new UpdateHandler();
        lenient().doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.INVALID_RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .rules(ModelHelper.buildModelRules(TestUtil.createRulesList()))
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
    public void handleRequest_FailedCreateForConflictException() {
        final UpdateHandler handler = new UpdateHandler();
        doThrow(ConflictException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .rules(ModelHelper.buildModelRules(TestUtil.createRulesList()))
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
    public void handleRequest_FailedCreateForServiceQuotaExceededException() {
        final UpdateHandler handler = new UpdateHandler();
        doThrow(ServiceQuotaExceededException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .rules(ModelHelper.buildModelRules(TestUtil.createRulesList()))
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
    public void handleRequest_FailedCreateForAccessDeniedException() {
        final UpdateHandler handler = new UpdateHandler();
        doThrow(AccessDeniedException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .rules(ModelHelper.buildModelRules(TestUtil.createRulesList()))
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
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AccessDenied);
    }

    @Test
    public void handleRequest_FailedCreateForDatabrewException() {
        final UpdateHandler handler = new UpdateHandler();
        doThrow(DataBrewException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .rules(ModelHelper.buildModelRules(TestUtil.createRulesList()))
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

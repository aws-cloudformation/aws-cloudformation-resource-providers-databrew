package software.amazon.databrew.ruleset;

import software.amazon.awssdk.services.databrew.model.*;
import software.amazon.awssdk.services.databrew.model.Rule;
import software.amazon.cloudformation.proxy.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.databrew.ruleset.TestUtil;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
        final CreateRulesetResponse createRulesetResponse = CreateRulesetResponse.builder().build();
        doReturn(createRulesetResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .targetArn(TestUtil.RULESET_TARGET_ARN)
                .tags(ModelHelper.buildModelTags(TestUtil.sampleTags()))
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
    public void handleRequest_FailedCreateForInvalidRequestError_InvalidTargetArn() {
        final CreateHandler handler = new CreateHandler();
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .targetArn(TestUtil.INVALID_RULESET_TARGET_ARN)
                .tags(ModelHelper.buildModelTags(TestUtil.sampleTags()))
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
    public void handleRequest_FailedCreateForInvalidRequestError_InvalidName() {
        final CreateHandler handler = new CreateHandler();
        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.INVALID_RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .targetArn(TestUtil.RULESET_TARGET_ARN)
                .tags(ModelHelper.buildModelTags(TestUtil.sampleTags()))
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
    public void handleRequest_FailedCreateForInvalidRequestError_InvalidRules() {
        final CreateHandler handler = new CreateHandler();
        lenient().doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .targetArn(TestUtil.RULESET_TARGET_ARN)
                .rules(ModelHelper.buildModelRules(TestUtil.createInvalidRulesList()))
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
    public void handleRequest_FailedCreateForConflictException_RulesetAlreadyExists() {
        final CreateHandler handler = new CreateHandler();
        doThrow(ConflictException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .targetArn(TestUtil.RULESET_TARGET_ARN)
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
        final CreateHandler handler = new CreateHandler();
        doThrow(ServiceQuotaExceededException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .targetArn(TestUtil.RULESET_TARGET_ARN)
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
        final CreateHandler handler = new CreateHandler();
        doThrow(AccessDeniedException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .targetArn(TestUtil.RULESET_TARGET_ARN)
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
        final CreateHandler handler = new CreateHandler();
        doThrow(DataBrewException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());
        final ResourceModel model = ResourceModel.builder()
                .name(TestUtil.RULESET_NAME)
                .description(TestUtil.RULESET_DESCRIPTION)
                .targetArn(TestUtil.RULESET_TARGET_ARN)
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

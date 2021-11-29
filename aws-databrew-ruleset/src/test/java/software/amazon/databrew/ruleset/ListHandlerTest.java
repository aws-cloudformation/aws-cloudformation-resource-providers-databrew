package software.amazon.databrew.ruleset;

import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.ListRulesetsResponse;
import software.amazon.awssdk.services.databrew.model.RulesetItem;
import software.amazon.awssdk.services.databrew.model.ValidationException;
import software.amazon.cloudformation.proxy.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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

        final ResourceModel model = ResourceModel.builder().build();
        final RulesetItem rulesetItem1 = RulesetItem.builder()
                .name(TestUtil.RULESET_NAME_1)
                .description(TestUtil.RULESET_DESCRIPTION)
                .targetArn(TestUtil.RULESET_TARGET_ARN_1)
                .tags(TestUtil.createTags())
                .build();

        final RulesetItem rulesetItem2 = RulesetItem.builder()
                .name(TestUtil.RULESET_NAME_2)
                .description(TestUtil.RULESET_DESCRIPTION)
                .targetArn(TestUtil.RULESET_TARGET_ARN_2)
                .tags(TestUtil.createTags())
                .build();

        final RulesetItem rulesetItem3 = RulesetItem.builder()
                .name(TestUtil.RULESET_NAME_3)
                .description(TestUtil.RULESET_DESCRIPTION)
                .targetArn(TestUtil.RULESET_TARGET_ARN_3)
                .tags(TestUtil.createTags())
                .build();
        List<RulesetItem> rulesetItems = new ArrayList<RulesetItem>();
        rulesetItems.add(rulesetItem1);
        rulesetItems.add(rulesetItem2);
        rulesetItems.add(rulesetItem3);

        final ListRulesetsResponse listRulesetsResponse = ListRulesetsResponse.builder()
                .rulesets(rulesetItems)
                .build();

        doReturn(listRulesetsResponse)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

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
        TestUtil.assertThatRulesetModelsAreEqual(response.getResourceModels().get(0), rulesetItem1);
        TestUtil.assertThatRulesetModelsAreEqual(response.getResourceModels().get(1), rulesetItem2);
        TestUtil.assertThatRulesetModelsAreEqual(response.getResourceModels().get(2), rulesetItem3);

    }

    @Test
    public void handleRequest_FailedRequest_ValidationException() {
        final ListHandler handler = new ListHandler();

        final ResourceModel model = ResourceModel.builder()
                .targetArn(TestUtil.INVALID_RULESET_TARGET_ARN)
                .build();

        doThrow(ValidationException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);

    }

    @Test
    public void handleRequest_FailedRequest_DatabrewException() {
        final ListHandler handler = new ListHandler();

        final ResourceModel model = ResourceModel.builder()
                .targetArn(TestUtil.RULESET_TARGET_ARN)
                .build();

        doThrow(DataBrewException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);

    }
}

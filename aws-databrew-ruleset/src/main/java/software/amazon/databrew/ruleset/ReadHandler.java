package software.amazon.databrew.ruleset;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.DescribeRulesetResponse;
import software.amazon.awssdk.services.databrew.model.DescribeRulesetRequest;
import software.amazon.awssdk.services.databrew.model.ResourceNotFoundException;
import software.amazon.awssdk.services.databrew.model.ValidationException;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.cloudformation.proxy.*;

import java.util.ArrayList;


public class ReadHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        final String rulesetName = model.getName();
        final DataBrewClient databrewClient = ClientBuilder.getClient();
        final DescribeRulesetRequest describeRulesetRequest = DescribeRulesetRequest.builder()
                .name(rulesetName)
                .build();

        final DescribeRulesetResponse describeRulesetResponse;
        try {
            describeRulesetResponse = proxy.injectCredentialsAndInvokeV2(describeRulesetRequest, databrewClient::describeRuleset);
            logger.log(String.format("%s [%s] Read Successfully", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, rulesetName));
            software.amazon.databrew.ruleset.ResourceModel resultModel = ModelHelper.constructModel(describeRulesetResponse);
            return ProgressEvent.<software.amazon.databrew.ruleset.ResourceModel, software.amazon.databrew.ruleset.CallbackContext>builder()
                    .resourceModel(resultModel)
                    .status(OperationStatus.SUCCESS)
                    .build();

        } catch (ResourceNotFoundException ex) {
            logger.log(String.format("%s [%s] Does Not Exist", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, rulesetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.NotFound);
        } catch (ValidationException ex) {
            logger.log(String.format("%s [%s] Invalid Parameter", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, rulesetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.InvalidRequest);
        } catch (DataBrewException ex) {
            logger.log(String.format("%s [%s] Read Failed", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, rulesetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }
    }
}

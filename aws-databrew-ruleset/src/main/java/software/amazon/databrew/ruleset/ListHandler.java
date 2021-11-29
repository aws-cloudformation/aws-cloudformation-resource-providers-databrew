package software.amazon.databrew.ruleset;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.ListRulesetsRequest;
import software.amazon.awssdk.services.databrew.model.ListRulesetsResponse;
import software.amazon.awssdk.services.databrew.model.ValidationException;
import software.amazon.cloudformation.proxy.*;

import java.util.List;
import java.util.ArrayList;


public class ListHandler extends BaseHandler<CallbackContext> {

    private static final int MAX_RESULTS = 100;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        final List<ResourceModel> models = new ArrayList<>();

        final DataBrewClient databrewClient = ClientBuilder.getClient();

        final ListRulesetsRequest listRulesetsRequest = ListRulesetsRequest.builder()
                .targetArn(model.getTargetArn())
                .maxResults(MAX_RESULTS)
                .nextToken(request.getNextToken())
                .build();

        final ListRulesetsResponse listRulesetsResponse;
        try {
            listRulesetsResponse = proxy.injectCredentialsAndInvokeV2(listRulesetsRequest, databrewClient::listRulesets);
            logger.log(String.format("%s List Successfully", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME));

            List<software.amazon.databrew.ruleset.ResourceModel> outputModels = new ArrayList<>();
            if (listRulesetsResponse.rulesets() != null) {
                listRulesetsResponse.rulesets().forEach(ruleset ->{
                    software.amazon.databrew.ruleset.ResourceModel outputModel = ModelHelper.constructModel(ruleset);
                    outputModels.add(outputModel);
                });
            }
            return ProgressEvent.<software.amazon.databrew.ruleset.ResourceModel, software.amazon.databrew.ruleset.CallbackContext>builder()
                    .resourceModels(outputModels)
                    .nextToken(request.getNextToken())
                    .status(OperationStatus.SUCCESS)
                    .build();

        } catch (ValidationException ex) {
            logger.log(String.format("%s Invalid Parameter, List failed", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.InvalidRequest);
        } catch (DataBrewException ex) {
            logger.log(String.format("%s List Failed", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }

    }
}

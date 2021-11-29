package software.amazon.databrew.dataset;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.cloudformation.LambdaWrapper;

import java.time.Duration;

public class ClientBuilder {
    private static DataBrewClient client;

    public static DataBrewClient getClient() {
        if (client == null) {
            client = DataBrewClient.builder()
                    .httpClient(LambdaWrapper.HTTP_CLIENT)
                    .overrideConfiguration(
                            ClientOverrideConfiguration.builder()
                                    .apiCallAttemptTimeout(Duration.ofSeconds(28))
                                    .build()
                    )
                    .build();
        }

        return client;
    }
}

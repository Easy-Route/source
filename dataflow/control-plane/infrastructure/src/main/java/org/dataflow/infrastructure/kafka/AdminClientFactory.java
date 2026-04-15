package org.dataflow.infrastructure.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class AdminClientFactory {

    @Bean(destroyMethod = "close")
    public AdminClient adminClient(KafkaProperties props) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, props.requestTimeoutMs());
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "dataflow-control-plane");
        if (!"PLAINTEXT".equalsIgnoreCase(props.securityProtocol())) {
            config.put("security.protocol", props.securityProtocol());
            if (props.saslMechanism() != null) {
                config.put("sasl.mechanism", props.saslMechanism());
            }
            if (props.saslJaasConfig() != null) {
                config.put("sasl.jaas.config", props.saslJaasConfig());
            }
        }
        return AdminClient.create(config);
    }
}

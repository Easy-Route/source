package org.dataflow.infrastructure.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dataflow.domain.port.dataplane.KafkaConnectClient;
import org.dataflow.domain.status.ComponentHealth;
import org.dataflow.domain.status.ComponentKind;
import org.dataflow.domain.status.ComponentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClient;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
@EnableConfigurationProperties(KafkaConnectProperties.class)
public class KafkaConnectRestClient implements KafkaConnectClient {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectRestClient.class);

    private final RestClient http;
    private final ObjectMapper mapper;

    public KafkaConnectRestClient(KafkaConnectProperties props, ObjectMapper mapper) {
        this.mapper = mapper;
        this.http = RestClient.builder()
                .baseUrl(props.url())
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    @Override
    public void registerConnector(String name, Map<String, String> config) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("name", name);
        ObjectNode cfg = payload.putObject("config");
        config.forEach(cfg::put);
        try {
            http.post()
                    .uri("/connectors")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(payload)
                    .retrieve()
                    .toBodilessEntity();
            log.info("Connector registered: {}", name);
        } catch (HttpStatusCodeException e) {
            if (e.getStatusCode().value() == 409) {
                updateConnectorConfig(name, config);
                return;
            }
            throw new KafkaConnectException("Failed to register connector " + name, e);
        }
    }

    @Override
    public void updateConnectorConfig(String name, Map<String, String> config) {
        try {
            http.put()
                    .uri("/connectors/{name}/config", name)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(config)
                    .retrieve()
                    .toBodilessEntity();
            log.info("Connector config updated: {}", name);
        } catch (HttpStatusCodeException e) {
            throw new KafkaConnectException("Failed to update connector config " + name, e);
        }
    }

    @Override
    public void pauseConnector(String name) {
        callMethod(HttpMethod.PUT, "/connectors/{name}/pause", name);
    }

    @Override
    public void resumeConnector(String name) {
        callMethod(HttpMethod.PUT, "/connectors/{name}/resume", name);
    }

    @Override
    public void unregisterConnector(String name) {
        try {
            http.delete().uri("/connectors/{name}", name).retrieve().toBodilessEntity();
            log.info("Connector unregistered: {}", name);
        } catch (HttpStatusCodeException e) {
            if (e.getStatusCode().value() == 404) {
                return;
            }
            throw new KafkaConnectException("Failed to unregister connector " + name, e);
        }
    }

    @Override
    public Optional<ComponentStatus> connectorStatus(String name) {
        try {
            ResponseEntity<JsonNode> resp = http.get()
                    .uri("/connectors/{name}/status", name)
                    .retrieve()
                    .toEntity(JsonNode.class);
            JsonNode body = resp.getBody();
            if (body == null) {
                return Optional.empty();
            }
            String state = body.path("connector").path("state").asText("UNKNOWN");
            Map<String, String> attrs = new HashMap<>();
            attrs.put("worker_id", body.path("connector").path("worker_id").asText(""));
            attrs.put("type", body.path("type").asText(""));
            return Optional.of(new ComponentStatus(
                    ComponentKind.DEBEZIUM_CONNECTOR,
                    name,
                    mapHealth(state),
                    attrs,
                    Instant.now()
            ));
        } catch (HttpStatusCodeException e) {
            if (e.getStatusCode().value() == 404) {
                return Optional.empty();
            }
            throw new KafkaConnectException("Failed to fetch connector status " + name, e);
        }
    }

    @Override
    public List<String> listConnectors() {
        ResponseEntity<String[]> resp = http.get()
                .uri("/connectors")
                .retrieve()
                .toEntity(String[].class);
        return resp.getBody() == null ? List.of() : List.of(resp.getBody());
    }

    private void callMethod(HttpMethod method, String uri, Object... args) {
        try {
            http.method(method).uri(uri, args).retrieve().toBodilessEntity();
        } catch (HttpStatusCodeException e) {
            throw new KafkaConnectException("Connect call " + method + " " + uri + " failed", e);
        }
    }

    private ComponentHealth mapHealth(String state) {
        return switch (state) {
            case "RUNNING" -> ComponentHealth.RUNNING;
            case "PAUSED" -> ComponentHealth.PAUSED;
            case "FAILED" -> ComponentHealth.FAILED;
            case "UNASSIGNED" -> ComponentHealth.PENDING;
            default -> ComponentHealth.UNKNOWN;
        };
    }
}

package org.dataflow.infrastructure.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dataflow.domain.port.dataplane.FlinkJobClient;
import org.dataflow.domain.status.ComponentHealth;
import org.dataflow.domain.status.ComponentKind;
import org.dataflow.domain.status.ComponentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClient;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
@EnableConfigurationProperties(FlinkProperties.class)
public class FlinkRestClient implements FlinkJobClient {

    private static final Logger log = LoggerFactory.getLogger(FlinkRestClient.class);

    private final RestClient http;
    private final ObjectMapper mapper;

    public FlinkRestClient(FlinkProperties props, ObjectMapper mapper) {
        this.mapper = mapper;
        this.http = RestClient.builder()
                .baseUrl(props.url())
                .build();
    }

    @Override
    public String uploadJar(byte[] jarBytes, String filename) {
        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("jarfile", new ByteArrayResource(jarBytes) {
            @Override
            public String getFilename() {
                return filename;
            }
        });
        try {
            ResponseEntity<JsonNode> resp = http.post()
                    .uri("/jars/upload")
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .body(body)
                    .retrieve()
                    .toEntity(JsonNode.class);
            JsonNode root = resp.getBody();
            if (root == null) {
                throw new FlinkRestException("Empty response from /jars/upload", null);
            }
            String jarFilename = root.path("filename").asText();
            int slash = jarFilename.lastIndexOf('/');
            String jarId = slash >= 0 ? jarFilename.substring(slash + 1) : jarFilename;
            log.info("Uploaded jar: {}", jarId);
            return jarId;
        } catch (HttpStatusCodeException e) {
            throw new FlinkRestException("Failed to upload jar " + filename, e);
        }
    }

    @Override
    public boolean jarExists(String jarFilename) {
        try {
            ResponseEntity<JsonNode> resp = http.get().uri("/jars").retrieve().toEntity(JsonNode.class);
            JsonNode files = resp.getBody() == null ? null : resp.getBody().path("files");
            if (files == null || !files.isArray()) {
                return false;
            }
            for (JsonNode f : files) {
                if (jarFilename.equals(f.path("name").asText())) {
                    return true;
                }
            }
            return false;
        } catch (HttpStatusCodeException e) {
            return false;
        }
    }

    @Override
    public String submitJob(String jarId, JobSubmission submission) {
        return submitInternal(jarId, submission, null);
    }

    @Override
    public String submitJobFromSavepoint(String jarId, JobSubmission submission, String savepointPath) {
        return submitInternal(jarId, submission, savepointPath);
    }

    private String submitInternal(String jarId, JobSubmission submission, String savepointPath) {
        ObjectNode payload = mapper.createObjectNode();
        if (submission.entryClass() != null) {
            payload.put("entryClass", submission.entryClass());
        }
        if (submission.programArgs() != null) {
            payload.put("programArgsList", submission.programArgs());
        }
        payload.put("parallelism", submission.parallelism());
        if (savepointPath != null) {
            payload.put("savepointPath", savepointPath);
            payload.put("allowNonRestoredState", false);
        }
        try {
            ResponseEntity<JsonNode> resp = http.post()
                    .uri("/jars/{jarid}/run", jarId)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(payload)
                    .retrieve()
                    .toEntity(JsonNode.class);
            String jobId = resp.getBody() == null ? null : resp.getBody().path("jobid").asText();
            log.info("Submitted Flink job {} (jar {})", jobId, jarId);
            return jobId;
        } catch (HttpStatusCodeException e) {
            throw new FlinkRestException("Failed to submit Flink job from " + jarId, e);
        }
    }

    @Override
    public void cancelJob(String jobId) {
        try {
            http.patch().uri("/jobs/{jid}", jobId).retrieve().toBodilessEntity();
            log.info("Cancelled Flink job {}", jobId);
        } catch (HttpStatusCodeException e) {
            if (e.getStatusCode().value() == 404) {
                return;
            }
            throw new FlinkRestException("Failed to cancel job " + jobId, e);
        }
    }

    @Override
    public String triggerSavepoint(String jobId, String savepointDir) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("target-directory", savepointDir);
        payload.put("cancel-job", false);
        return triggerSavepointInternal(jobId, payload);
    }

    @Override
    public String stopWithSavepoint(String jobId, String savepointDir) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("targetDirectory", savepointDir);
        payload.put("drain", false);
        try {
            ResponseEntity<JsonNode> resp = http.post()
                    .uri("/jobs/{jid}/stop", jobId)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(payload)
                    .retrieve()
                    .toEntity(JsonNode.class);
            return resp.getBody() == null ? null : resp.getBody().path("request-id").asText();
        } catch (HttpStatusCodeException e) {
            throw new FlinkRestException("Failed to stop with savepoint " + jobId, e);
        }
    }

    private String triggerSavepointInternal(String jobId, ObjectNode payload) {
        try {
            ResponseEntity<JsonNode> resp = http.post()
                    .uri("/jobs/{jid}/savepoints", jobId)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(payload)
                    .retrieve()
                    .toEntity(JsonNode.class);
            return resp.getBody() == null ? null : resp.getBody().path("request-id").asText();
        } catch (HttpStatusCodeException e) {
            throw new FlinkRestException("Failed to trigger savepoint for " + jobId, e);
        }
    }

    @Override
    public Optional<ComponentStatus> jobStatus(String jobId) {
        try {
            ResponseEntity<JsonNode> resp = http.get()
                    .uri("/jobs/{jid}", jobId)
                    .retrieve()
                    .toEntity(JsonNode.class);
            JsonNode body = resp.getBody();
            if (body == null) {
                return Optional.empty();
            }
            String state = body.path("state").asText("UNKNOWN");
            Map<String, String> attrs = new HashMap<>();
            attrs.put("name", body.path("name").asText(""));
            attrs.put("startTime", Long.toString(body.path("start-time").asLong(0L)));
            attrs.put("duration", Long.toString(body.path("duration").asLong(0L)));
            return Optional.of(new ComponentStatus(
                    ComponentKind.FLINK_JOB, jobId, mapHealth(state), attrs, Instant.now()));
        } catch (HttpStatusCodeException e) {
            if (e.getStatusCode().value() == 404) {
                return Optional.empty();
            }
            throw new FlinkRestException("Failed to fetch job status " + jobId, e);
        }
    }

    @Override
    public List<JobMetric> jobMetrics(String jobId, List<String> metricNames) {
        String filter = String.join(",", metricNames);
        try {
            ResponseEntity<JsonNode> resp = http.get()
                    .uri("/jobs/{jid}/metrics?get={names}", jobId, filter)
                    .retrieve()
                    .toEntity(JsonNode.class);
            JsonNode body = resp.getBody();
            if (body == null || !body.isArray()) {
                return List.of();
            }
            List<JobMetric> out = new ArrayList<>(body.size());
            for (JsonNode m : body) {
                out.add(new JobMetric(m.path("id").asText(), m.path("value").asText("")));
            }
            return out;
        } catch (HttpStatusCodeException e) {
            return List.of();
        }
    }

    private ComponentHealth mapHealth(String state) {
        return switch (state) {
            case "RUNNING" -> ComponentHealth.RUNNING;
            case "CREATED", "INITIALIZING", "RECONCILING" -> ComponentHealth.PENDING;
            case "SUSPENDED" -> ComponentHealth.PAUSED;
            case "FAILED", "FAILING" -> ComponentHealth.FAILED;
            case "CANCELED", "CANCELLING", "FINISHED" -> ComponentHealth.GONE;
            default -> ComponentHealth.UNKNOWN;
        };
    }
}

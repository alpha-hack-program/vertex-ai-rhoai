package com.redhat.docbot;

import java.util.Optional;

import javax.naming.ConfigurationException;

import org.apache.camel.builder.RouteBuilder;

import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;

@ApplicationScoped 
public class GSEventListener extends RouteBuilder {

    @Inject
    @ConfigProperty(name = "bucket.name")
    String bucketName;

    @Inject
    @ConfigProperty(name = "kfp.pipeline.namespace")
    String kfpNamespace;

    @Inject
    @ConfigProperty(name = "kfp.pipeline.display-name")
    String pipelineDisplayName;

    @Inject
    @RestClient
    KubeflowPipelineClient kfpClient;

    private String pipelineId;

    @PostConstruct
    void init() throws ConfigurationException {
        try {
            fetchPipelineId();
        } catch (Exception e) {
            log.error("Failed to fetch pipeline ID", e);
            throw new ConfigurationException("Failed to fetch pipeline ID");
        }
    }

    @Override
    public void configure() throws Exception {
        from("google-storage://{{bucket.name}}?autoCreateBucket=false&moveAfterRead=false")
            .routeId("gcs-listener")
            .log("Detected file: ${header.CamelGoogleCloudStorageObjectName}")
            .process(exchange -> {
                String fileName = exchange.getIn().getHeader("CamelGoogleCloudStorageObjectName", String.class);
                if ("model.onnx".equals(fileName)) {
                    log.info("model.onnx detected, triggering Kubeflow Pipeline");
                    runPipeline();
                }
            });
    }

    private void fetchPipelineId() {
        Response response = kfpClient.getPipelines();
        if (response.getStatus() != 200) {
            throw new RuntimeException("Failed to fetch pipelines from Kubeflow");
        }

        PipelineResponse pipelineResponse = response.readEntity(PipelineResponse.class);
        log.info("Pipeline list: " + pipelineResponse.getPipelines());
        Optional<Pipeline> pipelineOpt = pipelineResponse.getPipelines().stream()
            .filter(p -> pipelineDisplayName.equals(p.getDisplayName()))
            .findFirst();

        if (pipelineOpt.isPresent()) {
            this.pipelineId = pipelineOpt.get().getPipelineId();
            log.info("Pipeline ID for display name " + pipelineDisplayName + ": " + this.pipelineId);
        } else {
            throw new RuntimeException("Pipeline with display name " + pipelineDisplayName + " not found");
        }
    }

    private void runPipeline() {
        PipelineSpec pipelineSpec = new PipelineSpec();
        pipelineSpec.setDisplayName(pipelineDisplayName + "_run");
        pipelineSpec.setDescription("This is run from Camel route");
        RuntimeConfig runtimeConfig = new RuntimeConfig();
        // runtimeConfig.setParameters(Map.of("param1", "value1"));
        pipelineSpec.setRuntimeConfig(runtimeConfig);
        PipelineVersionReference pipelineVersionReference = new PipelineVersionReference();
        pipelineVersionReference.setPipelineId(this.pipelineId);
        pipelineSpec.setPipelineVersionReference(pipelineVersionReference);

        Response response = kfpClient.runPipeline(pipelineSpec);
        if (response.getStatus() != 200) {
            throw new RuntimeException("Failed to run pipeline " + this.pipelineId + " in Kubeflow");
        }

        String stringResponse = response.readEntity(String.class);
        log.info("Pipeline Run: " +stringResponse);
    }
}

package it.unibz.inf.ontop.rdf4j.predefined;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Optional;

public class PredefinedConfig {

    /**
     * TODO: enforce required (shall we use @JsonCreator?)
     */
    public static class QueryEntry {
        @JsonProperty(value = "sparqlQueryType", required = true)
        private String sparqlQueryType;
        @JsonProperty(value = "name", required = false)
        private String name;
        @JsonProperty(value = "description", required = false)
        private String description;
        @JsonProperty(value = "context", required = false)
        private Object context;
        @JsonProperty(value = "outputContext", required = false)
        private Object outputContext;
        @JsonProperty(value = "frame", required = false)
        private Map<String, Object> frame;
        @JsonProperty(value = "parameters", required = true)
        private Map<String, QueryParameter> parameters;

        public String getSparqlQueryType() {
            return sparqlQueryType;
        }

        public Optional<String> getName() {
            return Optional.ofNullable(name);
        }

        public Optional<String> getDescription() {
            return Optional.ofNullable(description);
        }

        public Optional<Object> getContext() {
            return Optional.ofNullable(context);
        }

        public Optional<Object> getOutputContext() {
            return Optional.ofNullable(outputContext);
        }

        public Optional<Map<String, Object>> getFrame() {
            return Optional.ofNullable(frame);
        }

        public Map<String, QueryParameter> getParameters() {
            return parameters;
        }
    }

    /**
     * TODO: enforce required (shall we use @JsonCreator?)
     */
    public static class QueryParameter {
        @JsonProperty(value = "description", required = false)
        private String description;
        @JsonProperty(value = "type", required = true)
        private String type;
        @JsonProperty(value = "safeForRandomGeneration", required = true)
        private Boolean safeForRandomGeneration;
        @JsonProperty(value = "required", required = true)
        private Boolean required;

        public Optional<String> getDescription() {
            return Optional.ofNullable(description);
        }

        public String getType() {
            return type;
        }

        public Boolean getSafeForRandomGeneration() {
            return safeForRandomGeneration;
        }

        public Boolean getRequired() {
            return required;
        }
    }
}

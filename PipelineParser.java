package com.apple.aml.stargate.pipeline.parser;

import com.apple.aml.stargate.beam.sdk.metrics.HubbleMetricsSink;
import com.apple.aml.stargate.beam.sdk.options.StargateOptions;
import com.apple.aml.stargate.beam.sdk.ts.ErrorInterceptor;
import com.apple.aml.stargate.beam.sdk.utils.ErrorPayloadConverterFns;
import com.apple.aml.stargate.beam.sdk.values.SCollection;
import com.apple.aml.stargate.common.constants.A3Constants;
import com.apple.aml.stargate.common.constants.CommonConstants;
import com.apple.aml.stargate.common.constants.PipelineConstants;
import com.apple.aml.stargate.common.exceptions.InvalidInputException;
import com.apple.aml.stargate.common.nodes.StargateNode;
import com.apple.aml.stargate.common.nodes.StargatePipeline;
import com.apple.aml.stargate.common.options.ErrorOptions;
import com.apple.aml.stargate.common.pojo.ErrorRecord;
import com.apple.aml.stargate.common.utils.A3Utils;
import com.apple.aml.stargate.common.utils.AppConfig;
import static com.apple.aml.stargate.common.utils.SchemaUtils.parseSchema;
import com.apple.aml.stargate.common.utils.WebUtils;
import com.apple.aml.stargate.common.web.clients.AppConfigClient;
import com.apple.aml.stargate.flink.functions.ErrorToGenericRecordMapperFunction;
import com.apple.aml.stargate.flink.streaming.api.EnrichedDataStream;
import com.apple.aml.stargate.flink.utils.FlinkErrorUtils;
import com.apple.aml.stargate.flink.utils.FlinkUtils;
import com.apple.aml.stargate.pipeline.pojo.BeamEdge;
import com.apple.aml.stargate.pipeline.pojo.BeamVertx;
import com.apple.aml.stargate.pipeline.pojo.FlinkEdge;
import com.apple.aml.stargate.pipeline.pojo.FlinkVertx;
import com.apple.aml.stargate.pipeline.pojo.Vertx;
import com.apple.aml.stargate.pipeline.sdk.ts.BaseErrorPayloadConverter;
import com.apple.aml.stargate.pipeline.sdk.utils.GenericTransform;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.parse.Parser;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.metrics.MetricsSink;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.slf4j.Logger;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.beam.sdk.utils.BeamUtils.registerDefaultCoders;
import static com.apple.aml.stargate.common.constants.A3Constants.APP_DS_PASSWORD;
import static com.apple.aml.stargate.common.constants.CommonConstants.CONFIG_PARSE_OPTIONS_JSON;
import static com.apple.aml.stargate.common.constants.CommonConstants.CONFIG_RENDER_OPTIONS_CONCISE;
import static com.apple.aml.stargate.common.constants.CommonConstants.EMPTY_STRING;
import static com.apple.aml.stargate.common.constants.CommonConstants.FORMATTER_DATE_1;
import static com.apple.aml.stargate.common.constants.CommonConstants.FORMATTER_DATE_TIME_1;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_A3_TOKEN;
import static com.apple.aml.stargate.common.constants.CommonConstants.HEADER_CLIENT_APP_ID;
import static com.apple.aml.stargate.common.constants.CommonConstants.OfsConstants.APP_IGNITE_DISCOVERY_TYPE;
import static com.apple.aml.stargate.common.constants.PipelineConstants.DirectoryPaths.CONNECT_ID_PROP_FILE_NAME;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.APPCONFIG_ENVIRONMENT;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.APPCONFIG_MODULE_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.APP_LOG_OVERRIDES_FILE_PATH;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_BASE_SHARED_DIRECTORY;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_CONNECT_BACKEND;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_HUBBLE_METRICS_ENABLED;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_OPERATOR_URI;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_SHARED_DIRECTORY;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.STARGATE_SHARED_TOKEN;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.pipelineId;
import static com.apple.aml.stargate.common.constants.PipelineConstants.EnvironmentVariables.sharedToken;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_APP_ID;
import static com.apple.aml.stargate.common.constants.PipelineConstants.HEADER_PIPELINE_SHARED_TOKEN;
import static com.apple.aml.stargate.common.constants.PipelineConstants.NODE_TYPE.ErrorNode;
import static com.apple.aml.stargate.common.utils.AppConfig.appId;
import static com.apple.aml.stargate.common.utils.AppConfig.env;
import static com.apple.aml.stargate.common.utils.AppConfig.environment;
import static com.apple.aml.stargate.common.utils.AppConfig.mode;
import static com.apple.aml.stargate.common.utils.ClassUtils.cascadeProperties;
import static com.apple.aml.stargate.common.utils.ClassUtils.duplicate;
import static com.apple.aml.stargate.common.utils.ClassUtils.getAs;
import static com.apple.aml.stargate.common.utils.ClassUtils.parseBoolean;
import static com.apple.aml.stargate.common.utils.CsvUtils.readSimpleSchema;
import static com.apple.aml.stargate.common.utils.FormatUtils.parseDateTime;
import static com.apple.aml.stargate.common.utils.HoconUtils.appendHoconToBuilder;
import static com.apple.aml.stargate.common.utils.HoconUtils.hoconString;
import static com.apple.aml.stargate.common.utils.HoconUtils.hoconToMap;
import static com.apple.aml.stargate.common.utils.HoconUtils.hoconToPojo;
import static com.apple.aml.stargate.common.utils.JsonUtils.isRedactable;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.nonNullMap;
import static com.apple.aml.stargate.common.utils.JsonUtils.pojoToProperties;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.JsonUtils.readNullableJsonMap;
import static com.apple.aml.stargate.common.utils.JsonUtils.yamlToMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.LogbackUtils.reloadLogbackLogger;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.saveLocalSchema;
import static com.apple.aml.stargate.pipeline.sdk.utils.PipelineUtils.sparkUtilsClass;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static com.typesafe.config.ConfigRenderOptions.concise;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.Arrays.asList;
import static java.util.Base64.getDecoder;

@SuppressWarnings({"rawtypes"})
public final class PipelineParser {
    public static final String DEFAULT_DAG_NAME = "default";
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());
    private static final String LOCAL_SG_BASE_PATH = System.getProperty("user.home") + "/.stargate-pipeline";
    private static final Set<String> IGNORE_EXTENSIONS = new HashSet<>(asList("xml", "json", "jks", "pem", "txt", "yml", "yaml", "conf", "properties", "ini"));

    private PipelineParser() {
    }

    @SuppressWarnings("unchecked")
    public static Pair<PipelineOptions, StargatePipeline> fetch(final String[] args) throws Exception {
        StargateOptions sOptions = PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().as(StargateOptions.class);
        String pipelineId = pipelineId();
        Map<String, String> properties;
        Map<String, String> dslContentMap = new HashMap<>();
        if (sOptions.getPipelineToken() != null) {
            sOptions.setPipelineToken(sOptions.getPipelineToken().replaceAll("'", EMPTY_STRING).replaceAll("\"", EMPTY_STRING).trim());
            if (sOptions.getPipelineToken().isBlank()) sOptions.setPipelineToken(null);
        }
        List<String> dslExtentions = asList("yml", "yaml", "conf", "txt", "cnf", "json");
        appendPipelineIdToLog(pipelineId, sOptions.getPipelineRunNo() == null ? 0 : sOptions.getPipelineRunNo());
        if (sOptions.getConfigPath() != null && !sOptions.getConfigPath().trim().isBlank() && Paths.get(sOptions.getConfigPath()).toFile().exists() && Paths.get(sOptions.getConfigPath()).toFile().isFile()) {
            File configFile = Paths.get(sOptions.getConfigPath()).toFile().getAbsoluteFile();
            String configFilePath = configFile.getAbsolutePath();
            String extension = configFilePath.substring(configFilePath.lastIndexOf('.') + 1).toLowerCase();
            if (!dslExtentions.stream().anyMatch(x -> x.equals(extension)))
                throw new InvalidInputException("Not a valid config file extension. Valid extensions are ", dslExtentions);
            String dslString = Files.readString(Paths.get(sOptions.getConfigPath()));
            if (extension.equalsIgnoreCase("yml") || extension.equalsIgnoreCase("yaml")) {
                try {
                    Map<String, Object> map = yamlToMap(dslString);
                    if (map.containsKey("kind") && CommonConstants.K8sOperator.KIND.equalsIgnoreCase(map.get("kind").toString())) {
                        Map<String, Object> spec = (Map<String, Object>) map.get("spec");
                        Map<String, Object> definition = (Map<String, Object>) spec.get("definition");
                        if (definition == null || definition.isEmpty()) {
                            dslString = (String) spec.get("spec");
                        } else {
                            dslString = jsonString(definition);
                        }
                    }
                } catch (Exception ignored) {

                }
            } else {
            }
            String dslContentType = detectDSLContentType(dslString);
            if (!isBlank(sOptions.getPipelineProfile())) {
                Path profileFilePath = Paths.get(configFilePath.substring(0, configFilePath.length() - (1 + extension.length())) + "-" + sOptions.getPipelineProfile().toLowerCase() + "." + extension);
                if (profileFilePath.toFile().exists() && profileFilePath.toFile().isFile()) {
                    String profileOverride = Files.readString(profileFilePath);
                    if (isBlank(profileOverride)) {
                        sOptions.setPipelineProfile(null);
                    } else {
                        dslString = mergeDSLContent(dslString, profileOverride);
                        dslContentType = "json";
                    }
                } else {
                    sOptions.setPipelineProfile(null);
                }
            } else {
                sOptions.setPipelineProfile(null);
            }
            dslContentMap.put(dslContentType, dslString);
            properties = new HashMap<>();
        } else {
            String baseUrl = env(STARGATE_OPERATOR_URI, environment().getConfig().getStargateUri());
            Map<String, String> headers = operatorHeaders();
            dslContentMap.put(EMPTY_STRING, WebUtils.getData(String.format("%s/sg/pipeline/spec/%s", baseUrl, pipelineId), headers, String.class, true));
            Map remoteProps = WebUtils.getData(String.format("%s/sg/pipeline/props/%s", baseUrl, pipelineId), headers, Map.class, false);
            properties = remoteProps == null ? new HashMap<>() : new HashMap<>(remoteProps);
        }
        Map<String, String> logLevels = new HashMap<>();
        Arrays.stream(args).filter(x -> (x != null && x.trim().toLowerCase().startsWith("--loglevel."))).map(x -> x.trim().substring(11).split("=")).forEach(p -> {
            String key = String.valueOf(p[0]).trim();
            if (key.isBlank() || p.length == 1) {
                return;
            }
            String value = String.valueOf(p[1]);
            if (value.length() > 2 && ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'")))) {
                value = value.substring(1, value.length() - 1);
            }
            logLevels.put(key, value);
        });
        if (!logLevels.isEmpty() && !sOptions.isDryRun()) {
            String logPath = env(APP_LOG_OVERRIDES_FILE_PATH, null);
            if (!isBlank(logPath)) {
                Path path = Paths.get(logPath);
                path.getParent().toFile().mkdirs();
                String logString = logLevels.entrySet().stream().map(e -> String.format("<logger name=\"%s\" level=\"%s\"/>", e.getKey(), e.getValue().toUpperCase())).collect(Collectors.joining("\n"));
                String fileContent = String.format("<included>\n%s\n</included>", logString);
                Files.writeString(Paths.get(logPath), fileContent, CREATE, TRUNCATE_EXISTING);
                reloadLogbackLogger();
            }
        }
        Arrays.stream(args).filter(x -> (x != null && x.trim().startsWith("--stargate."))).map(x -> x.trim().substring(11).split("=")).forEach(p -> {
            String key = String.valueOf(p[0]).trim();
            if (key.isBlank() || p.length == 1) {
                return;
            }
            String value = String.valueOf(p[1]);
            if (value.length() > 2 && ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'")))) {
                value = value.substring(1, value.length() - 1);
            }
            properties.put(key, value);
        });
        properties.put("environment", mode());
        properties.put("ENV", mode().toLowerCase());
        Pattern replacePattern = Pattern.compile("\\#\\{([^}]+)\\}");
        Set<String> dslTokens = new HashSet<>();
        dslContentMap.keySet().stream().forEach(type -> dslTokens.addAll(replacePattern.matcher(dslContentMap.get(type)).results().map(result -> result.group(1)).collect(Collectors.toList())));
        Map<String, String> tokenValues = new HashMap<>();
        for (final String token : dslTokens) {
            String value = properties.get(token);
            if (value != null) {
                tokenValues.put(token, value);
                continue;
            }
            value = System.getProperty(token);
            if (value != null) {
                tokenValues.put(token, value);
                continue;
            }
            value = System.getenv(token);
            tokenValues.put(token, value == null ? "#{" + token + "}" : value);
        }
        StringBuilder dslBuilder = new StringBuilder();
        Pattern connectIdPattern = Pattern.compile("(((writer|reader|transformer|(nodes\\.[A-z0-9-_]+))([\\.0-9]+))connectId(s?))");
        Map<String, List<String>> connectIdsMap = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : dslContentMap.entrySet()) {
            String content = entry.getValue();
            for (Map.Entry<String, String> tokenEntry : tokenValues.entrySet()) {
                content = content.replaceAll("\\#\\{" + tokenEntry.getKey() + "\\}", tokenEntry.getValue());
            }
            String extension = entry.getKey().toLowerCase();
            if (isBlank(extension)) extension = detectDSLContentType(content);
            Config config;
            if ("json".equals(extension)) {
                config = ConfigFactory.parseString(content, CONFIG_PARSE_OPTIONS_JSON);
                dslBuilder.append(config.root().render(CONFIG_RENDER_OPTIONS_CONCISE));
            } else if ("yaml".equals(extension) || "yml".equals(extension)) {
                config = ConfigFactory.parseString(jsonString(yamlToMap(content)), CONFIG_PARSE_OPTIONS_JSON);
                dslBuilder.append(config.root().render(CONFIG_RENDER_OPTIONS_CONCISE));
            } else {
                config = ConfigFactory.parseString(content);
                dslBuilder.append(content);
            }
            Map<String, Object> props = pojoToProperties(config.root().unwrapped(), EMPTY_STRING);
            for (Map.Entry<String, Object> keyentry : props.entrySet()) {
                Matcher matcher = connectIdPattern.matcher(keyentry.getKey());
                if (!matcher.find()) continue;
                Object connectIdValue = properties.get(matcher.group(1));
                if (connectIdValue == null) connectIdValue = keyentry.getValue();
                if (connectIdValue == null || isBlank(connectIdValue.toString())) continue;
                List<String> connectIds = (connectIdValue instanceof List) ? (List) connectIdValue : Arrays.stream(connectIdValue.toString().split(",")).distinct().collect(Collectors.toList());
                for (String connectId : connectIds) {
                    List<String> keys = connectIdsMap.get(connectId);
                    if (keys == null) {
                        keys = new ArrayList<>();
                        connectIdsMap.put(connectId, keys);
                    }
                    keys.add(matcher.group(2) + "config");
                }
            }
            if (props.containsKey("connectId")) {
                String connectId = props.get("connectId").toString();
                List<String> keys = connectIdsMap.get(connectId);
                if (keys == null) {
                    keys = new ArrayList<>();
                    connectIdsMap.put(connectId, keys);
                }
                keys.add("config");
            }
            if (props.containsKey("config.checkpoint.connectId")) {
                String connectId = props.get("config.checkpoint.connectId").toString();
                List<String> keys = connectIdsMap.get(connectId);
                if (keys == null) {
                    keys = new ArrayList<>();
                    connectIdsMap.put(connectId, keys);
                }
                keys.add("config.checkpoint");
            }
            dslBuilder.append("\n\n");
        }
        StringBuilder connectIdBuilder = new StringBuilder();
        Map<String, Map<String, Object>> connectIdMap = fetchConnectInfo(pipelineId, connectIdsMap.keySet());
        for (Map.Entry<String, List<String>> connectEntry : connectIdsMap.entrySet()) {
            String connectId = connectEntry.getKey();
            Map<String, Object> connectInfo = connectIdMap.get(connectId);
            if (connectInfo == null || connectInfo.isEmpty() || isBlank((String) connectInfo.get(CONNECT_ID_PROP_FILE_NAME))) {
                continue;
            }
            Map<String, Object> props = (Map) readNullableJsonMap(new String(getDecoder().decode((String) connectInfo.get(CONNECT_ID_PROP_FILE_NAME)), UTF_8));
            if (props == null)
                throw new InvalidInputException(String.format("Could not fetch connectId details for connectId : %s; Is connectId valid ? Are you trying to connect to a valid environment ?", connectId));
            String connectInfoString = hoconString(props);
            for (final String connectKey : connectEntry.getValue()) {
                connectIdBuilder.append(connectKey).append(" = {\n").append(connectInfoString).append("\n}\n");
            }
        }
        String dsl = connectIdBuilder + dslBuilder.toString();
        StargatePipeline stargatePipeline = create(pipelineId, dsl, properties);
        for (StargateNode node : stargatePipeline.nodes().values()) {
            List<String> connectIds = node.connectIds();
            if (connectIds == null || connectIds.isEmpty()) {
                continue;
            }
            for (String _connectId : connectIds) {
                String connectId = _connectId.trim();
                Map<String, Object> connectInfo = connectIdMap.get(connectId);
                if (connectInfo == null) {
                    if (connectIdMap.containsKey(connectId)) continue;
                    connectIdMap.putAll(fetchConnectInfo(pipelineId, asList(connectId)));
                }
                if (connectInfo == null || connectInfo.isEmpty()) continue;
                for (Map.Entry<String, Object> entry : connectInfo.entrySet()) {
                    if (CONNECT_ID_PROP_FILE_NAME.equals(entry.getKey())) continue;
                    byte[] bytes = ByteBuffer.wrap(getDecoder().decode((String) entry.getValue())).array();
                    if (node.configFiles() == null) node.configFiles(new HashMap<>());
                    node.configFiles().put(entry.getKey(), bytes);
                    String[] fileTokens = entry.getKey().split("\\.");
                    if (fileTokens.length > 1 && IGNORE_EXTENSIONS.contains(fileTokens[fileTokens.length - 1].toLowerCase())) {
                        String[] mtokens = new String[fileTokens.length - 1];
                        System.arraycopy(fileTokens, 0, mtokens, 0, fileTokens.length - 1);
                        String fileName = Arrays.stream(mtokens).collect(Collectors.joining("."));
                        node.configFiles().put(fileName, bytes);
                    }
                }
            }
        }
        final List<String> argsList = new ArrayList<>();
        Arrays.stream(args).filter(x -> x != null && !x.trim().isBlank()).forEach(x -> argsList.add(x));
        if (sOptions.getSharedDirectoryPath() == null || sOptions.getSharedDirectoryPath().isBlank()) {
            String baseSharedDirectory = env(STARGATE_BASE_SHARED_DIRECTORY, null);
            String sharedDir = env(STARGATE_SHARED_DIRECTORY, baseSharedDirectory == null ? null : baseSharedDirectory + "/" + pipelineId);
            if (isBlank(sharedDir)) sharedDir = LOCAL_SG_BASE_PATH + "/" + pipelineId + "/tmp/shared";
            java.nio.file.Path sharedDirectory = Paths.get(sharedDir).normalize();
            if (!Files.exists(sharedDirectory) || !Files.isDirectory(sharedDirectory))
                Files.createDirectories(sharedDirectory);
            sharedDir = sharedDirectory.toFile().getAbsolutePath();
            argsList.add("--sharedDirectoryPath=" + sharedDir);
        }
        Instant pipelineInvokeTime = sOptions.getPipelineInvokeTime() == null || sOptions.getPipelineInvokeTime().trim().isBlank() ? Instant.now() : parseDateTime(sOptions.getPipelineInvokeTime().trim());
        argsList.add("--pipelineInvokeTime=" + FORMATTER_DATE_TIME_1.format(pipelineInvokeTime));

        String jobName = jobName(sOptions, stargatePipeline, DEFAULT_DAG_NAME);
        argsList.add("--jobName=" + jobName);
        LOGGER.info("Creating Apache Beam Pipeline using arguments", argsList.stream().filter(x -> !isRedactable(x)).collect(Collectors.toList()));
        String[] arguments = argsList.toArray(new String[argsList.size()]);
        StargateOptions pipelineOptions = PipelineOptionsFactory.fromArgs(arguments).withoutStrictParsing().as(StargateOptions.class);
        pipelineOptions.setJobName(jobName);
        pipelineOptions.setPipelineId(pipelineId);
        pipelineOptions.setPipelineToken(sOptions.getPipelineToken());
        PipelineConstants.RUNNER runner = PipelineConstants.RUNNER.valueOf(isBlank(sOptions.getPipelineRunner()) ? PipelineConstants.RUNNER.direct.name() : sOptions.getPipelineRunner());
        pipelineOptions.setRunner((Class<? extends PipelineRunner<?>>) Class.forName(runner.getConfig().getClassName()));
        if (!pipelineOptions.isDryRun()) {
            if (runner == PipelineConstants.RUNNER.driver || runner == PipelineConstants.RUNNER.spark) {
                Class runnerInitClass = sparkUtilsClass();
                runnerInitClass.getDeclaredMethod("initProperties", StargateOptions.class).invoke(null, sOptions);
            } else if (runner == PipelineConstants.RUNNER.direct) {
                System.setProperty(APP_IGNITE_DISCOVERY_TYPE, "multi");
            }
        }
        if (isBlank(stargatePipeline.getFailOnError())) stargatePipeline.setFailOnError("true");
        if (isBlank(stargatePipeline.getSchemaReference()))
            stargatePipeline.setSchemaReference(environment().getConfig().getSchemaReference());
        stargatePipeline.nodes().values().stream().filter(n -> n.isActive()).forEach(node -> {
            cascadeProperties(stargatePipeline, node, false);
            cascadeProperties(node, node.getConfig(), true);
        });
        return Pair.of(pipelineOptions, stargatePipeline);
    }

    private static String mergeDSLContent(final String baseString, final String overrideString) {
        Config base = ConfigFactory.parseString(detectDSLContentType(baseString).equals("yaml") ? jsonString(yamlToMap(baseString)) : baseString);
        Config override = ConfigFactory.parseString(detectDSLContentType(overrideString).equals("yaml") ? jsonString(yamlToMap(overrideString)) : overrideString);
        Config merged = override.withFallback(base);
        return merged.root().render(concise().setJson(true));
    }

    private static String detectDSLContentType(final String content) {
        String type = "yaml";
        String trimmed = content.trim();
        if (trimmed.startsWith("{")) {
            type = "json";
        } else if (trimmed.startsWith("//")) {
            type = "hocon";
        } else if (trimmed.startsWith("#")) {
            Matcher matcher = Pattern.compile("#([\\t ]?)format([\\t ]?):([\\t ]?)(json|hocon|yaml|yml)").matcher(trimmed.split("\n")[0].toLowerCase());
            if (matcher.find()) {
                type = matcher.group(4);
            }
        }
        return isBlank(type) ? "yaml" : type;
    }

    @SuppressWarnings("unchecked")
    private static void appendPipelineIdToLog(final String pipelineId, final long runNo) {
        String logString = String.format(",\"pipelineId\":\"%s\",\"runNo\":\"%d\"", pipelineId, runNo);
        asList("legacy.JsonLogLayout", "log4j.JsonLogLayout", "JsonLogLayout").stream().forEach(className -> {
            try {
                Class cls = Class.forName(String.format("com.apple.aml.stargate.common.logging.%s", className));
                Method method = cls.getDeclaredMethod("setAdditionalGlobalLogString", String.class);
                method.invoke(cls, logString);
            } catch (NoClassDefFoundError | ClassNotFoundException | NoSuchMethodException | IllegalAccessException |
                     InvocationTargetException e) {
                LOGGER.trace("Could not set additional log string. Will ignore", e.getMessage());
            }
        });
    }

    private static Map<String, String> operatorHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put(HEADER_CLIENT_APP_ID, String.valueOf(appId()));
        headers.put(HEADER_PIPELINE_APP_ID, String.valueOf(appId()));
        if (!isBlank(env(APP_DS_PASSWORD, null)) || !isBlank(env("EXECUTOR_" + APP_DS_PASSWORD, null)))
            headers.put(HEADER_A3_TOKEN, A3Utils.getA3Token(A3Constants.KNOWN_APP.STARGATE.appId(), A3Constants.KNOWN_APP.STARGATE.getConfig().getContextString()));
        if (!isBlank(env(STARGATE_SHARED_TOKEN, null))) headers.put(HEADER_PIPELINE_SHARED_TOKEN, sharedToken());
        return headers;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Object>> fetchConnectInfo(final String pipelineId, final Collection<String> connectIds) throws Exception {
        Map<String, Map<String, Object>> connectIdMap = new HashMap<>();
        String connectBaseUrl = null;
        Map<String, String> connectHeaders = null;
        for (String connectId : connectIds) {
            Map<String, Object> connectInfo = null;
            String envValue = env(String.format("CONNECT_ID_%s", connectId.replace('-', '_').toUpperCase()), null);
            if (!isBlank(envValue)) {
                connectInfo = (Map) readNullableJsonMap(new String(getDecoder().decode(envValue), UTF_8));
            } else if (!isBlank(env(STARGATE_SHARED_TOKEN, null))) {
                if (connectBaseUrl == null)
                    connectBaseUrl = env(STARGATE_OPERATOR_URI, environment().getConfig().getStargateUri());
                if (connectHeaders == null) connectHeaders = operatorHeaders();
                connectInfo = WebUtils.getData(String.format("%s/sg/pipeline/connect-info/%s/%s", connectBaseUrl, pipelineId, connectId), connectHeaders, Map.class, true);
            } else if ("appconfig".equalsIgnoreCase(env(STARGATE_CONNECT_BACKEND, null))) {
                connectInfo = AppConfigClient.getProperties(connectId, env(APPCONFIG_MODULE_ID, null), env(APPCONFIG_ENVIRONMENT, null));
            } else {
                if (connectBaseUrl == null) connectBaseUrl = environment().getConfig().getStargateUri();
                if (connectHeaders == null) connectHeaders = operatorHeaders();
                connectInfo = WebUtils.getData(String.format("%s/sg/pipeline/connect-info/%s/%s", connectBaseUrl, pipelineId, connectId), connectHeaders, Map.class, true);
            }
            connectIdMap.put(connectId, connectInfo);
        }
        return connectIdMap;
    }

    @SuppressWarnings("unchecked")
    public static StargatePipeline create(final String pipelineId, final String dslString, final Map<String, String> properties) throws Exception {
        StringBuilder pipelineBuilder = new StringBuilder(dslString == null ? "" : dslString);
        pipelineBuilder.append("\n");
        if (properties != null) {
            TreeMap<String, Object> sortedMap = new TreeMap<>(properties);
            for (Map.Entry<String, Object> entry : sortedMap.entrySet()) {
                Object value = entry.getValue();
                if (value == null) {
                    continue;
                }
                String key = entry.getKey();
                appendHoconToBuilder(key, value, pipelineBuilder);
            }
        }
        String baseDsl = replaceGenericNodeTokens(pipelineBuilder.toString());
        baseDsl += "\n\nname = " + pipelineId + "\n\n";
        Config baseConfig = ConfigFactory.parseString(baseDsl);
        String mode = mode();
        Config config = baseConfig.hasPath(mode) ? baseConfig.getConfig(mode).withFallback(baseConfig) : baseConfig;
        String mergedDsl = config.root().render(CONFIG_RENDER_OPTIONS_CONCISE);
        StargatePipeline pipeline = hoconToPojo(mergedDsl, StargatePipeline.class);
        Map<String, StargateNode> nodes = new HashMap<>();
        asList("reader", "writer", "transformer", "trigger").stream().forEach(x -> fillConfig(x, config, nodes));
        if (config.hasPath("nodes")) {
            Config configNodes = config.getConfig("nodes");
            for (Map.Entry<String, ConfigValue> entry : configNodes.root().entrySet()) {
                if (nodes.containsKey(entry.getKey())) continue;
                StargateNode node = parseNode(entry.getKey(), configNodes.getConfig(entry.getKey()));
                nodes.put(entry.getKey(), node);
            }
        }
        StargateNode errorNode = new StargateNode();
        errorNode.setName(ErrorNode.name());
        errorNode.setType(ErrorNode.name());
        errorNode.setAlias(ErrorNode.getConfig().getAliases().stream().distinct().collect(Collectors.joining(",")));
        nodes.put(errorNode.getName(), errorNode);
        pipeline.nodes(nodes);
        pipeline.enableDirectAccess();
        if (LOGGER.isTraceEnabled()) {
            String conciseDsl = ConfigFactory.parseString(jsonString(nonNullMap(pipeline))).root().render(CONFIG_RENDER_OPTIONS_CONCISE);
            LOGGER.trace(conciseDsl);
            System.out.println("----------- PIPELINE DEFINITION -----------");
            System.out.println(conciseDsl);
            System.out.println("----------- PIPELINE DEFINITION -----------");
        }
        LOGGER.trace("mergedDsl : {}", mergedDsl);
        if (pipeline.getConfig() == null) pipeline.setConfig(new HashMap<>());
        if (properties != null) pipeline.getConfig().putAll(properties);
        pipeline.setDag(digraph(pipeline.getDag()));
        Map<String, String> jobMap = new HashMap<>();
        if (pipeline.getFlows() != null && !pipeline.getFlows().isEmpty()) {
            for (int i = pipeline.getFlows().size() - 1; i >= 0; i--) {
                Object flow = pipeline.getFlows().get(i);
                if (flow == null) {
                    continue;
                }
                String flowString = flow.toString().trim();
                if (flowString.isBlank()) {
                    continue;
                }
                String[] split = flowString.split(":");
                jobMap.put((split.length == 1 ? ("flow" + i) : split[0].trim()).toLowerCase(), split.length == 1 ? split[0] : split[1]);
            }
        }
        if (pipeline.getJobs() != null && !pipeline.getJobs().isEmpty()) {
            pipeline.getJobs().forEach((k, v) -> {
                if ((v instanceof String && isBlank((String) v)) || (v instanceof List && ((List) v).isEmpty())) {
                    return;
                }
                jobMap.put(k.toLowerCase(), digraph(v));
            });
        }
        if (pipeline.getDag() != null) {
            jobMap.put(DEFAULT_DAG_NAME, pipeline.getDag().toString());
        }
        pipeline.setJobs((Map) jobMap);
        return pipeline;
    }

    private static String replaceGenericNodeTokens(final String input) {
        String output = input.replaceAll("(reader)([0-9]+)((\\.)|([ \\t=:]+)|(\"[ \\t=:]+))", "$1.$2$3");
        output = output.replaceAll("(reader)((\\.([^0-9]))|([ \\t=:]+)|(\"[ \\t=:]+))", "$1.0$2");
        output = output.replaceAll("(writer)([0-9]+)((\\.)|([ \\t=:]+)|(\"[ \\t=:]+))", "$1.$2$3");
        output = output.replaceAll("(writer)((\\.([^0-9]))|([ \\t=:]+)|(\"[ \\t=:]+))", "$1.0$2");
        output = output.replaceAll("(transformer)([0-9]+)((\\.)|([ \\t=:]+)|(\"[ \\t=:]+))", "$1.$2$3");
        output = output.replaceAll("(transformer)((\\.([^0-9]))|([ \\t=:]+)|(\"[ \\t=:]+))", "$1.0$2");
        output = output.replaceAll("(flow)([0-9]+)((\\.)|([ \\t=:]+)|(\"[ \\t=:]+))", "$1.$2$3");
        output = output.replaceAll("(flow)((\\.([^0-9]))|([ \\t=:]+)|(\"[ \\t=:]+))", "$1.0$2");
        return output;
    }

    private static String jobName(final StargateOptions options, final StargatePipeline stargatePipeline, final String dagName) {
        if (stargatePipeline.isDynamicJobName()) {
            return String.join("-", asList(mode().toLowerCase(), pipelineId(), String.valueOf(options.getPipelineRunNo()), dagName.toLowerCase(), FORMATTER_DATE_1.format(Instant.now()), Integer.toHexString((new Random()).nextInt())));
        }
        return String.join("-", asList(pipelineId(), dagName.toLowerCase()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @SneakyThrows
    private static void fillConfig(final String configType, final Config inputConfig, final Map<String, StargateNode> nodes) {
        List<Config> configList = inputConfig.hasPath(configType + "s") ? (List<Config>) inputConfig.getConfigList(configType + "s") : new ArrayList<>(1);
        Config config = inputConfig.hasPath(configType) ? inputConfig.getConfig(configType) : null;
        int requiredSize = configList.size();
        if (!(config == null || config.root() == null || config.isEmpty())) {
            requiredSize = Math.max(Collections.max(config.root().keySet().stream().map(Integer::parseInt).distinct().collect(Collectors.toList())) + 1, requiredSize);
        }
        for (int i = 0; i < requiredSize; i++) {
            Config directConfig = config != null && config.hasPath("" + i) ? config.getConfig("" + i) : null;
            Config arrayConfig = i < configList.size() ? configList.get(i) : null;
            String mergedHocon = asList(arrayConfig, directConfig).stream().filter(c -> c != null).map(c -> c.root().render(CONFIG_RENDER_OPTIONS_CONCISE)).collect(Collectors.joining("\n"));
            Config value = ConfigFactory.parseString(mergedHocon);
            if (value.hasPath("name") && !isBlank(value.getString("name")) && config.hasPath("nodes") && config.getConfig("nodes").hasPath(value.getString("name"))) {
                mergedHocon = asList(config.getConfig("nodes").getConfig(value.getString("name")), value).stream().filter(c -> c != null).map(c -> c.root().render(CONFIG_RENDER_OPTIONS_CONCISE)).collect(Collectors.joining("\n"));
                value = ConfigFactory.parseString(mergedHocon);
            }
            StargateNode node = parseNode(String.format("%s%d", configType, i), value);
            if (node == null) continue;
            nodes.put(node.getName(), node);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static StargateNode parseNode(final String defaultNodeName, final Config config) throws InvalidInputException {
        if (!config.hasPath("type") || isBlank(config.getString("type"))) {
            if (config.isEmpty()) return null;
            throw new InvalidInputException(String.format("Missing type attribute for node - %s", config.hasPath("name") ? config.getString("name") : defaultNodeName));
        }
        PipelineConstants.NODE_TYPE nodeType = PipelineConstants.NODE_TYPE.nodeType(config.getString("type"));
        if (nodeType == null)
            throw new InvalidInputException(String.format("Could not determine a valid node type using supplied type : %s", config.getString("type")));
        Class<?> confClass = nodeType.getConfig().getOptionsClass() == null ? Map.class : nodeType.getConfig().getOptionsClass();
        ConfigObject conf = config.hasPath("config") ? config.getObject("config") : null;
        StargateNode node = hoconToPojo(config.root().render(CONFIG_RENDER_OPTIONS_CONCISE), StargateNode.class);
        if (isBlank(node.getName())) node.setName(defaultNodeName);
        Object configObject = conf == null ? null : (confClass != null ? hoconToPojo(conf.render(), confClass) : hoconToMap(conf.render()));
        node.setConfig(configObject);
        Set<String> aliases = isBlank(node.getAlias()) ? new HashSet<>() : new HashSet<>(asList(node.getAlias().split(",")));
        aliases.add(defaultNodeName);
        aliases.remove(node.getName());
        node.setAlias(aliases.stream().collect(Collectors.joining(",")));
        return node;
    }

    public static String digraph(final Object dagObject) {
        if (dagObject == null || String.valueOf(dagObject).isBlank()) return null;
        String digraph = (dagObject instanceof List) ? ((List<?>) dagObject).stream().map(Object::toString).collect(Collectors.joining("\n")) : dagObject.toString();
        if (!Pattern.compile("digraph([\\t \\r\\n]*)\\{").matcher(digraph).find()) {
            digraph = "digraph {\n" + digraph + "\n}";
        }
        digraph = digraph.toLowerCase().replaceAll("~", ":");
        MutableGraph graph;
        try {
            graph = new Parser().read(digraph);
        } catch (Exception e) {
            throw new InvalidInputException("Invalid DAG", Map.of("digraph", digraph), e).wrap();
        }
        return graph.toString();
    }

    @SuppressWarnings("unchecked")
    public static Pipeline initializePipeline(final PipelineOptions options, final StargatePipeline stargatePipeline, final String dagName, final DirectedAcyclicGraph<BeamVertx, BeamEdge> graph) throws Exception {
        StargateOptions sOptions = options.as(StargateOptions.class);
        String jobName = jobName(sOptions, stargatePipeline, dagName);
        PipelineConstants.RUNNER runner = PipelineConstants.RUNNER.valueOf(isBlank(sOptions.getPipelineRunner()) ? PipelineConstants.RUNNER.direct.name() : sOptions.getPipelineRunner());
        Map<Object, Object> overrides = readJsonMap(jsonString(options));
        Map<String, Object> overrideOptions = ((Map<String, Object>) overrides.get("options"));
        overrideOptions.put("asyncEnabled", true);
        ((List<Object>) overrides.get("display_data")).add(new LinkedHashMap<>(Map.of("namespace", "org.apache.beam.runners.flink.FlinkStargateOptions", "key", "asyncEnabled", "type", "BOOLEAN", "value", "true")));
        Object jobargs = stargatePipeline.getJobargs() == null ? null : stargatePipeline.getJobargs().get(dagName);
        if (jobargs != null) {
            Map<Object, Object> jobargsMap = jobargs instanceof String ? readJsonMap((String) jobargs) : (jobargs instanceof Map ? (Map) jobargs : readJsonMap(jsonString(jobargs)));
            jobargsMap.forEach((k, v) -> overrideOptions.putIfAbsent(k.toString(), v));
        }
        PipelineOptions newOptions = getAs(overrides, PipelineOptions.class);
        newOptions.setJobName(jobName);
        newOptions.setRunner((Class<? extends PipelineRunner<?>>) Class.forName(runner.getConfig().getClassName()));
        Pipeline pipeline = Pipeline.create(newOptions);
        registerDefaultCoders(pipeline, null, environment());
        graph.vertexSet().stream().filter(v -> v.getValue().isActive()).forEach(v -> v.initRuntime(pipeline));
        initBeamGraph(pipeline, graph);
        if (runner == PipelineConstants.RUNNER.driver) {
            Class runnerInitClass = sparkUtilsClass();
            runnerInitClass.getDeclaredMethod("initSparkSession").invoke(null);
        }
        return pipeline;
    }

    public static void startMetricCollectionThread(final Pipeline pipeline, final PipelineResult result) {
        MetricsEnvironment.setMetricsSupported(true);
        if (!parseBoolean(env(STARGATE_HUBBLE_METRICS_ENABLED, "false"))) {
            LOGGER.debug("Hubble metrics are not enabled");
            return;
        }
        long hubbleFrequency = AppConfig.config().getLong("hubble.publisher.frequency");
        MetricsOptions metricsOptions = pipeline.getOptions().as(MetricsOptions.class);
        if (metricsOptions.getMetricsPushPeriod() == null || metricsOptions.getMetricsPushPeriod().longValue() > hubbleFrequency) {
            metricsOptions.setMetricsPushPeriod(hubbleFrequency);
        } else {
            hubbleFrequency = metricsOptions.getMetricsPushPeriod();
        }
        if (pipeline.getOptions().as(StargateOptions.class).isMetricSinkEnabled()) {
            LOGGER.info("Metrics Sink is enabled. Will configure now");
            metricsOptions.setMetricsSink(HubbleMetricsSink.class);
            final MetricsSink metricsSink = InstanceBuilder.ofType(MetricsSink.class).fromClass(metricsOptions.getMetricsSink()).withArg(MetricsOptions.class, metricsOptions).build();
            ScheduledExecutorService metricsScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("metrics-thread").build());
            metricsScheduler.scheduleAtFixedRate(() -> {
                LOGGER.debug("Publishing metrics now..");
                try {
                    MetricResults metricResults = result.metrics();
                    MetricQueryResults metricQueryResults = metricResults.allMetrics();
                    if ((Iterables.size(metricQueryResults.getDistributions()) != 0) || (Iterables.size(metricQueryResults.getGauges()) != 0) || (Iterables.size(metricQueryResults.getCounters()) != 0)) {
                        metricsSink.writeMetrics(metricQueryResults);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Could not publish metrics to sink", e);
                }
            }, 0, hubbleFrequency, TimeUnit.SECONDS);
            LOGGER.info("Metrics Sink configured successfully with publish frequency of " + hubbleFrequency + " seconds");
        }
    }



    @SuppressWarnings("unchecked")
    public static void loadPipelineSchemas(final StargatePipeline pipeline) {
        if (pipeline.getSchemas() == null || pipeline.getSchemas().isEmpty()) return;
        for (Map.Entry<String, Object> entry : pipeline.getSchemas().entrySet()) {
            if (isBlank(entry.getKey()) || entry.getValue() == null) continue;
            String schemaId = entry.getKey().trim();
            Object object = entry.getValue();
            Schema schema = parseSchema(schemaId, object);
            saveLocalSchema(schemaId, schema.toString());
        }
    }

    @SuppressWarnings("unchecked")
    public static Pipeline initializeBeamPipeline(final PipelineOptions options, final StargatePipeline stargatePipeline, final String dagName, final DirectedAcyclicGraph<BeamVertx, BeamEdge> graph) throws Exception {
        StargateOptions sOptions = options.as(StargateOptions.class);
        String jobName = jobName(sOptions, stargatePipeline, dagName);
        PipelineConstants.RUNNER runner = PipelineConstants.RUNNER.valueOf(isBlank(sOptions.getPipelineRunner()) ? PipelineConstants.RUNNER.direct.name() : sOptions.getPipelineRunner());
        Map<Object, Object> overrides = readJsonMap(jsonString(options));
        Map<String, Object> overrideOptions = ((Map<String, Object>) overrides.get("options"));
        overrideOptions.put("asyncEnabled", true);
        ((List<Object>) overrides.get("display_data")).add(new LinkedHashMap<>(Map.of("namespace", "org.apache.beam.runners.flink.FlinkStargateOptions", "key", "asyncEnabled", "type", "BOOLEAN", "value", "true")));
        Object jobargs = stargatePipeline.getJobargs() == null ? null : stargatePipeline.getJobargs().get(dagName);
        if (jobargs != null) {
            Map<Object, Object> jobargsMap = jobargs instanceof String ? readJsonMap((String) jobargs) : (jobargs instanceof Map ? (Map) jobargs : readJsonMap(jsonString(jobargs)));
            jobargsMap.forEach((k, v) -> overrideOptions.putIfAbsent(k.toString(), v));
        }
        PipelineOptions newOptions = getAs(overrides, PipelineOptions.class);
        newOptions.setJobName(jobName);
        newOptions.setRunner((Class<? extends PipelineRunner<?>>) Class.forName(runner.getConfig().getClassName()));
        Pipeline pipeline = Pipeline.create(newOptions);
        registerDefaultCoders(pipeline, null, environment());
        graph.vertexSet().stream().filter(v -> v.getValue().isActive()).forEach(v -> v.initRuntime(pipeline));
        initBeamGraph(pipeline, graph);
        if (runner == PipelineConstants.RUNNER.driver) {
            Class runnerInitClass = sparkUtilsClass();
            runnerInitClass.getDeclaredMethod("initSparkSession").invoke(null);
        }
        return pipeline;
    }

    @SuppressWarnings("unchecked")
    private static void initBeamGraph(final Pipeline pipeline, final DirectedAcyclicGraph<BeamVertx, BeamEdge> graph) throws Exception {
        List<Vertx> starters = graph.vertexSet().stream().filter(v -> v.getValue().isActive() && graph.getAncestors(v).isEmpty()).collect(Collectors.toList());
        BeamVertx starter = (BeamVertx) starters.stream().filter(v -> v.getValue().isPrimary()).findFirst().get();
        StargateNode reader = starter.getValue();
        LOGGER.info("Linking starter node", Map.of("name", reader.getName()));
        starter.output(starter.invokeRuntime(pipeline));
        linkBeamPipeline(graph, starter, pipeline);
        Map<String, ErrorOptions> errorDefinition = new HashMap<>();
        graph.vertexSet().stream().map(v -> v.getValue()).forEach(v -> {
            try {
                errorDefinition.put(v.getName(), duplicate(v, ErrorOptions.class));
            } catch (Exception ignored) {
            }
        });
        LOGGER.info("Error definition is set to", errorDefinition);
        ErrorInterceptor interceptor = new ErrorInterceptor(errorDefinition);
        PCollectionList<KV<String, ErrorRecord>> errorList = null;


        List<BeamEdge> edges = graph.edgeSet().stream().map(e -> (BeamEdge) e).filter(e -> e.target() != null && e.target().output() != null && e.target().output() instanceof SCollection && (e.target().getValue() instanceof StargateNode) && (graph.getDescendants(e.target()) == null || graph.getDescendants(e.target()).isEmpty())).collect(Collectors.toList());
        for (BeamEdge edge : edges) {
            PCollectionList<KV<String, ErrorRecord>> errors = ((SCollection) edge.target().output()).getErrors();
            if (errorList == null) {
                errorList = errors;
                continue;
            }
            errorList = errorList.and(errors.getAll());
        }
        if (errorList != null) {
            AtomicInteger counter = new AtomicInteger();
            List<PCollection<KV<String, ErrorRecord>>> interceptors = errorList.getAll().stream().map(col -> col.apply(String.format("error-collector-%d", counter.getAndIncrement()), ParDo.of(interceptor))).collect(Collectors.toList());
            Optional<BeamVertx> errorVertex = graph.vertexSet().stream().filter(v -> v.getValue().isActive() && graph.getAncestors(v).isEmpty() && ErrorNode.getConfig().getAliases().contains(v.getValue().getName())).findFirst();
            if (errorVertex.isPresent()) {
                BeamVertx eVertex = errorVertex.get();
                Set<BeamEdge> errorEdges = graph.edgesOf(eVertex);
                if (errorEdges != null && !errorEdges.isEmpty() && errorEdges.iterator().hasNext() && ((BeamEdge) errorEdges.iterator().next()).target() != eVertex) {
                    eVertex.output(SCollection.of(pipeline, PCollectionList.of(interceptors).apply("flatten-errors", Flatten.pCollections()).apply("error-transformer", ParDo.of(new ErrorPayloadConverterFns("errors", environment())))));
                    LOGGER.info("Linking error node", Map.of("name", eVertex.getValue().getName()));
                    linkBeamPipeline(graph, eVertex, pipeline);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void linkBeamPipeline(final DirectedAcyclicGraph<BeamVertx, BeamEdge> graph, final BeamVertx vertex, final Pipeline pipeline) throws Exception {
        if (vertex.init()) return;
        vertex.init(true);
        for (BeamEdge edge : graph.edgesOf(vertex)) {
            BeamVertx target = edge.target();
            if (target == vertex) continue;
            Object input = edge.source().output();
            target.output(target.invokeRuntime(pipeline, input));
            linkBeamPipeline(graph, target, pipeline);
        }
    }


    @SuppressWarnings("unchecked")
    public static StreamExecutionEnvironment initializeFlinkPipeline(final PipelineOptions options, final StargatePipeline stargatePipeline, final String dagName, final DirectedAcyclicGraph<FlinkVertx, FlinkEdge> graph) throws Exception {
        StargateOptions sOptions = options.as(StargateOptions.class);
        String jobName = jobName(sOptions, stargatePipeline, dagName);
        PipelineConstants.RUNNER runner = PipelineConstants.RUNNER.valueOf(isBlank(sOptions.getPipelineRunner()) ? PipelineConstants.RUNNER.direct.name() : sOptions.getPipelineRunner());
        Map<Object, Object> overrides = readJsonMap(jsonString(options));
        Map<String, Object> overrideOptions = ((Map<String, Object>) overrides.get("options"));
        overrideOptions.put("asyncEnabled", true);
        ((List<Object>) overrides.get("display_data")).add(new LinkedHashMap<>(Map.of("namespace", "org.apache.beam.runners.flink.FlinkStargateOptions", "key", "asyncEnabled", "type", "BOOLEAN", "value", "true")));
        Object jobargs = stargatePipeline.getJobargs() == null ? null : stargatePipeline.getJobargs().get(dagName);
        if (jobargs != null) {
            Map<Object, Object> jobargsMap = jobargs instanceof String ? readJsonMap((String) jobargs) : (jobargs instanceof Map ? (Map) jobargs : readJsonMap(jsonString(jobargs)));
            jobargsMap.forEach((k, v) -> overrideOptions.putIfAbsent(k.toString(), v));
        }
        PipelineOptions newOptions = getAs(overrides, PipelineOptions.class);
        newOptions.setJobName(jobName);
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkUtils.registerSerde(executionEnvironment);
        FlinkUtils.registerUDF(executionEnvironment);
        graph.vertexSet().stream().filter(v -> v.getValue().isActive()).forEach(v -> v.initRuntime(executionEnvironment));
        initFlinkGraph(executionEnvironment, graph);
        return executionEnvironment;
    }


    @SuppressWarnings("unchecked")
    private static void initFlinkGraph(final StreamExecutionEnvironment executionEnvironment, final DirectedAcyclicGraph<FlinkVertx, FlinkEdge> graph) throws Exception {
        List<FlinkVertx> starters = graph.vertexSet().stream().filter(v -> v.getValue().isActive() && graph.getAncestors(v).isEmpty()).collect(Collectors.toList());
        FlinkVertx starter = starters.stream().filter(v -> v.getValue().isPrimary()).findFirst().get();
        StargateNode reader = starter.getValue();
        LOGGER.info("Linking starter node", Map.of("name", reader.getName()));
        starter.output(starter.invokeRuntime(new EnrichedDataStream(executionEnvironment, null)));
        DataStream<Tuple2<String, ErrorRecord>> combinedErrorStream = null;
        linkFlinkPipeline(graph, starter, executionEnvironment, combinedErrorStream, true);

        Optional<FlinkVertx> errorVertex = graph.vertexSet().stream().filter(v -> v.getValue().isActive() && graph.getAncestors(v).isEmpty() && ErrorNode.getConfig().getAliases().contains(v.getValue().getName())).findFirst();
        if (errorVertex.isPresent()) {
            FlinkVertx eVertex = errorVertex.get();
            Set<FlinkEdge> errorEdges = graph.edgesOf(eVertex);
            if (errorEdges != null && !errorEdges.isEmpty() && errorEdges.iterator().hasNext() && ((FlinkEdge) errorEdges.iterator().next()).target() != eVertex) {
                DataStream<Tuple2<String, GenericRecord>> errorStream = combinedErrorStream.map(kv -> {
                    return FlinkUtils.convertErrorRecordToKV(kv);
                }).flatMap(new ErrorToGenericRecordMapperFunction("errors", environment()));
                EnrichedDataStream enrichedDataStream = new EnrichedDataStream(executionEnvironment, errorStream);
                eVertex.output(enrichedDataStream);
                LOGGER.info("Linking error node", Map.of("name", eVertex.getValue().getName()));
                linkFlinkPipeline(graph, starter, executionEnvironment, combinedErrorStream, false);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void linkFlinkPipeline(final DirectedAcyclicGraph<FlinkVertx, FlinkEdge> graph, final FlinkVertx vertex, final StreamExecutionEnvironment executionEnvironment, DataStream<Tuple2<String, ErrorRecord>> combinedErrorStream, boolean stitchErrors) throws Exception {
        if (vertex.init()) return;
        vertex.init(true);
        for (FlinkEdge edge : graph.edgesOf(vertex)) {
            FlinkVertx target = edge.target();
            if (target == vertex) continue;
            EnrichedDataStream output = edge.source().output();
            target.output(target.invokeRuntime(output));
            linkFlinkPipeline(graph, target, executionEnvironment, combinedErrorStream, stitchErrors);
            if (stitchErrors && edge.target().output() != null) {
                DataStream<Tuple2<String, ErrorRecord>> errorStream = FlinkErrorUtils.getErrorStream(edge.target().output().getDataStream());
                if (combinedErrorStream == null && errorStream != null) {
                    combinedErrorStream = errorStream;
                } else if (errorStream != null) {
                    combinedErrorStream.union(errorStream);
                }
            }
        }
    }
}
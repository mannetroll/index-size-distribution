package com.mannetroll;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * @author mannetroll
 */
public class JsonUtil {
	private final static Logger LOGGER = LogManager.getLogger(JsonUtil.class);
	private static final ObjectMapper mapper;
	private static final ObjectMapper pretty;

	static {
		mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		//
		pretty = new ObjectMapper();
		pretty.setSerializationInclusion(Include.NON_NULL);
		pretty.enable(SerializationFeature.INDENT_OUTPUT);
	}

	public static String getFile(String file) {
		try {
			return new String(Files.readAllBytes(Paths.get(file).toAbsolutePath()));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static String toJson(Object object) {
		try {
			StringWriter sw = new StringWriter();
			mapper.writeValue(sw, object);
			return sw.toString();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return null;
		}
	}

	public static List<Map<String, Object>> parseMapList(String json) {
		try {
			return mapper.readValue(json, new TypeReference<List<Map<String, Object>>>() {
			});
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
			return null;
		}
	}

	public static String toPretty(Object object) {
		try {
			StringWriter sw = new StringWriter();
			pretty.writeValue(sw, object);
			return sw.toString();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return null;
		}
	}

}

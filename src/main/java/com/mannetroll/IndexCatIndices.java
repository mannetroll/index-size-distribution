package com.mannetroll;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

@SpringBootApplication
public class IndexCatIndices implements CommandLineRunner {
	private static final Logger LOG = LogManager.getLogger(IndexCatIndices.class);
	private static ConfigurableApplicationContext context;

	@Override
	public void run(String... args) {
		// Create the low-level client
		RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();

		// Create the transport with a Jackson mapper
		RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

		// Create the Elasticsearch client
		ElasticsearchClient esClient = new ElasticsearchClient(transport);

		String json = JsonUtil.getFile("json/tv-tech-prod-20240831.json");
		List<Map<String, Object>> maps = JsonUtil.parseMapList(json);
		LOG.info("maps: " + maps.size());

		try {
			// Iterate over each index record and prepare data for indexing
			for (Map<String, Object> jsonMap : maps) {
				Map<String, Object> updatedMap = updateKeys(jsonMap);
				// LOG.info("updatedMap:" + JsonUtil.toPretty(updatedMap));
				updatedMap.put("@timestamp", "2024-08-30");

				// Index the data into a new index, e.g., "stats-indices"
				IndexRequest<Map<String, Object>> request = IndexRequest
						.of(i -> i.index("stats-indices").document(updatedMap).refresh(Refresh.True));

				IndexResponse response = esClient.index(request);
				LOG.info("Indexed document ID: " + response.id()); //

			}

			// Close the transport, freeing the underlying thread
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		try {
			transport.close();
		} catch (Exception e) {
		}
		System.exit(0);
	}

	private Map<String, Object> updateKeys(Map<String, Object> map) {
		// Create a new map to store modified keys
		Map<String, Object> updatedMap = new HashMap<>();

		// Iterate through the original map
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			// Replace "." with "_" in the key
			String newKey = entry.getKey().replace('.', '_');

			Object value = entry.getValue();
			if (newKey.contains("size") || newKey.contains("docs") || newKey.contains("pri")
					|| newKey.contains("rep")) {
				long parseLong = Long.parseLong(value.toString());
				updatedMap.put(newKey, parseLong);
			} else {
				updatedMap.put(newKey, value);
			}

			//
			// category
			//
			String key = entry.getKey();
			if (key.equals("index")) {
				if (value.toString().contains("ls-mandel")) {
					updatedMap.put("category", "mandel");
				} else if (value.toString().contains("ls-varnish")) {
					updatedMap.put("category", "varnish");
				} else if (value.toString().contains("ds-logs-apm")) {
					updatedMap.put("category", "apm-logs");
				} else if (value.toString().contains("ds-metrics-apm")) {
					updatedMap.put("category", "apm-metrics");
				} else if (value.toString().contains("ds-traces-apm")) {
					updatedMap.put("category", "apm-traces");
				}
			}

		}
		return updatedMap;
	}

	public static void main(String[] args) {
		context = SpringApplication.run(IndexCatIndices.class, args);
		LOG.info("BeanDefinitionCount: " + context.getBeanDefinitionCount());
	}

}

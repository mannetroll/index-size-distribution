package com.mannetroll;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
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

	@Autowired
	private ServiceProperties config;

	@Override
	public void run(String... args) {
		// Create the low-level client
		RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();

		// Create the transport with a Jackson mapper
		RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

		// Create the Elasticsearch client
		ElasticsearchClient esClient = new ElasticsearchClient(transport);

		String[] stringArray = { "2024-08-31", "2024-09-05", "2024-09-13", "2024-09-21", "2024-09-27", "2024-10-02",
				"2024-10-03", "2024-10-04" };
		Set<String> dates = new TreeSet<>(Arrays.asList(stringArray));

		for (String date : dates) {
			String tmp = date.replace("-", "");
			String json = JsonUtil.getFile("json/tv-tech-prod-" + tmp + ".json");
			List<Map<String, Object>> maps = JsonUtil.parseMapList(json);
			LOG.info("maps: " + maps.size());

			try {
				// Iterate over each index record and prepare data for indexing
				for (Map<String, Object> jsonMap : maps) {
					Map<String, Object> updatedMap = updateKeys(jsonMap);
					// LOG.info("updatedMap:" + JsonUtil.toPretty(updatedMap));
					updatedMap.put("@timestamp", date);

					// Index the data into a new index, e.g., "stats-indices"
					IndexRequest<Map<String, Object>> request = IndexRequest
							.of(i -> i.index("stats-indices").document(updatedMap).refresh(Refresh.True));

					IndexResponse response = esClient.index(request);
					LOG.info(date + ": indexed document ID: " + response.id()); //

				}

				// Close the transport, freeing the underlying thread
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
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
		Map<String, Long> calcMap = new HashMap<>();

		// Iterate through the original map
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			// Replace "." with "_" in the key
			String newKey = entry.getKey().replace('.', '_');

			Object value = entry.getValue();
			if (newKey.contains("size") || newKey.contains("docs") || newKey.contains("pri")
					|| newKey.contains("rep")) {
				long parseLong = Long.parseLong(value.toString());
				calcMap.put(newKey, parseLong);
				updatedMap.put(newKey, parseLong);
			} else {
				updatedMap.put(newKey, value);
			}

			if (entry.getKey().toString().contains("index")) {
				//
				// category
				//
				List<String> list = config.getCategory();
				for (String category : list) {
					if (value.toString().contains(category)) {
						updatedMap.put("category", category);
					}
				}
				if (value.toString().contains("partial")) {
					updatedMap.put("frozen", true);
				} else {
					updatedMap.put("frozen", false);
				}
			}

		}
		//
		// bytes per doc
		//
		Long dataset_size = calcMap.get("dataset_size");
		Long docs_count = calcMap.get("docs_count");
		updatedMap.put("bytes_per_doc", Long.valueOf(dataset_size / (1 + docs_count)));

		return updatedMap;
	}

	public static void main(String[] args) {
		context = SpringApplication.run(IndexCatIndices.class, args);
		LOG.info("BeanDefinitionCount: " + context.getBeanDefinitionCount());
	}

}

package com.mannetroll;

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

import com.mannetroll.util.JsonUtil;

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

				// Index the data into a new index, e.g., "stats-indices"
				IndexRequest<Map<String, Object>> request = IndexRequest
						.of(i -> i.index("stats-indices").document(jsonMap).refresh(Refresh.True));

				IndexResponse response = esClient.index(request);
				System.out.println("Indexed document ID: " + response.id());
			}

			// Close the transport, freeing the underlying thread
			transport.close();
		} catch (Exception e) {
		}
		System.exit(0);
	}

	public static void main(String[] args) {
		context = SpringApplication.run(IndexCatIndices.class, args);
		LOG.info("BeanDefinitionCount: " + context.getBeanDefinitionCount());
	}

}

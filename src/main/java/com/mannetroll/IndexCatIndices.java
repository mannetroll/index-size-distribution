package com.mannetroll;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
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

		try {

			// Close the transport, freeing the underlying thread
			transport.close();
		} catch (Exception e) {
		}

	}

	public static void main(String[] args) {
		context = SpringApplication.run(IndexCatIndices.class, args);
		LOG.info("BeanDefinitionCount: " + context.getBeanDefinitionCount());
	}

}

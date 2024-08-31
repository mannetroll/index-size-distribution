package com.mannetroll;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class EcsApp implements CommandLineRunner {
	private static final Logger LOG = LogManager.getLogger(EcsApp.class);
	private static ConfigurableApplicationContext context;

	@Override
	public void run(String... args) {
		LOG.info("######### ECS ~ ELASTIC COMMON SCHEMA #########");
	}

	public static void doLog() {
		LOG.info("The message goes here!");
	}

	public static void main(String[] args) {
		context = SpringApplication.run(EcsApp.class, args);
		LOG.info("BeanDefinitionCount: " + context.getBeanDefinitionCount());
	}

}

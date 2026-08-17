package com.GridWeaver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GridWeaverApplication {

	public static void main(String[] args) {
		SpringApplication.run(GridWeaverApplication.class, args);
	}

}

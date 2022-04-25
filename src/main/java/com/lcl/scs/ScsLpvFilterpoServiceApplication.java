package com.lcl.scs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ScsLpvFilterpoServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ScsLpvFilterpoServiceApplication.class, args);
	}

}


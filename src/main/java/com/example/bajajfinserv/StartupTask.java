package com.example.bajajfinserv;

import com.example.bajajfinserv.model.GenerateWebhookRequest;
import com.example.bajajfinserv.model.GenerateWebhookResponse;
import com.example.bajajfinserv.model.SubmitQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
public class StartupTask implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(StartupTask.class);
    private final WebClient webClient = WebClient.create();

    @Override
    public void run(ApplicationArguments args) {
        try {
            // Step 1: Send POST to generate webhook
            GenerateWebhookRequest request = new GenerateWebhookRequest(
                    "Anand Sreekumar",      // REPLACE WITH YOUR FULL NAME
                    "22BCE2045",            // REPLACE WITH YOUR REGISTRATION NUMBER
                    "anand234.sreekumar@gmail.com"  // REPLACE WITH YOUR EMAIL
            );

            logger.info("Sending webhook generation request for regNo: {}", request.regNo());

            Mono<GenerateWebhookResponse> responseMono = webClient.post()
                    .uri("https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(request)
                    .retrieve()
                    .onStatus(status -> status.is4xxClientError() || status.is5xxServerError(),
                            clientResponse -> Mono.error(new RuntimeException("API error: " + clientResponse.statusCode())))
                    .bodyToMono(GenerateWebhookResponse.class);

            GenerateWebhookResponse response = responseMono.block();
            if (response == null) {
                logger.error("Failed to receive webhook response");
                return;
            }

            logger.info("Received webhook: {}, accessToken: {}", response.webhook(), response.accessToken());

            // Step 2: Get SQL query (based on regNo)
            String finalSqlQuery = solveSqlQuery(request.regNo());

            // Step 3: Submit the solution
            SubmitQueryRequest submitRequest = new SubmitQueryRequest(finalSqlQuery);

            logger.info("Submitting SQL query to webhook: {}", response.webhook());

            Mono<String> submitMono = webClient.post()
                    .uri(response.webhook())
                    .header("Authorization", "Bearer " + response.accessToken())
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(submitRequest)
                    .retrieve()
                    .onStatus(status -> status.is4xxClientError() || status.is5xxServerError(),
                            clientResponse -> Mono.error(new RuntimeException("Submission error: " + clientResponse.statusCode())))
                    .bodyToMono(String.class);

            String submitResponse = submitMono.block();
            logger.info("Submission response: {}", submitResponse);

        } catch (Exception e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
        }
    }

    private String solveSqlQuery(String regNo) {
        // Check odd/even for question assignment
        String lastTwoDigits = regNo.substring(regNo.length() - 2);
        int number = Integer.parseInt(lastTwoDigits);
        boolean isOdd = number % 2 != 0;
        String questionType = isOdd ? "Odd (Question 1)" : "Even (Question 2)";
        logger.info("Assigned SQL question: {}", questionType);

        // SQL query for the provided problem (highest salary not on 1st)
        // Assumes this matches your regNo assignment; adjust if different
        return "SELECT p.AMOUNT AS SALARY, CONCAT(e.FIRST_NAME, ' ', e.LAST_NAME) AS NAME, TIMESTAMPDIFF(YEAR, e.DOB, '2025-08-29') AS AGE, d.DEPARTMENT_NAME FROM PAYMENTS p JOIN EMPLOYEE e ON p.EMP_ID = e.EMP_ID JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID WHERE DAY(p.PAYMENT_TIME) != 1 ORDER BY p.AMOUNT DESC LIMIT 1;";
    }
}
package dev.codingstoic.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.codingstoic.Customer;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


@Slf4j
public class FileParser {

    public static List<Customer> parseClients(String filePath) {
        try {
            var path = Paths.get(filePath);
            byte[] bytes = Files.readAllBytes(path);
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(bytes, objectMapper.getTypeFactory().constructCollectionType(List.class, Customer.class));
        } catch (Exception e) {
            log.error("Error while parsing clients: {}", e.getMessage());
        }
        return null;
    }
}

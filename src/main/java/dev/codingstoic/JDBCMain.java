package dev.codingstoic;

import dev.codingstoic.helper.FileParser;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

@Slf4j
public class JDBCMain {
    public static void main(String[] args) throws SQLException {
        var jdbcMain = new JDBCMain();
        Objects.requireNonNull(args);
        var customers = FileParser.parseClients(args[2]);
        if (customers == null) {
            log.error("Error while parsing clients");
            return;
        }
        String url = "jdbc:postgresql://localhost:5432/postgres_test";
        try (var connection = DriverManager.getConnection(url, args[0], args[1])) {
            int[] executedBatch = jdbcMain.create(customers, connection);
            log.info("Executed batch: {}", executedBatch.length);
            log.info("Executed batch: {}", List.of(executedBatch));
        }
    }


    public int[] create(List<Customer> customers, Connection connection) throws SQLException {
        var insertSQL = "INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?)";

        try (var statement = connection.prepareStatement(insertSQL)) {
            for (var customer : customers) {
                statement.setInt(1, Integer.parseInt(customer.id()));
                statement.setString(2, customer.firstName());
                statement.setString(3, customer.lastName());
                statement.setString(4, customer.email());
                statement.setString(5, customer.gender());
                statement.setString(6, customer.ipAddress());
                statement.addBatch();
            }
            return statement.executeBatch();
        }
    }
}

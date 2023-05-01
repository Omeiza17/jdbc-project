package dev.codingstoic;

import lombok.extern.slf4j.Slf4j;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

@Slf4j
public class JDBCMain {
    public static void main(String[] args) throws SQLException {
        Objects.requireNonNull(args);
        String url = "jdbc:postgresql://localhost:5432/postgres_test";
        try (var connection = DriverManager.getConnection(url, args[0], args[1]);
             var statement = connection.prepareStatement("SELECT * FROM customers");
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                System.out.println(resultSet.getString("first_name"));
            }
        }
    }
}

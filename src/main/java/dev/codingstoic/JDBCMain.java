package dev.codingstoic;

import dev.codingstoic.helper.FileParser;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

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
            var countSql = "SELECT COUNT(*) FROM customers";
            try (var statement = connection.prepareStatement(countSql)) {
                var resultSet = statement.executeQuery();
                if (!resultSet.next()) {
                    int[] executedBatch = jdbcMain.create(customers, connection);
                    log.info("Executed batch: {}", executedBatch.length);
                    log.info("Executed batch: {}", List.of(executedBatch));
                }
            }
            connection.setAutoCommit(false);

            boolean update1 = jdbcMain.updateWithAutoCommitOff(connection, "192.98.20.150", "1");
            boolean update3 = jdbcMain.updateWithAutoCommitOff(connection, "192.98.20.155", "3");

            if (!update1 || !update3) {
                connection.rollback();
                log.error("Rolling back");
            } else {
                connection.commit();
                log.info("Committing");
            }
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

    public Map<String, String> idAndIpAddresses(Connection connection) throws SQLException {
        Map<String, String> result = new HashMap<>();
        var selectSQL = "SELECT id, ip_address FROM customers";
        try (var statement = connection.prepareStatement(selectSQL)) {
            var resultSet = statement.executeQuery();
            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String ipAddress = resultSet.getString("ip_address");
                result.put(String.valueOf(id), ipAddress);
            }
        }
        return result;
    }

    public List<Customer> firstNameStartingWithE(Connection connection) throws SQLException {
        var selectSQL = "{call get_customers_with_e()}";
        List<Customer> result = new ArrayList<>();
        try (var statement = connection.prepareCall(selectSQL)) {
            var resultSet = statement.executeQuery();
            while (resultSet.next()) {
                var customer = new Customer(String.valueOf(resultSet.getInt(1)), resultSet.getString(2), resultSet.getString(3),
                        resultSet.getString(4), resultSet.getString(5), resultSet.getString(6));
                result.add(customer);
            }
        }
        return result;
    }

    public List<Customer> getCustomersWithNameStartingWith(Connection connection, String letter) throws SQLException {
        var selectSQL = "{call get_customers_by_starting_letter(?)}";
        List<Customer> result = new ArrayList<>();
        try (var statement = connection.prepareCall(selectSQL)) {
            statement.setString(1, letter);
            var resultSet = statement.executeQuery();
            while (resultSet.next()) {
                var customer = new Customer(String.valueOf(resultSet.getInt(1)), resultSet.getString(2), resultSet.getString(3),
                        resultSet.getString(4), resultSet.getString(5), resultSet.getString(6));
                result.add(customer);
            }
        }
        return result;
    }

    public Integer getMagicNumber(Connection conn) throws SQLException {
        var sql = "{call magic_number(?) }";
        try (var statement = conn.prepareCall(sql)) {
            statement.registerOutParameter(1, Types.INTEGER);
            statement.execute();
            return statement.getInt(1);
        }
    }

    public Integer doubleNumber(Connection conn, Integer number) throws SQLException {
        var sql = "{call double_number(?)}";
        try (var statement = conn.prepareCall(sql)) {
            statement.setInt(1, number);
            statement.registerOutParameter(1, Types.INTEGER);
            statement.execute();
            return statement.getInt(1);
        }
    }

    public boolean updateWithAutoCommitOff(Connection conn, String ipAddress, String id) throws SQLException {
        var updateSql = """
                UPDATE customers
                SET ip_address = ?
                WHERE id = ?
                """;

        try (var statement = conn.prepareStatement(updateSql)) {
            statement.setString(1, ipAddress);
            statement.setInt(2, Integer.parseInt(id));
            var result = statement.executeUpdate();
            return result > 0;
        }
    }
}

package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DepartmentDaoImpl implements DepartmentDao {

    @Override
    public Optional<Department> getById(BigInteger Id) {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try (Connection connection = connectionSource.createConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM DEPARTMENT WHERE ID = " + Id);
            if (resultSet.next()) {
                String name = resultSet.getString("NAME");
                String location = resultSet.getString("LOCATION");
                return Optional.of(new Department(Id, name, location));
            } else
                return Optional.empty();


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public List<Department> getAll() {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try (Connection connection = connectionSource.createConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM DEPARTMENT ");

            List<Department> departments = new ArrayList<>();
            while (resultSet.next()) {
                BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));
                String name = resultSet.getString("NAME");
                String location = resultSet.getString("LOCATION");
                departments.add(new Department(id, name, location));
            }
            return departments;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Department save(Department department) {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try (Connection connection = connectionSource.createConnection()) {
            Statement statement = connection.createStatement();

            Optional<Department> optionalDepartment = getById(department.getId());
            if (optionalDepartment.isPresent()) {
                Department department1 = optionalDepartment.get();
                statement.executeQuery(
                        "UPDATE DEPARTMENT SET NAME = '" +
                                department.getName() +
                                "', LOCATION = '" + department.getLocation() + "'  WHERE ID = " + department1.getId()
                );
                return department;
            } else if (statement.execute(
                    "INSERT INTO DEPARTMENT (ID, NAME, LOCATION)  VALUES(" +
                            department.getId() + ", '" + department.getName() + "', '" +
                            department.getLocation() + "');")) {
                return department;
            } else return null;


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Department department) {

        ConnectionSource connectionSource = ConnectionSource.instance();
        try (Connection connection = connectionSource.createConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(
                    "DELETE FROM DEPARTMENT WHERE ID = " + department.getId());

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }
}


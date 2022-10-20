package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class EmployeeDaoImpl implements EmployeeDao {


    @Override
    public Optional<Employee> getById(BigInteger Id) {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try (Connection connection = connectionSource.createConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE WHERE ID = " + Id);
            if (resultSet.next()) {
                String firstname = resultSet.getString("FIRSTNAME");
                String lastname = resultSet.getString("LASTNAME");
                String middleName = resultSet.getString("MIDDLENAME");
                FullName fullName = new FullName(firstname, lastname, middleName);
                String position = resultSet.getString("POSITION");
                int managerId = resultSet.getInt("MANAGER");
                Date hiredate = resultSet.getDate("HIREDATE");
                BigDecimal salary = resultSet.getBigDecimal("SALARY");
                int departmentId = resultSet.getInt("DEPARTMENT");
                return Optional.of(new Employee(
                        Id,
                        fullName,
                        Position.valueOf(position),
                        hiredate.toLocalDate(),
                        salary,
                        BigInteger.valueOf(managerId),
                        BigInteger.valueOf(departmentId)
                ));
            } else
                return Optional.empty();


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    public List<Employee> getAll() {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try (Connection connection = connectionSource.createConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE");
            return getEmployees(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Employee save(Employee employee) {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try (Connection connection = connectionSource.createConnection()) {
            Statement statement = connection.createStatement();

            Optional<Employee> optionalEmployee = getById(employee.getId());
            String sql = "INSERT INTO EMPLOYEE " +
                    "VALUES(" +
                    employee.getId() + ", '" +
                    employee.getFullName().getFirstName() + "', '" +
                    employee.getFullName().getLastName() + "', '" +
                    employee.getFullName().getMiddleName() + "', '" +
                    employee.getPosition().name() + "', " +
                    employee.getManagerId() + ", '" +
                    employee.getHired() + "', " +
                    employee.getSalary() + ", " +
                    employee.getDepartmentId() + ");";
            if (optionalEmployee.isPresent()) {
                Employee employee1 = optionalEmployee.get();
                statement.executeQuery(
                        "UPDATE DEPARTMENT SET" +
                                " FIRSTNAME = '" + employee.getFullName().getFirstName() +
                                "' LASTNAME = '" + employee.getFullName().getLastName() +
                                "' MIDDLENAME = '" + employee.getFullName().getMiddleName() +
                                "' POSITION = " + employee.getPosition().name() +
                                "' MANAGER = '" + employee.getManagerId() +
                                "' HIREDATE = " + employee.getHired() +
                                "  SALARY = " + employee.getSalary().doubleValue() +
                                "' DEPARTMENT = " + employee.getDepartmentId() +
                                "  WHERE ID = " + employee1.getId()
                );
                return employee;
            } else
                statement.execute(sql);
            return employee;


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Employee employee) {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try (Connection connection = connectionSource.createConnection()) {
            Statement statement = connection.createStatement();
            statement.execute("DELETE FROM EMPLOYEE WHERE ID = " + employee.getId());

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Employee> getByDepartment(Department department) {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try (Connection connection = connectionSource.createConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE WHERE DEPARTMENT = " + department.getId());

            return getEmployees(resultSet);


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public List<Employee> getByManager(Employee employee) {
        ConnectionSource connectionSource = ConnectionSource.instance();
        try (Connection connection = connectionSource.createConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM EMPLOYEE WHERE MANAGER = " + employee.getId());

            return getEmployees(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Employee> getEmployees(ResultSet resultSet) throws SQLException {
        List<Employee> employees = new ArrayList<>();
        while (resultSet.next()) {
            BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));
            String firstname = resultSet.getString("FIRSTNAME");
            String lastname = resultSet.getString("LASTNAME");
            String middleName = resultSet.getString("MIDDLENAME");
            FullName fullName = new FullName(firstname, lastname, middleName);
            String position = resultSet.getString("POSITION");
            int managerId = resultSet.getInt("MANAGER");
            Date hiredate = resultSet.getDate("HIREDATE");
            BigDecimal salary = resultSet.getBigDecimal("SALARY");
            int departmentId = resultSet.getInt("DEPARTMENT");
            employees.add(new Employee(
                    id,
                    fullName,
                    Position.valueOf(position),
                    hiredate.toLocalDate(),
                    salary,
                    BigInteger.valueOf(managerId),
                    BigInteger.valueOf(departmentId)
            ));
        }
        return employees;
    }
}


/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pruebamsql;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author carol
 */
public class DataSynchronizer {
    public static void sincronizarDatos(Connection mysqlConn, Connection postgresConn){
      try {
            ResultSet empleadosMySQL = DatabaseOperations.obtenerEmpleados(mysqlConn);
            ResultSet empleadosPostgres = DatabaseOperations.obtenerEmpleados(postgresConn);

            while (empleadosMySQL.next()) {
                int id = empleadosMySQL.getInt("id");
                String nombre = empleadosMySQL.getString("nombre");
                String puesto = empleadosMySQL.getString("puesto");
                double salario = empleadosMySQL.getDouble("salario");
                java.sql.Timestamp fechaActualizacionMySQL = empleadosMySQL.getTimestamp("fecha_actualizacion");

                // Buscar el registro correspondiente en PostgreSQL
                boolean existeEnPostgres = false;
                while (empleadosPostgres.next()) {
                    if (empleadosPostgres.getInt("id") == id) {
                        existeEnPostgres = true;
                        java.sql.Timestamp fechaActualizacionPostgres = empleadosPostgres.getTimestamp("fecha_actualizacion");
                        
                        if (fechaActualizacionMySQL.after(fechaActualizacionPostgres)) {
                            // Actualizar PostgreSQL si MySQL tiene un registro más reciente
                            DatabaseOperations.actualizarSalario(postgresConn, id, salario);
                        }
                        break;
                    }
                }

                if (!existeEnPostgres) {
                    // Insertar en PostgreSQL si el registro no existe
                    DatabaseOperations.insertarEmpleado(postgresConn, nombre, puesto, salario);
                }
            }

            System.out.println("Sincronización completada con éxito.");

        } catch (SQLException e) {
            System.out.println("Error durante la sincronización: " + e.getMessage());
        }
    }
}
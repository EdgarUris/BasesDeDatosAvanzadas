/* =========================================================
   BASE DE DATOS: banco_sql
   OBJETIVO: Práctica de SQL Avanzado
   ========================================================= */

-- 1️ Crear base de datos
CREATE DATABASE IF NOT EXISTS BancoBBVA;
USE BancoBBVA;

/* =========================================================
   TABLAS
   ========================================================= */

-- Tabla de clientes
CREATE TABLE clientes (
    cliente_id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    fecha_registro DATE
);

-- Tabla de cuentas bancarias
CREATE TABLE cuentas (
    cuenta_id INT AUTO_INCREMENT PRIMARY KEY,
    cliente_id INT NOT NULL,
    tipo_cuenta VARCHAR(50),
    saldo DECIMAL(12,2) CHECK (saldo >= 0),
    fecha_apertura DATE,
    CONSTRAINT fk_cuentas_clientes
        FOREIGN KEY (cliente_id)
        REFERENCES clientes(cliente_id)
);

-- Tabla de movimientos
CREATE TABLE movimientos (
    movimiento_id INT AUTO_INCREMENT PRIMARY KEY,
    cuenta_id INT NOT NULL,
    tipo_movimiento VARCHAR(50), -- Deposito / Retiro / Transferencia
    monto DECIMAL(12,2) CHECK (monto > 0),
    fecha_movimiento DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_movimientos_cuentas
        FOREIGN KEY (cuenta_id)
        REFERENCES cuentas(cuenta_id)
);

/* =========================================================
   DATOS DE PRUEBA
   ========================================================= */

INSERT INTO clientes (nombre, email) VALUES
('Juan Pérez', 'juan@correo.com'),
('María López', 'maria@correo.com'),
('Carlos Gómez', 'carlos@correo.com');

INSERT INTO cuentas (cliente_id, tipo_cuenta, saldo) VALUES
(1, 'Ahorro', 5000),
(2, 'Débito', 3000),
(3, 'Ahorro', 10000);

INSERT INTO movimientos (cuenta_id, tipo_movimiento, monto) VALUES
(1, 'Deposito', 2000),
(1, 'Retiro', 500),
(2, 'Deposito', 1500),
(3, 'Retiro', 1000);

/* =========================================================
   CONSULTAS DE VERIFICACIÓN
   ========================================================= */

SELECT * FROM clientes;
SELECT * FROM cuentas;
SELECT * FROM movimientos;

/* =========================================================
   ACTIVIDADES A DESARROLLAR POR EL ALUMNO
   ========================================================= */

------------------------------------------------------------
-- FUNCIÓN DEFINIDA POR EL USUARIO
------------------------------------------------------------
/*
Crear una FUNCIÓN que reciba un cuenta_id
y regrese el SALDO CALCULADO
a partir de los movimientos.

Reglas:
- Deposito suma
- Retiro resta
- Usar SUM() y CASE
- Debe regresar DECIMAL(12,2)

Nombre sugerido:
fn_calcular_saldo(cuenta_id INT)
*/

DELIMITER $$
CREATE FUNCTION fn_calcular_saldo(cuenta_id INT)
RETURNS DECIMAL(12,2)
AS
BEGIN
	DECLARE saldo DECIMAL(12,2);
    SELECT saldo = SUM(
		CASE
			WHEN tipo_movimiento = 'Deposito' THEN monto
            WHEN tipo_movimiento = 'Retiro' THEN monto*-1
		END CASE;
    ) FROM movimientos WHERE cuenta_id = movimientos.cuenta_id;
    RETURN saldo;
END
DELIMITER $$;

SELECT fn_calcular_saldo(1) AS saldo;
------------------------------------------------------------
-- VISTA
------------------------------------------------------------
/*
Crear una VISTA que muestre:
- Nombre del cliente
- Tipo de cuenta
- Saldo almacenado
- Total de movimientos realizados

Debe usar JOINs
y simplificar consultas complejas.

Nombre sugerido:
vw_resumen_cuentas
*/

CREATE VIEW vw_resumen_cuentas
AS 
SELECT
	c.nombre AS NombreCliente,
    cu.tipo_cuenta  AS TipoCuenta,
    cu.saldo AS SaldoAlmacenado,
    COUNT(m.movimiento_id) AS TotalMovimientos
FROM clientes c
INNER JOIN cuentas cu
	ON c.cliente_id = cu.cliente_id
LEFT JOIN movimientos m
	ON cu.cuenta_id = m.cuenta_id
GROUP BY c.nombre, cu.tipo_cuenta, cu.saldo;
    

------------------------------------------------------------
-- PROCEDIMIENTO ALMACENADO CON TRANSACCIÓN
------------------------------------------------------------
/*
Crear un PROCEDIMIENTO ALMACENADO que:
- Reciba: cuenta_id, tipo_movimiento y monto
- Inserte un nuevo movimiento
- Actualice el saldo de la cuenta
- Use TRANSACCIONES (START TRANSACTION, COMMIT, ROLLBACK)
- Use manejo de errores con DECLARE HANDLER

Reglas:
- Si el retiro es mayor al saldo, cancelar la operación
- No permitir montos negativos

Nombre sugerido:
sp_registrar_movimiento
*/
CREATE PROCEDURE sp_registrar_movimiento
	cuenta_id INT,
    tipo_movimiento(20),
    monto DECIMAL(12,2)
AS
BEGIN
	SET NOCOUNT ON;
    BEGIN TRY
		BEGIN TRANSACTION;
        IF monto <= 0
        BEGIN
			RAISERRROR('El monto debe ser mayor a cero.', 16, 1);
            ROLLBACK TRANSACTION;
            RETURN;
		END;
        
        DECLARE saldo_actual DECIMAL(12, 2);
        
        SELECT saldo_actual = saldo
        FROM cuentas
        WHERE cuenta_id = cuenta_id;
        
		IF tipo_movimiento = 'Retiro' AND monto > saldo_actual
        BEGIN
			RAISERROR('El retiro excede el saldo disponible.', 16, 1);
            ROLLBACK TRANSACTION;
            RETURN;
		END;
        INSERT INTO movimientos (cuenta_id, tipo_movimiento, monto)
        VALUES (cuenta_id, tipo_movimiento, monto);
        
		UPDATE cuentas
		SET saldo = CASE
					WHEN tipo_movimiento = 'Deposito' THEN saldo + monto
					WHEN tipo_movimiento = 'Retiro' THEN saldo - monto
                    END
		WHERE cuenta_id = cuenta_id;
        
        COMMIT TRANSACTION;
	END TRY
    BEGIN CATCH
		ROLLBACK TRANSACTION;
        DECLARE ErrorMesage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(ErrorMessage, 16, 1);
	END CATCH
END;

------------------------------------------------------------
-- ÍNDICE
------------------------------------------------------------
/*
Crear un ÍNDICE que optimice
las búsquedas de movimientos por cuenta_id
*/
CREATE INDEX idx_moviemientos_cuenta
ON movimientos (cuenta_id);

------------------------------------------------------------
-- TRIGGER
------------------------------------------------------------
/*
Crear un TRIGGER que valide
que el monto del movimiento sea mayor a cero
antes de insertar.
*/
CREATE TRIGGER trg_validar_monto_movimiento
ON movimientos
INSTEAD OF INSERT 
AS 
BEGIN
	SET NOCOUNT ON;
    IF EXISTS(
		SELECT 1
        FROM inserted
        WHERE monto <= 0
        )
        BEGIN
			RAISERRROR('El monto del movimiento debe ser mayor a cero.', 16, 1);
            ROLLBACK TRANSACTION;
            RETURN;
		END;
            
		INSERT INTO movimientos (cuenta_id, tipo_movimiento, monto)
        SELECT cuenta_id, tipo_movimiento, monto
        FROM inserted;
	END;



/* =========================================================
   FIN DEL SCRIPT
   ========================================================= */

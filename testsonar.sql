SET SERVEROUTPUT ON
spool 80PatchEFT20240930CreatePartitionPinerr.log

DECLARE
  p_schema VARCHAR2(30) := UPPER('&1.');
  p_tbs_dato VARCHAR2(50) := UPPER('&2.');
  p_tbs_indice VARCHAR2(50) := UPPER('&3.');
  p_partitions_antes NUMBER := &4-1; 
  p_partitions_desp NUMBER := &5;
  v_fecha_base date:= SYSDATE;
  v_sql VARCHAR2(8000);
  v_sql_ddl VARCHAR2(80);
  v_countgeneral NUMBER;
  v_tabla VARCHAR2(30);
  v_tabla_legacy VARCHAR2(50);
  v_query_count VARCHAR2(200);
  v_out_mensaje VARCHAR2(400);
  TYPE t_comments IS TABLE OF VARCHAR2(300) INDEX BY PLS_INTEGER;
  v_comments t_comments;
  e_schemanovalido          EXCEPTION;
  e_tbsdatonovalido       EXCEPTION;
  e_tbsindicenovalido     EXCEPTION;
  e_tablanoexiste     EXCEPTION;
  e_tablaparticionada EXCEPTION;
  e_num_meses EXCEPTION;
BEGIN

  v_query_count := q'{SELECT COUNT(1) FROM ALL_USERS WHERE USERNAME= :1}';
  EXECUTE IMMEDIATE v_query_count INTO v_countgeneral USING p_schema;
  IF v_countgeneral=0 THEN
  RAISE e_schemanovalido;
  END IF;

  v_query_count := q'{SELECT COUNT(1) FROM USER_TABLESPACES WHERE tablespace_name= :1}';
  EXECUTE IMMEDIATE v_query_count INTO v_countgeneral USING p_tbs_dato;
  IF v_countgeneral=0 THEN
  RAISE e_tbsdatonovalido;
  END IF;

  v_query_count := q'{SELECT COUNT(1) FROM USER_TABLESPACES WHERE tablespace_name= :1}';
  EXECUTE IMMEDIATE v_query_count INTO v_countgeneral USING p_tbs_indice;
  IF v_countgeneral=0 THEN
  RAISE e_tbsindicenovalido;
  END IF;

  IF p_partitions_antes <= 0 OR p_partitions_desp <= 0 THEN
         RAISE e_num_meses;
  END IF;

  v_tabla := 'PINERR';
  v_query_count := q'{SELECT COUNT(1) FROM all_tables WHERE owner=:1 AND table_name = :2}';
  EXECUTE IMMEDIATE v_query_count INTO v_countgeneral USING p_schema,v_tabla;
  IF v_countgeneral=0 THEN
  RAISE e_tablanoexiste;
  END IF;

  v_query_count := q'{SELECT COUNT(1) FROM all_tab_partitions WHERE table_owner= :1 AND table_name = :2}';
  EXECUTE IMMEDIATE v_query_count INTO v_countgeneral USING p_schema,v_tabla;
  IF v_countgeneral <> 0 THEN
  RAISE e_tablaparticionada;
  END IF;

  v_tabla := 'THPINERR';
  v_query_count := q'{SELECT COUNT(1) FROM all_tab_partitions WHERE table_owner= :1 AND table_name = :2}';
  EXECUTE IMMEDIATE v_query_count INTO v_countgeneral USING p_schema,v_tabla;
  IF v_countgeneral <> 0 THEN
  RAISE e_tablaparticionada;
  END IF;

    v_out_mensaje:= 'Resultado:';
    v_tabla := 'PINERR';
    v_sql_ddl:= 'ALTER TABLE';
    v_tabla_legacy:= 'PINERR_LEGACY'; 
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = ''YYYY-MM-DD''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD''';

    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||' RENAME TO '||v_tabla_legacy; 
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla_legacy||' RENAME CONSTRAINT PK_PAN TO PK_PAN_LEGACY'; 
    EXECUTE IMMEDIATE 'ALTER INDEX '||p_schema||'.PK_PAN RENAME TO PK_PAN_LEGACY'; 


    v_sql := 'CREATE TABLE ' || p_schema || '.PINERR (' ||
             '"PAN" VARCHAR2(64 BYTE) NOT NULL ENABLE, ' ||
             '"RETRIES" NUMBER(2,0), ' ||
             '"LAST_DATE" NUMBER(10,0), ' ||
             '"TERMID" VARCHAR2(8 BYTE), ' ||
             '"RETRIES_EXE" VARCHAR2(1 BYTE), ' ||
             '"LAST_TIME" VARCHAR2(6 BYTE), ' ||
             '"USERIN" VARCHAR2(24 BYTE) DEFAULT substr(user,1,24), ' ||
             '"DATEIN" DATE DEFAULT sysdate, ' ||
             '"USERCHG" VARCHAR2(24 BYTE), ' ||
             '"DATECHG" DATE) TABLESPACE ' || p_tbs_dato || ' PARTITION BY RANGE (DATEIN) (';

    FOR i IN -p_partitions_antes .. p_partitions_desp LOOP
         
    v_sql := v_sql || ' PARTITION P_PINERR_'||TO_CHAR(ADD_MONTHS(v_fecha_base,i),'YYYYMM')||' VALUES LESS THAN ('''||TO_CHAR(TRUNC(ADD_MONTHS(v_fecha_base,i+1), 'MM'),'YYYY-MM-DD')||''')';
         
    IF i < p_partitions_desp THEN
        v_sql := v_sql || ', ';
    END IF;

    END LOOP;      
    v_sql := v_sql || ')';

    EXECUTE IMMEDIATE v_sql;

    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||' ADD CONSTRAINT "PK_PAN" PRIMARY KEY ("PAN") USING INDEX TABLESPACE '||p_tbs_indice;

--COMMENTS
    v_comments(1) := 'Número de la tarjeta a la cual se le ingreso una clave errada.';
    v_comments(2) := 'Número de reintentos de ingreso de clave secreta.';
    v_comments(3) := 'Fecha del último ingreso de clave secreta errada.';
    v_comments(4) := 'Número de terminal del último ingreso de clave secreta errada.';
    v_comments(5) := 'Indica si la tarjeta excedió el número de reintentos de ingresos de clave secreta.';
    v_comments(6) := 'Hora del último ingreso de clave secreta errada.';
    v_comments(7) := 'Usuario que inserto el registro.';
    v_comments(8) := 'Fecha de inserción del registro.  Formato: DD/MM/YY.';
    v_comments(9) := 'Ultimo usuario que modificó el registro.';
    v_comments(10) := 'Fecha de la ultima modificación realizada.  Formato: DD/MM/YY.';
    v_comments(11) := 'Tabla que contiene la informaciún de los tarjetahabientes que han ingresado su clave errada con el fin de llevar la cuenta de la cantidad de PINES errados a soportar';
    v_sql_ddl:= 'COMMENT ON COLUMN';

    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||'."PAN" IS ''' || v_comments(1) || '''';
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||'."RETRIES" IS ''' || v_comments(2) || '''';
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||'."LAST_DATE" IS ''' || v_comments(3) || '''';
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||'."TERMID" IS ''' || v_comments(4) || '''';
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||'."RETRIES_EXE" IS ''' || v_comments(5) || '''';
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||'."LAST_TIME" IS ''' || v_comments(6) || '''';
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||'."USERIN" IS ''' || v_comments(7) || '''';
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||'."DATEIN" IS ''' || v_comments(8) || '''';
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||'."USERCHG" IS ''' || v_comments(9) || '''';
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||'."DATECHG" IS ''' || v_comments(10) || '''';
    EXECUTE IMMEDIATE 'COMMENT ON TABLE '||p_schema||'.'||v_tabla||'  IS ''' || v_comments(11) || '''';
 
    
    v_out_mensaje:= v_out_mensaje||''||CHR(10)||'OK: Tabla PINERR Particionada creada';


    v_sql := 'CREATE TABLE ' || p_schema || '.THPINERR (' ||
             '"PAN" VARCHAR2(64 BYTE) NOT NULL ENABLE, ' ||
             '"RETRIES" NUMBER(2,0), ' ||
             '"LAST_DATE" NUMBER(10,0), ' ||
             '"TERMID" VARCHAR2(8 BYTE), ' ||
             '"RETRIES_EXE" VARCHAR2(1 BYTE), ' ||
             '"LAST_TIME" VARCHAR2(6 BYTE), ' ||
             '"USERIN" VARCHAR2(24 BYTE) DEFAULT substr(user,1,24), ' ||
             '"DATEIN" DATE DEFAULT sysdate, ' ||
             '"USERCHG" VARCHAR2(24 BYTE), ' ||
             '"DATECHG" DATE) TABLESPACE ' || p_tbs_dato || ' PARTITION BY RANGE (DATEIN) (';
              
    FOR i IN -p_partitions_antes .. p_partitions_desp LOOP
       
    v_sql := v_sql || ' PARTITION P_THPINERR_'||TO_CHAR(ADD_MONTHS(v_fecha_base,i),'YYYYMM')||' VALUES LESS THAN ('''||TO_CHAR(TRUNC(ADD_MONTHS(v_fecha_base,i+1), 'MM'),'YYYY-MM-DD')||''')';
       
    IF i < p_partitions_desp THEN
            v_sql := v_sql || ', ';
        END IF;
    END LOOP; 
    
    v_sql := v_sql || ')';
    
    EXECUTE IMMEDIATE v_sql;
    v_tabla := 'THPINERR';
    v_tabla_legacy:= 'THPINERR_LEGACY';
    v_sql_ddl:= 'ALTER TABLE';

    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||' ADD CONSTRAINT "PK_THPAN" PRIMARY KEY ("PAN") USING INDEX TABLESPACE '||p_tbs_indice;

    v_out_mensaje:= v_out_mensaje||''||CHR(10)||'OK: Tabla THPINERR Particionada creada';
    DBMS_OUTPUT.PUT_LINE(v_out_mensaje);


EXCEPTION
  WHEN e_schemanovalido THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: El schema '||p_schema||' no es valido');
  WHEN e_tbsdatonovalido THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Tablespace de datos '||p_tbs_dato||' invalido');
  WHEN e_tbsindicenovalido THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Tablespace de indices '||p_tbs_indice||' inexistente');
  WHEN e_tablanoexiste THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: La Tabla '||v_tabla||' no esta creada');
  WHEN e_tablaparticionada THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: La Tabla '||v_tabla||' ya esta particionada');
  WHEN e_num_meses THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: El numero de particiones ingresado debe ser mayor a 0');
  WHEN OTHERS THEN 
        DBMS_OUTPUT.PUT_LINE(v_out_mensaje||''||CHR(10)||'ERROR: '||v_tabla||' Particionada: '||SQLERRM);
        return;
END;
/

spool off
exit

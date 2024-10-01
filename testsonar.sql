SET SERVEROUTPUT ON
spool 80PatchEFT20240930CreatePartitionPinerr.log

DECLARE
  p_schema := UPPER(&1.);
  p_tbs_dato := UPPER(&2.);
  p_tbs_indice := UPPER(&3.);
  p_partitions_antes NUMBER := &4-1; 
  p_partitions_desp NUMBER := &5;
  v_fecha_base date:= SYSDATE;
  v_sql VARCHAR2(8000);
  v_countpartition NUMBER;
  v_counttable NUMBER;
  v_countgeneral NUMBER;
  v_log VARCHAR2(30);

BEGIN
  SELECT COUNT(1) INTO v_countgeneral FROM ALL_USERS WHERE USERNAME= p_schema;
  IF v_countgeneral=0 THEN
  RAISE schemanovalido;
  END IF;

  SELECT COUNT(1) INTO v_countgeneral FROM USER_TABLESPACES WHERE tablespace_name= p_tbs_dato;
  IF v_countgeneral=0 THEN
  RAISE tbsdatonovalido;
  END IF;

  SELECT COUNT(1) INTO v_countgeneral FROM USER_TABLESPACES WHERE tablespace_name= p_tbs_indice;
  IF v_countgeneral=0 THEN
  RAISE tbsindicenovalido;
  END IF;
  v_log := 'Tabla PINERR';

  SELECT COUNT(1) INTO v_countpartition FROM all_tab_partitions WHERE table_owner= p_schema AND table_name = 'PINERR';
  SELECT COUNT(1) INTO v_counttable FROM all_tables WHERE owner=p_schema AND table_name = 'PINERR';

  IF v_countpartition= 0 AND v_counttable=1 THEN 
    
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = ''YYYY-MM-DD''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD''';

    EXECUTE IMMEDIATE 'ALTER TABLE '||p_schema||'.PINERR RENAME TO PINERR_LEGACY'; 
    EXECUTE IMMEDIATE 'ALTER TABLE '||p_schema||'.PINERR_LEGACY RENAME CONSTRAINT PK_PAN TO PK_PAN_LEGACY'; 
    EXECUTE IMMEDIATE 'ALTER INDEX '||p_schema||'.PK_PAN RENAME TO PK_PAN_LEGACY'; 


    v_sql := 'CREATE TABLE '||p_schema||'.PINERR (
    "PAN" VARCHAR2(64 BYTE) NOT NULL ENABLE, 
  	"RETRIES" NUMBER(2,0), 
  	"LAST_DATE" NUMBER(10,0), 
  	"TERMID" VARCHAR2(8 BYTE), 
  	"RETRIES_EXE" VARCHAR2(1 BYTE), 
  	"LAST_TIME" VARCHAR2(6 BYTE), 
  	"USERIN" VARCHAR2(24 BYTE) DEFAULT substr(user,1,24), 
  	"DATEIN" DATE DEFAULT sysdate, 
  	"USERCHG" VARCHAR2(24 BYTE), 
  	"DATECHG" DATE
                ) TABLESPACE '||p_tbs_dato||'
                PARTITION BY RANGE (DATEIN)
                (';
                
    FOR i IN -p_partitions_antes .. p_partitions_desp LOOP
         
      v_sql := v_sql || '  PARTITION P_PINERR_'||TO_CHAR(ADD_MONTHS(v_fecha_base,i),'YYYYMM')||' 
      VALUES LESS THAN ('''||TO_CHAR(TRUNC(ADD_MONTHS(v_fecha_base,i+1), 'MM'),'YYYY-MM-DD')||''')';
         
      IF i < p_partitions_desp THEN
        v_sql := v_sql || ', ';
      END IF;

    END LOOP; 
      
    v_sql := v_sql || ')';
      
    EXECUTE IMMEDIATE v_sql;

--COMMENTS
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN '||p_schema||'."PINERR"."PAN" IS ''Número de la tarjeta a la cual se le ingreso una clave errada.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN '||p_schema||'."PINERR"."RETRIES" IS ''Número de reintentos de ingreso de clave secreta.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN '||p_schema||'."PINERR"."LAST_DATE" IS ''Fecha del último ingreso de clave secreta errada.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN '||p_schema||'."PINERR"."TERMID" IS ''Número de terminal del último ingreso de clave secreta errada.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN '||p_schema||'."PINERR"."RETRIES_EXE" IS ''Indica si la tarjeta excediú el número de reintentos de ingresos de clave secreta.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN '||p_schema||'."PINERR"."LAST_TIME" IS ''Hora del último ingreso de clave secreta errada.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN '||p_schema||'."PINERR"."USERIN" IS ''Usuario que inserto el registro.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN '||p_schema||'."PINERR"."DATEIN" IS ''Fecha de inserciún del registro.  Formato: DD/MM/YY.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN '||p_schema||'."PINERR"."USERCHG" IS ''Ultimo usuario que modificó el registro.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN '||p_schema||'."PINERR"."DATECHG" IS ''Fecha de la ultima modificación realizada.  Formato: DD/MM/YY.''';
    EXECUTE IMMEDIATE 'COMMENT ON TABLE '||p_schema||'."PINERR"  IS ''Tabla que contiene la informaciún de los tarjetahabientes que han ingresado su clave errada con el fin de llevar la cuenta de la cantidad de PINES errados a soportar''';
 
    EXECUTE IMMEDIATE 'ALTER TABLE '||p_schema||'."PINERR" ADD CONSTRAINT "PK_PAN" PRIMARY KEY ("PAN")
    USING INDEX TABLESPACE '||p_tbs_indice;

    DBMS_OUTPUT.PUT_LINE('OK: Tabla PINERR Particionada creada');
  ELSE
    DBMS_OUTPUT.PUT_LINE('INFO: La tabla PINERR ya esta Particionada o no existe');
  END IF;

  v_log := 'Tabla THPINERR';

  SELECT COUNT(1) INTO v_countpartition FROM all_tab_partitions WHERE table_owner=p_schema AND table_name = 'THPINERR';
  SELECT COUNT(1) INTO v_counttable FROM all_tables WHERE owner=p_schema AND table_name = 'THPINERR';

  IF v_countpartition= 0 THEN 

    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = ''YYYY-MM-DD''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD''';

    v_sql := 'CREATE TABLE '||p_schema||'.THPINERR (
    "PAN" VARCHAR2(64 BYTE) NOT NULL ENABLE, 
    "RETRIES" NUMBER(2,0), 
    "LAST_DATE" NUMBER(10,0), 
    "TERMID" VARCHAR2(8 BYTE), 
    "RETRIES_EXE" VARCHAR2(1 BYTE), 
    "LAST_TIME" VARCHAR2(6 BYTE), 
    "USERIN" VARCHAR2(24 BYTE) DEFAULT substr(user,1,24), 
    "DATEIN" DATE DEFAULT sysdate, 
    "USERCHG" VARCHAR2(24 BYTE), 
    "DATECHG" DATE
                ) TABLESPACE '||p_tbs_dato||'
                PARTITION BY RANGE (DATEIN)
                (';
              
    FOR i IN -p_partitions_antes .. p_partitions_desp LOOP
       
    v_sql := v_sql || '  PARTITION P_THPINERR_'||TO_CHAR(ADD_MONTHS(v_fecha_base,i),'YYYYMM')||' 
    VALUES LESS THAN ('''||TO_CHAR(TRUNC(ADD_MONTHS(v_fecha_base,i+1), 'MM'),'YYYY-MM-DD')||''')';
       
    IF i < p_partitions_desp THEN
            v_sql := v_sql || ', ';
        END IF;
    END LOOP; 
    
    v_sql := v_sql || ')';
    
    EXECUTE IMMEDIATE v_sql;

    EXECUTE IMMEDIATE 'ALTER TABLE '||p_schema||'."THPINERR" ADD CONSTRAINT "PK_THPAN" PRIMARY KEY ("PAN")
    USING INDEX TABLESPACE '||p_tbs_indice;
    DBMS_OUTPUT.PUT_LINE('OK: Tabla THPINERR Particionada creada');
  ELSE
    DBMS_OUTPUT.PUT_LINE('INFO: La tabla THPINERR ya esta Particionada o no existe');
  END IF;


EXCEPTION
  WHEN schemanovalido THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: El schema '||p_schema||' no existe');
  WHEN tbsdatonovalido THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Tablespace '||p_tbs_dato||' no existe');
  WHEN tbsindicenovalido THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Tablespace '||p_tbs_indice||' no existe');
  WHEN OTHERS THEN 
    DBMS_OUTPUT.PUT_LINE('ERROR: '||v_log||' Particionada: '||SQLERRM);
    return;
END;
/

spool off
exit

--01CreacionDMLMTTOPART
--Crear un packete para agregar o dropear particiones en la tablas del schema
-- &1 : Schema OLC
SET SERVEROUTPUT ON
spool 01CreacionDMLMTTOPART.log

create or replace  PACKAGE          &1..PKG_MTTOPART AS


CONST_CODE_OK CONSTANT NUMBER := 0;
CONST_CODE_ERROR CONSTANT NUMBER := -1;
CONST_CODE_NOPARTITION CONSTANT NUMBER := 1;
CONST_CODE_LOCK CONSTANT NUMBER := 2;
V_MAX_RETRIES  NUMBER:= 10;
V_RETRY_COUNT NUMBER:= 0;

type data_rec is record (pl   VARCHAR2(100));
type nombre_particiones_array is table of data_rec;

PROCEDURE SPP_MAIN_PART_MES_SIN_FK (
p_sqlcode           OUT NUMBER,
p_sqlerrm           OUT VARCHAR2);

PROCEDURE SPP_CREATE_PART_MES_SIN_FK
(
p_fecha_base        IN VARCHAR2,        
p_num_meses         IN NUMBER,          
p_tablespace        IN VARCHAR2,       
p_name_table        IN VARCHAR2,
p_tipo_campodate    IN VARCHAR2,
p_sqlcode           OUT NUMBER,
p_sqlerrm           OUT VARCHAR2
);

PROCEDURE SPP_DROP_PART_MES_SIN_FK(
p_fecha_base       IN VARCHAR2,        
p_num_meses        IN NUMBER,         
p_name_table       IN VARCHAR2,
p_sqlcode          OUT NUMBER,
p_sqlerrm          OUT VARCHAR2);


PROCEDURE sleep_time;

PROCEDURE SPI_INSERT_PART_SIN_FK(
        p_fecha_base            IN VARCHAR2,
        p_schema                IN VARCHAR2,        
        p_table_name            IN VARCHAR2,
        p_table_name_his        IN VARCHAR2,
        p_campo_fecha           IN VARCHAR2,
        p_tipo_fecha            IN VARCHAR2,

       -- p_arr_errores           IN v_array,
       -- p_num_reintentos        IN INTEGER,
        p_sqlcode           OUT NUMBER,
        p_sqlerrm           OUT VARCHAR2
    );

FUNCTION FUN_obtener_primer_dia_particion(
    nombre_particion IN VARCHAR2
) RETURN TIMESTAMP;


FUNCTION FUN_obtener_ultimo_dia_particion(
    nombre_particion IN VARCHAR2
) RETURN TIMESTAMP;

PROCEDURE SPU_INDEXESNOLOGGING (
    p_schema IN VARCHAR2,
    p_table_name IN VARCHAR2
);

PROCEDURE SPU_INDEXESLOGGING (
    p_schema IN VARCHAR2,
    p_table_name IN VARCHAR2
);


PROCEDURE SPS_obtener_particiones(
    fecha_inicio IN TIMESTAMP,
    fecha_fin IN TIMESTAMP,
    nombre_tabla IN VARCHAR2,
    particiones IN OUT nombre_particiones_array  
) ;


FUNCTION FUN_obt_max_fecha_partxn_times(
    p_table_name    IN VARCHAR2,  
    p_partition_name IN VARCHAR2, 
    p_fecha_field   IN VARCHAR2,
    p_tipo_fecha    IN VARCHAR2
) RETURN TIMESTAMP;

FUNCTION FUN_obt_actual_part(p_table_name IN VARCHAR2)
RETURN VARCHAR2;

FUNCTION fun_obtener_ultima_particion(p_name_table IN VARCHAR2)
RETURN VARCHAR2;

FUNCTION fun_obtener_primera_particion(p_name_table IN VARCHAR2)
RETURN VARCHAR2;

FUNCTION FUN_obt_min_fecha_partxn_times(
    p_table_name    IN VARCHAR2,  
    p_partition_name IN VARCHAR2, 
    p_fecha_field   IN VARCHAR2,
    p_tipo_fecha    IN VARCHAR2
) RETURN TIMESTAMP;

FUNCTION FUN_obt_max_fecha_parth_times(
    p_table_name    IN VARCHAR2,  
    p_partition_name IN VARCHAR2, 
    p_fecha_field   IN VARCHAR2,
    p_tipo_fecha    IN VARCHAR2
) RETURN TIMESTAMP;

FUNCTION FUN_obt_primer_part(p_table_name IN VARCHAR2)
RETURN VARCHAR2;


FUNCTION FUN_validar_particion_existente(
    p_table_name IN VARCHAR2,
    p_partition_name IN VARCHAR2
) RETURN BOOLEAN ;


PROCEDURE SPI_PART_LOG (
    p_schema IN VARCHAR2,
    p_tip_part_log IN VARCHAR2,
    p_desc_tip_part_log IN VARCHAR2,
    p_desc_part_log IN VARCHAR2,
    p_nom_procedure IN VARCHAR2,
    p_nom_tabla IN VARCHAR2
);

END PKG_MTTOPART;
/

create or replace PACKAGE BODY   &1..PKG_MTTOPART AS

PROCEDURE SPI_PART_LOG (
    p_schema IN VARCHAR2,
    p_tip_part_log IN VARCHAR2,
    p_desc_tip_part_log IN VARCHAR2,
    p_desc_part_log IN VARCHAR2,
    p_nom_procedure IN VARCHAR2,
    p_nom_tabla IN VARCHAR2
) IS
BEGIN
    EXECUTE IMMEDIATE 'INSERT INTO "' || p_schema || '"."TP_PART_LOG" 
                       (COD_PART_LOG, TIP_PART_LOG, DESC_TIP_PART_LOG, DESC_PART_LOG, 
                        NOM_PROCEDURE, NOM_TABLA, FEC_CREA, USU_CREA) 
                       VALUES (SEQ_PART_LOG.NEXTVAL, :1, :2, :3, :4, :5, SYSTIMESTAMP, :6)' 
    USING p_tip_part_log, p_desc_tip_part_log, 
          p_desc_part_log, p_nom_procedure, 
          p_nom_tabla, p_schema;  -- Usar el nombre del esquema como USU_CREA
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END SPI_PART_LOG;


PROCEDURE SPP_MAIN_PART_MES_SIN_FK (
p_sqlcode           OUT NUMBER,
p_sqlerrm           OUT VARCHAR2)
AS
  v_fila TP_PART_CONF%ROWTYPE;
  CURSOR cur_particiones IS SELECT * FROM TP_PART_CONF;
  v_tabla VARCHAR2(40);
  v_tablespace VARCHAR2(40);
  fecha_base VARCHAR2(8);
  v_query_count VARCHAR2(400);
  v_countgeneral NUMBER;
  v_schema VARCHAR2(50);
  e_finalizar               EXCEPTION;
  e_tablanoparticionada     EXCEPTION;
  e_tbsdatonovalido         EXCEPTION;
  v_procedure VARCHAR2(24) :='SPP_MAIN_PART_MES_SIN_FK';

BEGIN

  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = ''YYYYMMDD HH24MISS''';
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYYMMDD''';
  fecha_base :=TO_CHAR(sysdate,'YYYYMMDD');
  SELECT UPPER(USER) INTO v_schema FROM dual;

  SPI_PART_LOG(v_schema,CONST_CODE_OK,'OK','************Inicio ejecucion MANTENIMIENTO MENSUAL MAIN '||fecha_base||' ************',v_procedure,'******'); COMMIT;

OPEN cur_particiones;
   LOOP
      FETCH cur_particiones INTO v_fila;
      EXIT WHEN cur_particiones%NOTFOUND;

        IF v_fila.ESTADO_EJECUCION <> 0 THEN
         CONTINUE;
        END IF;

        SPI_PART_LOG(v_fila.NOM_SCHEMA,CONST_CODE_OK,'OK','Inicio de validaciones ...',v_procedure,v_fila.NOM_TABLA); COMMIT;

        v_tablespace:=v_fila.NOM_TBSDATOS;
        v_query_count := q'{SELECT COUNT(1) FROM USER_TABLESPACES WHERE tablespace_name= :1}';
        EXECUTE IMMEDIATE v_query_count INTO v_countgeneral USING v_tablespace;
        IF v_countgeneral=0 THEN
        RAISE e_tbsdatonovalido;
        END IF;

        v_tabla:= v_fila.NOM_TABLA;
        v_query_count := q'{SELECT COUNT(1) FROM all_tab_partitions WHERE table_owner= :1 AND table_name = :2}';
        EXECUTE IMMEDIATE v_query_count INTO v_countgeneral USING v_fila.NOM_SCHEMA,v_tabla;
        IF v_countgeneral = 0 THEN
        RAISE e_tablanoparticionada;
        END IF;


        p_sqlcode:=CONST_CODE_LOCK;
        WHILE p_sqlcode=CONST_CODE_LOCK LOOP
        SPP_CREATE_PART_MES_SIN_FK(fecha_base,v_fila.NUM_PART_ADELANTE,v_fila.NOM_TBSDATOS,
        v_fila.NOM_TABLA,v_fila.TIP_CAMPO_FECHA_PART,p_sqlcode,p_sqlerrm);

        IF p_sqlcode = CONST_CODE_OK THEN
        SPI_PART_LOG(v_fila.NOM_SCHEMA,CONST_CODE_OK,'OK','Fin ADD particiones en:'||v_fila.NOM_TABLA,v_procedure,v_fila.NOM_TABLA); COMMIT;
        ELSIF p_sqlcode = CONST_CODE_NOPARTITION THEN
          NULL;
        ELSIF p_sqlcode = CONST_CODE_ERROR THEN
          RAISE e_finalizar;
        ELSIF p_sqlcode = CONST_CODE_LOCK THEN
          sleep_time();
        ELSE
          RAISE e_finalizar;        
        END IF;
        END LOOP;
        V_RETRY_COUNT:=0;

        p_sqlcode:=CONST_CODE_LOCK;
        WHILE p_sqlcode=CONST_CODE_LOCK LOOP
        SPP_CREATE_PART_MES_SIN_FK(fecha_base,v_fila.NUM_THPART_ADELANTE,v_fila.NOM_TBSDATOS,
        v_fila.NOM_THTABLA,v_fila.TIP_CAMPO_FECHA_PART,p_sqlcode,p_sqlerrm);
        IF p_sqlcode = CONST_CODE_OK THEN
        SPI_PART_LOG(v_fila.NOM_SCHEMA,CONST_CODE_OK,'OK','Fin ADD particiones en:'||v_fila.NOM_THTABLA,v_procedure,v_fila.NOM_THTABLA); COMMIT;
        ELSIF p_sqlcode = CONST_CODE_NOPARTITION THEN
          NULL;
        ELSIF p_sqlcode = CONST_CODE_ERROR THEN
          RAISE e_finalizar;
        ELSIF p_sqlcode = CONST_CODE_LOCK THEN
          sleep_time();
        ELSE
          RAISE e_finalizar;        
        END IF;
        END LOOP;
        V_RETRY_COUNT:=0;
        
IF v_fila.FLG_MOVEDATATH = 'SI' THEN
        p_sqlcode:=CONST_CODE_LOCK;
        WHILE p_sqlcode=CONST_CODE_LOCK LOOP
        SPI_INSERT_PART_SIN_FK(fecha_base,v_fila.NOM_SCHEMA,v_fila.NOM_TABLA,v_fila.NOM_THTABLA,v_fila.NOM_CAMPO_FECHA_PART,v_fila.TIP_CAMPO_FECHA_PART,     
        p_sqlcode,p_sqlerrm);

        IF p_sqlcode = CONST_CODE_OK THEN
        SPI_PART_LOG(v_fila.NOM_SCHEMA,CONST_CODE_OK,'OK','Fin Carga data de:'||v_fila.NOM_TABLA||' --> '||v_fila.NOM_THTABLA,v_procedure,v_fila.NOM_THTABLA); COMMIT;
        ELSIF p_sqlcode = CONST_CODE_NOPARTITION THEN
          NULL;
        ELSIF p_sqlcode = CONST_CODE_ERROR THEN
          RAISE e_finalizar;
        ELSIF p_sqlcode = CONST_CODE_LOCK THEN
          sleep_time();
        ELSE
          RAISE e_finalizar;        
        END IF;
        END LOOP;
        V_RETRY_COUNT:=0;
END IF;

        p_sqlcode:=CONST_CODE_LOCK;
        WHILE p_sqlcode=CONST_CODE_LOCK LOOP
        SPP_DROP_PART_MES_SIN_FK(fecha_base,v_fila.NUM_PART_ATRAS,v_fila.NOM_TABLA,p_sqlcode,p_sqlerrm);

        IF p_sqlcode = CONST_CODE_OK THEN
        SPI_PART_LOG(v_fila.NOM_SCHEMA,CONST_CODE_OK,'OK','Fin DROP particiones en:'||v_fila.NOM_TABLA,v_procedure,v_fila.NOM_TABLA); COMMIT;
        ELSIF p_sqlcode = CONST_CODE_NOPARTITION THEN
          NULL;
        ELSIF p_sqlcode = CONST_CODE_ERROR THEN
          RAISE e_finalizar;
        ELSIF p_sqlcode = CONST_CODE_LOCK THEN
          sleep_time();
        ELSE
          RAISE e_finalizar;        
        END IF;    
        END LOOP;
        V_RETRY_COUNT:=0;

        p_sqlcode:=CONST_CODE_LOCK;
        WHILE p_sqlcode=CONST_CODE_LOCK LOOP
        SPP_DROP_PART_MES_SIN_FK(fecha_base,v_fila.NUM_THPART_ATRAS,v_fila.NOM_THTABLA,p_sqlcode,p_sqlerrm);

        IF p_sqlcode = CONST_CODE_OK THEN
        SPI_PART_LOG(v_fila.NOM_SCHEMA,CONST_CODE_OK,'OK','Fin DROP particiones en:'||v_fila.NOM_THTABLA,v_procedure,v_fila.NOM_THTABLA); COMMIT;
        ELSIF p_sqlcode = CONST_CODE_NOPARTITION THEN
          NULL;
        ELSIF p_sqlcode = CONST_CODE_ERROR THEN
          RAISE e_finalizar;
        ELSIF p_sqlcode = CONST_CODE_LOCK THEN
          sleep_time();
        ELSE
          RAISE e_finalizar;        
        END IF;    
        END LOOP;
        V_RETRY_COUNT:=0;

        p_sqlcode := CONST_CODE_OK;
        p_sqlerrm:= 'OK Fin ejecucion MANTENIMIENTO MENSUAL MAIN '||fecha_base;

        -- Mostrar el contenido de cada campo de la fila
       /* DBMS_OUTPUT.PUT_LINE('Producto: ' || v_fila.producto);
        DBMS_OUTPUT.PUT_LINE('Tabla: ' || v_fila.NOM_TABLA);
        DBMS_OUTPUT.PUT_LINE('Fecha de ParticiÃ³n: ' || v_fila.fecha_part);
        DBMS_OUTPUT.PUT_LINE('Tipo de Fecha de ParticiÃ³n: ' || v_fila.TIP_CAMPO_FECHA_PART);
        DBMS_OUTPUT.PUT_LINE('Flag Mover Historial: ' || v_fila.FLG_MOVEDATATH);
        DBMS_OUTPUT.PUT_LINE('Tabla Historial: ' || v_fila.NOM_THTABLA);
        DBMS_OUTPUT.PUT_LINE('Flag FK: ' || v_fila.flag_fk);
        DBMS_OUTPUT.PUT_LINE('Detalle FK: ' || v_fila.det_fk);
        DBMS_OUTPUT.PUT_LINE('Particiones Atras: ' || v_fila.NUM_PART_ATRAS);
        DBMS_OUTPUT.PUT_LINE('Particiones Adelante: ' || v_fila.NUM_PART_ADELANTE);
        DBMS_OUTPUT.PUT_LINE('Periodo: ' || v_fila.periodo);
        DBMS_OUTPUT.PUT_LINE('Tablespace: ' || v_fila.NOM_TBSDATOS);
        DBMS_OUTPUT.PUT_LINE('Flag PAM: ' || v_fila.flag_pam);
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');*/
    END LOOP;
    CLOSE cur_particiones;

        SPI_PART_LOG(v_schema,CONST_CODE_OK,'OK','************OK Fin ejecucion MANTENIMIENTO MENSUAL MAIN '||fecha_base||' ************',v_procedure,'******'); COMMIT;

COMMIT;
EXCEPTION
  WHEN e_finalizar THEN 
    return;
  WHEN e_tablanoparticionada THEN 
        p_sqlcode := CONST_CODE_NOPARTITION;
        p_sqlerrm := 'INFO..La tabla '||v_tabla||' no esta particionada';
  WHEN e_tbsdatonovalido THEN
        p_sqlcode := CONST_CODE_NOPARTITION;
        p_sqlerrm := 'ERROR: Tablespace de datos '||v_tablespace||' invalido';
  WHEN OTHERS THEN
        p_sqlcode := CONST_CODE_ERROR;
        p_sqlerrm := 'ERROR..'||SQLCODE||''||SUBSTR (SQLERRM, 1, 200);
        return;
END SPP_MAIN_PART_MES_SIN_FK;


/* ***************************************************************************************  */
/* Nombre                : SPP_CREATE_PART_MES_SIN_FK                                    */
/* Descripcion           : Permite agregar particiones mensuales a tablas transaccionales   */
/* **************************************************************************************** */

PROCEDURE SPP_CREATE_PART_MES_SIN_FK
  ( p_fecha_base        IN VARCHAR2,        
    p_num_meses         IN NUMBER,          
    p_tablespace        IN VARCHAR2,       
    p_name_table            IN VARCHAR2,
    p_tipo_campodate    IN VARCHAR2,
    p_sqlcode           OUT NUMBER,
    p_sqlerrm           OUT VARCHAR2
  )
  AS
      v_flag               INTEGER := 0;
      v_DateNow            DATE;
      v_DateNew            DATE;
      v_DateLastPart       DATE;
      v_StrCmd             VARCHAR2(512) := '';
      v_StrDateNewPart     VARCHAR2(8) := '';
      v_StrDateNewPartMax  VARCHAR2(8) := NULL;
      v_StrDateNow         VARCHAR2(8) := '';
      v_namePartition      VARCHAR2(80) := '';
      v_Idx                NUMBER := 0;
      v_NumMes             NUMBER;
      v_sqlcode            NUMBER := CONST_CODE_OK;
      v_sqlerrm            VARCHAR2(200) := '';
      v_lastday           VARCHAR2(8);
      v_schema            VARCHAR2(40);
      v_procedure         VARCHAR2(26):='SPP_CREATE_PART_MES_SIN_FK';
      e_num_meses          EXCEPTION;
      e_tablespace         EXCEPTION;
      e_no_add_partition   EXCEPTION;
  BEGIN
      p_sqlcode := CONST_CODE_OK;
      p_sqlerrm := 'OK...ADD PARTITION';
      v_flag := 0;
      v_DateNow := TO_DATE(p_fecha_base,'YYYYMMDD');
      SELECT UPPER(USER) INTO v_schema FROM dual;

      SPI_PART_LOG(v_schema,p_sqlcode,'OK','Inicio ADD particiones en:'||p_name_table,v_procedure,p_name_table); COMMIT;

      IF p_num_meses <= 0 THEN
           RAISE e_num_meses;
      END IF;

      IF TRIM(p_tablespace) IS NULL THEN
           RAISE e_tablespace;
      END IF;

      v_namePartition := fun_obtener_ultima_particion(p_name_table);
      v_DateLastPart := TO_DATE(SUBSTR(v_namePartition,-6,8)||'01', 'YYYYMMDD');
      v_DateNew      := ADD_MONTHS(v_DateNow, p_num_meses);
      v_NumMes       := TRUNC(MONTHS_BETWEEN(v_DateNew,v_DateLastPart));

      IF v_DateLastPart < v_DateNew THEN
            -- EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYYMMDD''';
                FOR v_Idx IN 1 .. v_NumMes LOOP
                    v_StrDateNewPart    := TO_CHAR(ADD_MONTHS(v_DateNow, v_Idx),'YYYYMM'); 
                    v_StrDateNewPart    := TO_CHAR(ADD_MONTHS(v_DateLastPart,v_Idx), 'YYYYMM'); --add mes+1 al limite de la particion values less a partir del nombre de la ultima particion
                    v_StrDateNewPartMax := TO_CHAR(ADD_MONTHS(v_DateLastPart, v_Idx + 1) , 'YYYYMM')||'01';

                    IF p_tipo_campodate = 'VARCHAR2' THEN
                      v_StrCmd := 'ALTER TABLE '||p_name_table||' ADD PARTITION P_'||p_name_table||'_' || v_StrDateNewPart || ' VALUES LESS THAN ('''|| v_StrDateNewPartMax || ''') TABLESPACE '|| p_tablespace;
                    ELSE
                      v_StrCmd := 'ALTER TABLE '||p_name_table||' ADD PARTITION P_'||p_name_table||'_' || v_StrDateNewPart || ' VALUES LESS THAN (TO_TIMESTAMP('''|| v_StrDateNewPartMax || ''',''YYYYMMDD'')) TABLESPACE '|| p_tablespace;
                    END IF;
                    EXECUTE IMMEDIATE v_StrCmd;
                    SPI_PART_LOG(v_schema,p_sqlcode,'OK','Se agrego la particion: P_'||p_name_table||'_' || v_StrDateNewPart,v_procedure,p_name_table); COMMIT;
                    v_flag := 1;

                END LOOP;
      END IF;

    IF v_flag = 0 THEN
      RAISE e_no_add_partition;
    END IF;

  EXCEPTION
    WHEN e_num_meses THEN
       p_sqlcode := CONST_CODE_ERROR;
       p_sqlerrm := 'ERROR..Numero de meses ADD incorrecto : '||p_num_meses;
       SPI_PART_LOG(v_schema,p_sqlcode,'ERROR','Numero de meses ADD incorrecto : '||p_num_meses,v_procedure,p_name_table); COMMIT;
    WHEN e_tablespace THEN
       p_sqlcode := CONST_CODE_ERROR;
       p_sqlerrm := 'ERROR..Falta parametro tablespace ';
       SPI_PART_LOG(v_schema,p_sqlcode,'ERROR','Falta parametro tablespace',v_procedure,p_name_table); COMMIT;

    WHEN e_no_add_partition THEN
       p_sqlcode := CONST_CODE_NOPARTITION;
       p_sqlerrm := 'INFO..Aun no esta en limite del mes para adicionar particion '||p_fecha_base || '--> '||v_namePartition;
       SPI_PART_LOG(v_schema,p_sqlcode,'INFO','Aun no esta en limite del mes para adicionar particion '||p_fecha_base || '--> '||v_namePartition,v_procedure,p_name_table); COMMIT;

    WHEN OTHERS THEN  
      v_sqlcode := SQLCODE;
      v_sqlerrm := SQLERRM;
      p_sqlcode := v_sqlcode;
      p_sqlerrm := SUBSTR (v_sqlerrm, 1, 200);
      V_RETRY_COUNT:=V_RETRY_COUNT+1;
      
      IF (v_sqlcode = -54 OR v_sqlcode = -60) AND V_RETRY_COUNT<=V_MAX_RETRIES THEN
        p_sqlcode := CONST_CODE_LOCK;
        p_sqlerrm := 'INFO..Se ha generado el bloqueo ('||V_RETRY_COUNT||'/'||V_MAX_RETRIES||')';
       SPI_PART_LOG(v_schema,p_sqlcode,'INFO','Se ha generado el bloqueo ('||V_RETRY_COUNT||'/'||V_MAX_RETRIES||')',v_procedure,p_name_table); COMMIT;    

      ELSE
       SPI_PART_LOG(v_schema,p_sqlcode,'ERROR',p_sqlerrm,v_procedure,p_name_table); COMMIT;    
      END IF;
      
      IF V_RETRY_COUNT > V_MAX_RETRIES THEN
        p_sqlcode := CONST_CODE_ERROR;
        p_sqlerrm := 'ERROR..Se alcanzo el maximo de reintentos ('||V_MAX_RETRIES||')';
       SPI_PART_LOG(v_schema,p_sqlcode,'ERROR','Se alcanzo el maximo de reintentos ('||V_MAX_RETRIES||')',v_procedure,p_name_table); COMMIT;    
      END IF;

END SPP_CREATE_PART_MES_SIN_FK;

/* ***************************************************************************************  */
/* Nombre                : SPP_DROP_PART_MES_SIN_FK                                    */
/* Descripcion           : Permite dropear particiones mensuales a tablas transaccionales   */
/* **************************************************************************************** */

PROCEDURE SPP_DROP_PART_MES_SIN_FK(
  p_fecha_base       IN VARCHAR2,        
  p_num_meses        IN NUMBER,         
  p_name_table       IN VARCHAR2,
  p_sqlcode          OUT NUMBER,
  p_sqlerrm          OUT VARCHAR2) 
  AS
      v_flag               INTEGER := 0;
      v_dif_mes            INTEGER := 0;
      v_DateNow            DATE;
      v_DateFirstPart1     DATE;
      v_DateFirstPart2     DATE;
      v_StrCmd             VARCHAR2(512) := '';
      v_StrDateDropPart    VARCHAR2(8) := '';
      v_StrDateNewPartMax  VARCHAR2(8) := NULL;
      v_StrDateNow         VARCHAR2(8) := '';
      v_namePartition      VARCHAR2(40) := '';
      v_Idx                NUMBER := 0;
      v_NumDays            NUMBER;
      v_sqlcode            NUMBER := CONST_CODE_OK;
      v_sqlerrm            VARCHAR2(200) := '';
      v_lastday           VARCHAR2(8);
      v_schema            VARCHAR2(40);
      v_procedure         VARCHAR2(40):= 'SPP_DROP_PART_MES_SIN_FK';

      e_no_drop            EXCEPTION;
      e_num_meses          EXCEPTION;
      v_num_reintentos NUMBER;
  BEGIN

      p_sqlcode := CONST_CODE_OK;
      p_sqlerrm := 'OK.. DROP PARTITION DE TABLA '||p_name_table;
      v_DateNow := TO_DATE(p_fecha_base,'YYYYMMDD');
      v_flag := 0;
      SELECT UPPER(USER) INTO v_schema FROM dual;

      SPI_PART_LOG(v_schema,p_sqlcode,'OK','DROP PARTITION DE TABLA '||p_name_table,v_procedure,p_name_table); COMMIT;

      IF p_num_meses <= 0 THEN
         RAISE e_num_meses;
      END IF;    
      v_namePartition := fun_obtener_primera_particion(p_name_table);

      --2.- Se obtiene nueva fecha y la probable primera partition --- se refiere a la nueva fecha para la primera particion
      v_DateFirstPart1 := ADD_MONTHS(v_DateNow, p_num_meses * -1);
      --3.- Convierte primnera partition en fecha  ---
      v_DateFirstPart2:= TO_DATE(SUBSTR(v_namePartition,-6,8)||'01', 'YYYYMMDD');
      --4.- se compara las fechas para proceder a eliminar las particiones mas antiguas ----
      IF v_DateFirstPart1 >= v_DateFirstPart2 THEN
         v_dif_mes := TRUNC(MONTHS_BETWEEN(v_DateFirstPart1,v_DateFirstPart2));  

         -- obtiene el numero de meses diferecias
         FOR v_idx IN 0..v_dif_mes LOOP
             v_StrDateDropPart := TO_CHAR(ADD_MONTHS(v_DateFirstPart2,v_Idx), 'YYYYMM'); 
             v_StrCmd := 'ALTER TABLE '||p_name_table||' DROP PARTITION ' || 'P_'||p_name_table||'_'||v_StrDateDropPart || ' UPDATE GLOBAL INDEXES';
               EXECUTE IMMEDIATE v_StrCmd; 
               SPI_PART_LOG(v_schema,p_sqlcode,'OK','Se dropeo la particion ' || 'P_'||p_name_table||'_'||v_StrDateDropPart,v_procedure,p_name_table); COMMIT;
            v_flag := 1;
         END LOOP;

      END IF;      

      IF v_flag = 0 THEN
         RAISE e_no_drop;
      END IF;

  EXCEPTION
    WHEN e_num_meses THEN
       p_sqlcode := CONST_CODE_ERROR;
       p_sqlerrm := 'ERROR.. Numero de meses DROP incorrecto : '||p_num_meses;
       SPI_PART_LOG(v_schema,p_sqlcode,'ERROR','Numero de meses DROP incorrecto : '||p_num_meses,v_procedure,p_name_table); COMMIT;
    WHEN e_no_drop THEN
       p_sqlcode := CONST_CODE_NOPARTITION;
       p_sqlerrm := 'INFO.. No esta aun en limite para DROP '||p_fecha_base || '--> '||v_namePartition;
       SPI_PART_LOG(v_schema,p_sqlcode,'INFO','No esta aun en limite para DROP '||p_fecha_base || '--> '||v_namePartition,v_procedure,p_name_table); COMMIT;

    WHEN OTHERS THEN
      v_sqlcode := SQLCODE;
      v_sqlerrm := SQLERRM;
      p_sqlcode := v_sqlcode;
      p_sqlerrm := SUBSTR (v_sqlerrm, 1, 200);
      V_RETRY_COUNT:=V_RETRY_COUNT+1;

      IF (v_sqlcode = -54 OR v_sqlcode = -60) AND V_RETRY_COUNT<=V_MAX_RETRIES THEN
        p_sqlcode := CONST_CODE_LOCK;
        p_sqlerrm := 'INFO..Se ha generado el bloqueo ('||V_RETRY_COUNT||'/'||V_MAX_RETRIES||')';
        SPI_PART_LOG(v_schema,p_sqlcode,'INFO','Se ha generado el bloqueo en DROP ('||V_RETRY_COUNT||'/'||V_MAX_RETRIES||')',v_procedure,p_name_table); COMMIT;    
       ELSE
        SPI_PART_LOG(v_schema,p_sqlcode,'ERROR',p_sqlerrm,v_procedure,p_name_table); COMMIT;    
       END IF;
      IF V_RETRY_COUNT > V_MAX_RETRIES THEN
        p_sqlcode := CONST_CODE_ERROR;
        p_sqlerrm := 'ERROR..Se alcanzo el maximo de reintentos ('||V_MAX_RETRIES||')';
        SPI_PART_LOG(v_schema,p_sqlcode,'ERROR','Se alcanzo el maximo de reintentos ('||V_MAX_RETRIES||')',v_procedure,p_name_table); COMMIT;    
      
      END IF;
END SPP_DROP_PART_MES_SIN_FK;

/* ***************************************************************************************  */
/* Nombre                : SPI_INSERT_PART_SIN_FK                                               */
/* Descripcion           : Permite insertar data de una particion de tabla transaccional    */
/*                         a su tabla historica                                             */
/* **************************************************************************************** */
PROCEDURE SPI_INSERT_PART_SIN_FK(
        p_fecha_base            IN VARCHAR2,
        p_schema                IN VARCHAR2,
        p_table_name            IN VARCHAR2,
        p_table_name_his        IN VARCHAR2,
        p_campo_fecha           IN VARCHAR2,
        p_tipo_fecha            IN VARCHAR2,
        p_sqlcode           OUT NUMBER,
        p_sqlerrm           OUT VARCHAR2
    )
    IS
        v_sqlcode            NUMBER := CONST_CODE_OK;
        v_sqlerrm            VARCHAR2(200) := '';
        move_data_query      VARCHAR2(1950);
        v_part_act_txt       VARCHAR2(50);
        v_part_act_his       VARCHAR2(50);
        v_part_primer_txt       VARCHAR2(50);
        v_fecha_inicio       TIMESTAMP(8);
        v_fecha_fin          TIMESTAMP(8);
        v_fecha_iniciotxn    TIMESTAMP(8);

        v_fecha_iniciop       TIMESTAMP(8);
        v_fecha_finp          TIMESTAMP(8);
        v_fecha_iniciopv       VARCHAR2(8);
        v_fecha_iniciotxnpv       VARCHAR2(8);

        v_fecha_finpv         VARCHAR2(8);
        v_where         VARCHAR2(1500);

        particiones nombre_particiones_array := nombre_particiones_array();
        particion             VARCHAR2(100);
        v_primaria            VARCHAR2(100);
        v_schema              VARCHAR2(40);
        v_procedure           VARCHAR2(22):= 'SPI_INSERT_PART_SIN_FK';

        e_sindatacarga          EXCEPTION;

        v_num_reintentos        INTEGER;
        ADD_PARTITION_EXCEPTION EXCEPTION;

    BEGIN
     
     p_sqlcode := CONST_CODE_OK;
     p_sqlerrm := 'OK...SPI_INSERT_PART_SIN_FK';
     SELECT UPPER(USER) INTO v_schema FROM dual;
     EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYYMMDD''';

     SPI_PART_LOG(v_schema,p_sqlcode,'OK','Inicio de Cargar datos a historica:'||p_table_name_his,v_procedure,p_table_name); COMMIT;

     v_part_act_his:= FUN_obt_actual_part(p_table_name_his);
     v_part_act_txt:= FUN_obt_actual_part(p_table_name);
     v_part_primer_txt:=FUN_obt_primer_part(p_table_name);
     v_fecha_inicio:= FUN_obt_max_fecha_parth_times(p_table_name_his, v_part_act_his, p_campo_fecha, p_tipo_fecha);
     v_fecha_fin := FUN_obt_max_fecha_partxn_times(p_table_name, v_part_act_txt, p_campo_fecha, p_tipo_fecha);
     v_fecha_iniciotxn:= FUN_obt_min_fecha_partxn_times(p_table_name, v_part_primer_txt, p_campo_fecha, p_tipo_fecha);

        IF p_tipo_fecha='VARCHAR2' THEN
            v_fecha_iniciopv:=TO_CHAR(v_fecha_inicio,'YYYYMMDD')||'';
            v_fecha_iniciotxnpv:=TO_CHAR(v_fecha_iniciotxn,'YYYYMMDD')||'';
            IF v_fecha_iniciotxnpv > v_fecha_iniciopv THEN
              v_fecha_inicio := v_fecha_iniciotxn; 
            END IF;
        ELSE
            IF v_fecha_iniciotxn > v_fecha_inicio THEN
              v_fecha_inicio := v_fecha_iniciotxn;  
            END IF;
        END IF;

      IF v_fecha_inicio >= v_fecha_fin THEN
      RAISE e_sindatacarga;
      END IF;
      
      SPS_obtener_particiones(v_fecha_inicio,v_fecha_fin,p_table_name,particiones);
 
    SPU_INDEXESNOLOGGING(p_schema,p_table_name_his);

    FOR i IN particiones.FIRST .. particiones.LAST LOOP
        particion:=particiones(i).pl||'';
        SPI_PART_LOG(v_schema,p_sqlcode,'OK','Cargando data desde la particion:'||particion,v_procedure,p_table_name); COMMIT;

        IF i = 1 THEN
              v_fecha_iniciop:=v_fecha_inicio;
              v_fecha_finp:=FUN_obtener_ultimo_dia_particion(particion);
        ELSIF i=particiones.COUNT THEN
              v_fecha_iniciop:=FUN_obtener_primer_dia_particion(particion);
              v_fecha_finp:=v_fecha_fin;
        ELSE
              v_fecha_iniciop:=FUN_obtener_primer_dia_particion(particion);
              v_fecha_finp:=FUN_obtener_ultimo_dia_particion(particion);
        END IF;
     
        IF EXTRACT(YEAR FROM v_fecha_inicio) = EXTRACT(YEAR FROM v_fecha_fin) AND
           EXTRACT(MONTH FROM v_fecha_inicio) = EXTRACT(MONTH FROM v_fecha_fin) AND
           i = 1 THEN
              v_fecha_iniciop:=v_fecha_inicio;
              v_fecha_finp:=v_fecha_fin; 
        END IF;
 
    IF p_tipo_fecha='VARCHAR2' THEN
      v_fecha_iniciopv:=TO_CHAR(v_fecha_iniciop,'YYYYMMDD');
      v_fecha_finpv:=TO_CHAR(v_fecha_finp,'YYYYMMDD');
      v_where:= 'WHERE '||p_campo_fecha||' BETWEEN '''||v_fecha_iniciopv||''' AND '''||v_fecha_finpv||'''';
      SPI_PART_LOG(v_schema,p_sqlcode,'OK','Inicio de cargar data desde:'||v_fecha_iniciopv||' -> '||v_fecha_finpv,v_procedure,p_table_name); COMMIT;

    ELSE
      EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYYMMDD HH24MISS''';
      v_where:= 'WHERE '||p_campo_fecha||' BETWEEN '''||v_fecha_iniciop||''' AND '''||v_fecha_finp||'''';
      SPI_PART_LOG(v_schema,p_sqlcode,'OK','Inicio de cargar data de:'||v_fecha_iniciop||' -> '||v_fecha_finp,v_procedure,p_table_name); COMMIT;
    END IF;


            move_data_query:='
                DECLARE
                    TYPE tipo_arreglo IS TABLE OF '||p_table_name||'%ROWTYPE INDEX BY BINARY_INTEGER;
                    lr_datos tipo_arreglo;
                    CURSOR cur_insert IS
                    SELECT /*+ PARALLEL('||p_table_name||',10) */ *
                    FROM '||p_table_name||' PARTITION ('||particion||')
                     '||v_where||';
                BEGIN
                    OPEN cur_insert;
                    LOOP
                        FETCH cur_insert BULK COLLECT INTO lr_datos LIMIT 5000;
                        FORALL i IN 1..lr_datos.COUNT SAVE EXCEPTIONS
                        INSERT /*+ PARALLEL('||p_table_name_his||',4) NOLOGGING APPEND*/ INTO '||p_table_name_his||' VALUES lr_datos (i);                             
                        EXIT WHEN cur_insert%NOTFOUND;
                        COMMIT;
                    END LOOP;
                    COMMIT;
                    CLOSE cur_insert;            
                    END;';
    
                EXECUTE IMMEDIATE move_data_query;

            IF p_tipo_fecha='VARCHAR2' THEN
            SPI_PART_LOG(v_schema,p_sqlcode,'OK','Se cargo la data desde:'||v_fecha_iniciopv||' -> '||v_fecha_finpv,v_procedure,p_table_name); COMMIT;
            ELSE
            SPI_PART_LOG(v_schema,p_sqlcode,'OK','Se cargo la data de:'||v_fecha_iniciop||' -> '||v_fecha_finp,v_procedure,p_table_name); COMMIT;
            END IF;

    END LOOP;

    SPU_INDEXESLOGGING(p_schema,p_table_name_his);

    EXCEPTION
      WHEN e_sindatacarga THEN
         p_sqlcode := CONST_CODE_NOPARTITION;
         p_sqlerrm := 'INFO..Aun no hay data para cargar en TH';
         SPI_PART_LOG(v_schema,p_sqlcode,'INFO','Aun no hay data para cargar en:'||p_table_name,v_procedure,p_table_name_his); COMMIT;    
      WHEN OTHERS THEN
        v_sqlcode := SQLCODE;
        v_sqlerrm := SQLERRM;
        p_sqlcode := v_sqlcode;
        p_sqlerrm := 'ERROR..'||SUBSTR (v_sqlerrm, 1, 200);
        V_RETRY_COUNT:=V_RETRY_COUNT+1;

        IF (v_sqlcode = -54 OR v_sqlcode = -60) AND V_RETRY_COUNT<=V_MAX_RETRIES THEN
          p_sqlcode := CONST_CODE_LOCK;
          p_sqlerrm := 'INFO..Se ha generado el bloqueo ('||V_RETRY_COUNT||'/'||V_MAX_RETRIES||')';
          SPI_PART_LOG(v_schema,p_sqlcode,'INFO','Se ha generado el bloqueo ('||V_RETRY_COUNT||'/'||V_MAX_RETRIES||')',v_procedure,p_table_name); COMMIT;    
        ELSE
        SPI_PART_LOG(v_schema,p_sqlcode,'ERROR',p_sqlerrm,v_procedure,p_table_name); COMMIT;   
        END IF;

        IF V_RETRY_COUNT > V_MAX_RETRIES THEN
          p_sqlcode := CONST_CODE_ERROR;
          p_sqlerrm := 'ERROR..Se alcanzo el maximo de reintentos ('||V_MAX_RETRIES||')';
          SPI_PART_LOG(v_schema,p_sqlcode,'ERROR','Se alcanzo el maximo de reintentos ('||V_MAX_RETRIES||')',v_procedure,p_table_name); COMMIT;    
        END IF;
        ROLLBACK;
    END SPI_INSERT_PART_SIN_FK;

/* ***************************************************************************************  */
/* Nombre                : SPU_INDEXESLOGGING                                            */
/* Descripcion           : Permite obtener el nombre de la llave primaria                   */
/* **************************************************************************************** */
PROCEDURE SPU_INDEXESLOGGING (
    p_schema IN VARCHAR2,
    p_table_name IN VARCHAR2
) IS
    v_index_name VARCHAR2(255);
    CURSOR c_indexes IS
        SELECT INDEX_NAME
        FROM ALL_INDEXES
        WHERE TABLE_OWNER = UPPER(p_schema)
          AND TABLE_NAME = UPPER(p_table_name);
BEGIN

    EXECUTE IMMEDIATE 'ALTER TABLE '|| p_schema || '.'||p_table_name||' LOGGING';

    FOR r_index IN c_indexes LOOP
        v_index_name := r_index.INDEX_NAME;
        EXECUTE IMMEDIATE 'ALTER INDEX '|| p_schema || '.' || v_index_name || ' LOGGING';
    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END SPU_INDEXESLOGGING;

/* ***************************************************************************************  */
/* Nombre                : SPU_INDEXESNOLOGGING                                            */
/* Descripcion           : Permite obtener el nombre de la llave primaria                   */
/* **************************************************************************************** */
PROCEDURE SPU_INDEXESNOLOGGING (
    p_schema IN VARCHAR2,
    p_table_name IN VARCHAR2
) IS
    v_index_name VARCHAR2(255);
    CURSOR c_indexes IS
        SELECT INDEX_NAME
        FROM ALL_INDEXES
        WHERE TABLE_OWNER = UPPER(p_schema)
          AND TABLE_NAME = UPPER(p_table_name);
BEGIN

    EXECUTE IMMEDIATE 'ALTER TABLE '|| p_schema || '.'||p_table_name||' NOLOGGING';

    FOR r_index IN c_indexes LOOP
        v_index_name := r_index.INDEX_NAME;
        EXECUTE IMMEDIATE 'ALTER INDEX '|| p_schema || '.' || v_index_name || ' NOLOGGING';
    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END SPU_INDEXESNOLOGGING;

/* ***************************************************************************************  */
/* Nombre                : FUN_obtener_primer_dia_particion                                            */
/* Descripcion           : Permite obtener el nombre de la llave primaria                   */
/* **************************************************************************************** */

FUNCTION FUN_obtener_primer_dia_particion(
    nombre_particion IN VARCHAR2
) RETURN TIMESTAMP IS
    anio_mes VARCHAR2(6);
    primer_dia TIMESTAMP;
BEGIN
    anio_mes := SUBSTR(nombre_particion, INSTR(nombre_particion, '_', -1) + 1);
    primer_dia := TO_TIMESTAMP(anio_mes || '01', 'YYYYMMDD');

    RETURN primer_dia;
EXCEPTION
    WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20001, 'Error en el procesamiento de la particion: ' || SQLERRM);
END FUN_obtener_primer_dia_particion;


/* ***************************************************************************************  */
/* Nombre                : FUN_obtener_ultimo_dia_particion                                            */
/* Descripcion           : Permite obtener el nombre de la llave primaria                   */
/* **************************************************************************************** */

FUNCTION FUN_obtener_ultimo_dia_particion(
    nombre_particion IN VARCHAR2
) RETURN TIMESTAMP IS
    anio_mes VARCHAR2(6);
    ultimo_dia TIMESTAMP;
BEGIN
    anio_mes := SUBSTR(nombre_particion, INSTR(nombre_particion, '_', -1) + 1);
    ultimo_dia := TO_TIMESTAMP(LAST_DAY(TO_DATE(anio_mes||'01', 'YYYYMMDD')) || ' 235959','YYYYMMDD HH24MISS');

    RETURN ultimo_dia;

EXCEPTION
    WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20001, 'Error en el procesamiento de la particion: ' || SQLERRM);
END FUN_obtener_ultimo_dia_particion;


/* ***************************************************************************************  */
/* Nombre                : SPS_obtener_particiones                                            */
/* Descripcion           : Obtiene las particiones en un rango de fechas                   */
/* **************************************************************************************** */

PROCEDURE SPS_obtener_particiones(
    fecha_inicio IN TIMESTAMP,
    fecha_fin IN TIMESTAMP,
    nombre_tabla IN VARCHAR2,
    particiones IN OUT nombre_particiones_array  
) IS
    inicio_mes NUMBER := EXTRACT(YEAR FROM fecha_inicio) * 100 + EXTRACT(MONTH FROM fecha_inicio);
    fin_mes NUMBER := EXTRACT(YEAR FROM fecha_fin) * 100 + EXTRACT(MONTH FROM fecha_fin);
    v_rec data_rec; 
    v_rec_var VARCHAR2(100);
BEGIN

    FOR mes IN inicio_mes .. fin_mes LOOP
        v_rec_var := 'P_' || nombre_tabla || '_' || TO_CHAR(mes, 'FM999999');
        v_rec.pl := 'P_' || nombre_tabla || '_' || TO_CHAR(mes, 'FM999999');

        IF FUN_validar_particion_existente(nombre_tabla,v_rec_var) THEN
        particiones.EXTEND;  
        particiones(particiones.LAST) := v_rec;
        END IF;

    END LOOP;

END SPS_obtener_particiones;

/* ***************************************************************************************  */
/* Nombre                : FUN_obt_min_fecha_partxn_times                                  */
/* Descripcion           : Permite obtener la minima fecha con data en una particion       */
/* **************************************************************************************** */

FUNCTION FUN_obt_min_fecha_partxn_times(
    p_table_name    IN VARCHAR2,  
    p_partition_name IN VARCHAR2, 
    p_fecha_field   IN VARCHAR2,
    p_tipo_fecha    IN VARCHAR2
) RETURN TIMESTAMP
IS
    v_min_date      TIMESTAMP;
    v_partition_date      TIMESTAMP;

BEGIN
    v_partition_date := TO_TIMESTAMP(SUBSTR(p_partition_name, -6)||'01', 'YYYYMMDD');

    IF p_tipo_fecha='VARCHAR2' THEN
      EXECUTE IMMEDIATE 'SELECT TO_TIMESTAMP(MIN(' || p_fecha_field || '),''YYYYMMDD'') FROM ' || p_table_name || ' PARTITION (' || p_partition_name || ')'
      INTO v_min_date;
      v_min_date := TO_TIMESTAMP(TRUNC(v_min_date) || ' 235958','YYYYMMDD HH24MISS');

    ELSE
      EXECUTE IMMEDIATE 'SELECT MIN(' || p_fecha_field || ') FROM ' || p_table_name || ' PARTITION (' || p_partition_name || ')'
      INTO v_min_date;
      v_min_date:= TO_TIMESTAMP(TRUNC(v_min_date) || ' 000000','YYYYMMDD HH24MISS');
    END IF;

    RETURN v_min_date;
EXCEPTION
    WHEN OTHERS THEN
        RETURN v_partition_date;
END FUN_obt_min_fecha_partxn_times;

/* ***************************************************************************************  */
/* Nombre                : FUN_obt_max_fecha_parth_times                                    */
/* Descripcion           : Permite obtener la maxima fecha con data en una particion historica */
/* **************************************************************************************** */

FUNCTION FUN_obt_max_fecha_parth_times(
    p_table_name    IN VARCHAR2,  
    p_partition_name IN VARCHAR2, 
    p_fecha_field   IN VARCHAR2,
    p_tipo_fecha    IN VARCHAR2
) RETURN TIMESTAMP
IS
    v_max_date      TIMESTAMP;
    v_partition_date TIMESTAMP;
BEGIN
    v_partition_date := TO_TIMESTAMP(SUBSTR(p_partition_name, -6)||'01 000000', 'YYYYMMDD HH24MISS');
    
    IF p_tipo_fecha='VARCHAR2' THEN
        EXECUTE IMMEDIATE 'SELECT TO_TIMESTAMP(MAX(' || p_fecha_field || '),''YYYYMMDD'') + INTERVAL ''1'' DAY FROM ' || p_table_name || ' PARTITION (' || p_partition_name || ')'
        INTO v_max_date;
    ELSE
      EXECUTE IMMEDIATE 'SELECT MAX(' || p_fecha_field || ') + INTERVAL ''1'' DAY FROM ' || p_table_name || ' PARTITION (' || p_partition_name || ')'
      INTO v_max_date;
    END IF;

    IF v_max_date IS NULL THEN
        v_max_date := v_partition_date;
    ELSE
        v_max_date := TO_TIMESTAMP(TRUNC(v_max_date) || ' 000000','YYYYMMDD HH24MISS');
    END IF;

    RETURN v_max_date;

EXCEPTION
    WHEN OTHERS THEN
        RETURN v_partition_date;
END FUN_obt_max_fecha_parth_times;

/* ***************************************************************************************  */
/* Nombre                : FUN_obt_max_fecha_partxn_times                                   */
/* Descripcion           : Permite obtener la maxima fecha con data en una particion txn    */
/* **************************************************************************************** */

FUNCTION FUN_obt_max_fecha_partxn_times(
    p_table_name    IN VARCHAR2,  
    p_partition_name IN VARCHAR2, 
    p_fecha_field   IN VARCHAR2,
    p_tipo_fecha    IN VARCHAR2
) RETURN TIMESTAMP
IS
    v_max_date      TIMESTAMP;
    v_partition_date      TIMESTAMP;

BEGIN
      v_partition_date := TO_TIMESTAMP(SUBSTR(p_partition_name, -6)||'01', 'YYYYMMDD');
    IF p_tipo_fecha='VARCHAR2' THEN
      EXECUTE IMMEDIATE 'SELECT TO_TIMESTAMP(MAX(' || p_fecha_field || '),''YYYYMMDD'') - INTERVAL ''1'' DAY FROM ' || p_table_name || ' PARTITION (' || p_partition_name || ')'
      INTO v_max_date;
    ELSE
      EXECUTE IMMEDIATE 'SELECT MAX(' || p_fecha_field || ') - INTERVAL ''1'' DAY FROM ' || p_table_name || ' PARTITION (' || p_partition_name || ')'
      INTO v_max_date;
    END IF;
    RETURN TO_TIMESTAMP(TRUNC(v_max_date) || ' 235959','YYYYMMDD HH24MISS');
EXCEPTION
    WHEN OTHERS THEN
        RETURN v_partition_date;
END FUN_obt_max_fecha_partxn_times;

/* ***************************************************************************************  */
/* Nombre                : FUN_obt_primer_part                                              */
/* Descripcion           : Permite obtener la primera Particion con data                    */
/* **************************************************************************************** */

FUNCTION FUN_obt_primer_part(p_table_name IN VARCHAR2)
RETURN VARCHAR2
IS
    v_last_month_partition VARCHAR2(50);
    v_count_rows           NUMBER;
BEGIN
    FOR rec IN (SELECT partition_name
                FROM user_tab_partitions
                WHERE table_name = UPPER(p_table_name)
                )
    LOOP
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_table_name || ' PARTITION (' || rec.partition_name || ')' 
            INTO v_count_rows;
        IF v_count_rows > 0 THEN
            v_last_month_partition := rec.partition_name;
            EXIT;
        END IF;
    END LOOP;

    IF v_last_month_partition IS NOT NULL THEN
        RETURN v_last_month_partition;
    ELSE
        RETURN fun_obtener_primera_particion(p_table_name);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END FUN_obt_primer_part;
/* ***************************************************************************************  */
/* Nombre                : FUN_obt_actual_part                                                   */
/* Descripcion           : Permite obtener el nombre de la particion actual                   */
/* **************************************************************************************** */

FUNCTION FUN_obt_actual_part(p_table_name IN VARCHAR2)
RETURN VARCHAR2
IS
    v_last_month_partition VARCHAR2(50);
    v_count_rows           NUMBER;
BEGIN
    FOR rec IN (SELECT partition_name
                FROM user_tab_partitions
                WHERE table_name = UPPER(p_table_name)  -- Asegura que el nombre de la tabla sea en mayÃºsculas
                ORDER BY partition_position DESC)
    LOOP
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_table_name || ' PARTITION (' || rec.partition_name || ')' 
            INTO v_count_rows;
        IF v_count_rows > 0 THEN
            v_last_month_partition := rec.partition_name;
            EXIT;
        END IF;
    END LOOP;

    IF v_last_month_partition IS NOT NULL THEN
        RETURN v_last_month_partition;
    ELSE
        RETURN fun_obtener_primera_particion(p_table_name);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END FUN_obt_actual_part;


/* ***************************************************************************************  */
/* Nombre                : fun_obtener_ultima_particion                                    */
/* Descripcion           : Permite obtener la ultima particion   */
/* **************************************************************************************** */

FUNCTION fun_obtener_ultima_particion(p_name_table IN VARCHAR2)
RETURN VARCHAR2
IS
   v_namePartition VARCHAR2(255);
BEGIN
   SELECT MAX(partition_name) KEEP (DENSE_RANK LAST ORDER BY partition_position) 
   INTO v_namePartition
   FROM user_tab_partitions
   WHERE table_name = UPPER(p_name_table)  
   GROUP BY table_name;
   RETURN v_namePartition;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      RETURN NULL;  
   WHEN OTHERS THEN
      RAISE;  
END fun_obtener_ultima_particion;
/* ***************************************************************************************  */
/* Nombre                : fun_obtener_primera_particion                                    */
/* Descripcion           : Permite obtener la primera particion                              */
/* **************************************************************************************** */

FUNCTION fun_obtener_primera_particion(p_name_table IN VARCHAR2)
RETURN VARCHAR2
IS
   v_namePartition VARCHAR2(255);
BEGIN
    SELECT max(partition_name) keep (dense_rank first order by partition_position) name_partition
    INTO  v_namePartition 
    FROM user_tab_partitions
    WHERE table_name = UPPER(p_name_table) 
    GROUP BY table_name;
   RETURN v_namePartition;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      RETURN NULL;  
   WHEN OTHERS THEN
      RAISE;  
END fun_obtener_primera_particion;

/* ***************************************************************************************  */
/* Nombre                : FUN_validar_particion_existente                                                  */
/* Descripcion           : Verifica si una particion existe                                  */
/* **************************************************************************************** */

FUNCTION FUN_validar_particion_existente(
    p_table_name IN VARCHAR2,
    p_partition_name IN VARCHAR2
) RETURN BOOLEAN IS
    v_count NUMBER;  -- Variable para contar las particiones
BEGIN
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM USER_TAB_PARTITIONS WHERE TABLE_NAME = UPPER(:1) AND PARTITION_NAME = UPPER(:2)'
    INTO v_count
    USING p_table_name, p_partition_name;
    RETURN v_count > 0;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error al validar la partición: ' || SQLERRM);
        RETURN FALSE;  
END FUN_validar_particion_existente;

/* ***************************************************************************************  */
/* Nombre                : sleep_time                                                        */
/* Descripcion           : Brinda tiempo de ejecucion al package                             */
/* **************************************************************************************** */
    PROCEDURE sleep_time
    IS
      start_time TIMESTAMP:=SYSTIMESTAMP;
      curr_time  TIMESTAMP;
      cnt        NUMBER:=0;
    BEGIN
      LOOP
        cnt:=cnt + 1;
        curr_time:=SYSTIMESTAMP;
      EXIT WHEN curr_time > start_time + 0.5/(24*60);
      END LOOP;
   END sleep_time;



END PKG_MTTOPART;
/

spool off
exit

SET SERVEROUTPUT ON
spool 82PatchEFT20240930RollbackCreatePartitionPinerr.log

DECLARE
  p_schema VARCHAR2(30) := UPPER('&1');
  v_tabla VARCHAR2(30);
  v_tabla_legacy VARCHAR2(50);
  v_tabla_leg VARCHAR2(30);
  v_sql_ddl VARCHAR2(80);
  v_sql_idx VARCHAR2(80);
  v_sql_rename VARCHAR2(80);
  v_countgeneral NUMBER;
  v_out_mensaje VARCHAR2(400);
  v_query_count VARCHAR2(200);

  e_tablanoparticionada EXCEPTION;

BEGIN
  
  v_tabla:='PINERR';
  v_query_count := q'{SELECT COUNT(1) FROM all_tab_partitions WHERE table_owner= :1 AND table_name = :2}';
  EXECUTE IMMEDIATE v_query_count INTO v_countgeneral USING p_schema,v_tabla;
  IF v_countgeneral = 0 THEN
  RAISE e_tablanoparticionada;
  END IF;
  
    v_out_mensaje:= 'Resultado:';

    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = ''YYYY-MM-DD''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD''';
    v_sql_ddl:='ALTER TABLE';
    v_sql_idx:='ALTER INDEX';
    v_sql_rename:='RENAME TO';
    v_tabla:='PINERR';
    v_tabla_leg:='PINERR_LEG';
    v_tabla_legacy:='PINERR_LEGACY';

    --Particionada a LEG
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||' '||v_sql_rename||' '||v_tabla_leg; 
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla_leg||' RENAME CONSTRAINT PK_PAN TO PK_PAN_LEG'; 
    EXECUTE IMMEDIATE v_sql_idx||' '||p_schema||'.PK_PAN RENAME TO PK_PAN_LEG';
    --Legacy a Regular
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla_legacy||'  '||v_sql_rename||' '||v_tabla||''; 
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||' RENAME CONSTRAINT PK_PAN_LEGACY TO PK_PAN'; 
    EXECUTE IMMEDIATE v_sql_idx||' '||p_schema||'.PK_PAN_LEGACY RENAME TO PK_PAN'; 
    --LEG a Legacy
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla_leg||' '||v_sql_rename||' '||v_tabla_legacy; 
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla_legacy||' RENAME CONSTRAINT PK_PAN_LEG TO PK_PAN_LEGACY'; 
    EXECUTE IMMEDIATE v_sql_idx||' '||p_schema||'.PK_PAN_LEG RENAME TO PK_PAN_LEGACY';

    v_out_mensaje:= v_out_mensaje||''||CHR(10)||'OK: Se realizo Rollback para la Tabla '||v_tabla||' Particionada';

  v_tabla:='THPINERR';
  v_tabla_leg:='THPINERR_LEG';
  v_tabla_legacy:='THPINERR_LEGACY';

  v_query_count := q'{SELECT COUNT(1) FROM all_tab_partitions WHERE table_owner= :1 AND table_name = :2}';
  EXECUTE IMMEDIATE v_query_count INTO v_countgeneral USING p_schema,v_tabla;
  IF v_countgeneral = 0 THEN
  RAISE e_tablanoparticionada;
  END IF;

    --Particionada a LEGACY
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla||' RENAME TO '||v_tabla_legacy||''; 
    EXECUTE IMMEDIATE v_sql_ddl||' '||p_schema||'.'||v_tabla_legacy||' RENAME CONSTRAINT PK_THPAN TO PK_THPAN_LEGACY';
    EXECUTE IMMEDIATE v_sql_idx||' '||p_schema||'.PK_THPAN RENAME TO PK_THPAN_LEGACY';
    v_out_mensaje:= v_out_mensaje||''||CHR(10)||'OK: Se realizo Rollback para la Tabla TH'||v_tabla||' Particionada';
    
    DBMS_OUTPUT.PUT_LINE(v_out_mensaje);

EXCEPTION
  WHEN e_tablanoparticionada THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: La Tabla '||v_tabla||' no esta particionada');
  WHEN OTHERS THEN 
    DBMS_OUTPUT.PUT_LINE(v_out_mensaje||''||CHR(10)||'ERROR: Rollback Tabla '||v_tabla||' Particionada: '||SQLERRM);
    return;
END;
/

spool off
exit

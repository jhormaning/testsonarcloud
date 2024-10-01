
SET SERVEROUTPUT ON
SET DEFINE ON
spool 01CreacionDDLMTTOPART.log

DECLARE
p_schema VARCHAR2(40):= UPPER('&1.');
v_countgeneral NUMBER(1);
v_query_count VARCHAR2(120);
v_objeto VARCHAR2(40);
BEGIN

  v_objeto := 'TP_PART_CONF';
  v_query_count := q'{SELECT NVL(0,1) FROM all_tables WHERE owner = :1 AND table_name = :2}';
  EXECUTE IMMEDIATE v_countgeneral INTO v_exist USING p_schema,v_objeto;
  IF v_countgeneral=0 THEN
        EXECUTE IMMEDIATE 'DROP TABLE &1..TP_PART_CONF PURGE';
        DBMS_OUTPUT.PUT_LINE('INFO: Se dropeo TP_PART_CONF');
  END IF;

  v_objeto := 'TP_PART_LOG';
  v_query_count := q'{SELECT NVL(0,1) FROM all_tables WHERE owner = :1 AND table_name = :2}';
  EXECUTE IMMEDIATE v_countgeneral INTO v_exist USING p_schema,v_objeto;
  IF v_countgeneral=0 THEN
        EXECUTE IMMEDIATE 'DROP TABLE &1..TP_PART_CONF PURGE';
        DBMS_OUTPUT.PUT_LINE('INFO: Se dropeo TP_PART_CONF');
  END IF;

  v_objeto := 'SEQ_PART_LOG';
  v_query_count := q'{SELECT NVL(0,1) FROM all_sequences WHERE sequence_owner = :1 AND sequence_name = :2}';
  EXECUTE IMMEDIATE v_countgeneral INTO v_exist USING p_schema,v_objeto;
  IF v_countgeneral=0 THEN
        EXECUTE IMMEDIATE 'DROP TABLE &1..TP_PART_CONF PURGE';
        DBMS_OUTPUT.PUT_LINE('INFO: Se dropeo TP_PART_CONF');
  END IF;

EXCEPTION
   WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('ERROR: Dropeo de objeto '||v_objeto||' '|| SQLERRM);
END;
/


CREATE TABLE &1..TP_PART_CONF (
    COD_PART_CONF           VARCHAR2(10) NOT NULL,
    NOM_TABLA               VARCHAR2(50) NOT NULL,
    NOM_THTABLA             VARCHAR2(50),
    NOM_CAMPO_FECHA_PART    VARCHAR2(50) NOT NULL,
    TIP_CAMPO_FECHA_PART    VARCHAR2(30) NOT NULL,
    NUM_PART_ATRAS          NUMBER(2),
    NUM_PART_ADELANTE       NUMBER(2),
    NUM_THPART_ATRAS        NUMBER(2),
    NUM_THPART_ADELANTE     NUMBER(2),
    FLG_MOVEDATATH          VARCHAR2(2), 
    FLG_FK                  VARCHAR2(2) NOT NULL,
    NOM_FK                  VARCHAR2(30),
    COD_PART_CONFDEP        VARCHAR2(10),
    TIP_PERIODO             VARCHAR2(10),
    NOM_TBSDATOS            VARCHAR2(30) NOT NULL,
    NOM_TBSIDX              VARCHAR2(30) NOT NULL,
    FLG_PAM                 VARCHAR2(2),
    ESTADO_EJECUCION        NUMBER(1) ---0 se ejecuta
) TABLESPACE &2.;

CREATE TABLE &1..TP_PART_LOG
    (   COD_PART_LOG        NUMBER(27),
        TIP_PART_LOG        NUMBER(1),
        DESC_TIP_PART_LOG   VARCHAR2(20),
        DESC_PART_LOG       VARCHAR2(600),
        NOM_PROCEDURE       VARCHAR2(20),
        NOM_TABLA           VARCHAR2(20),
        FEC_CREA            TIMESTAMP(6),
        USU_CREA            VARCHAR2(50)
    ) TABLESPACE &2.;

CREATE SEQUENCE &1..SEQ_PART_LOG INCREMENT BY 1 START WITH 1 MAXVALUE 999999999999999999999999999 MINVALUE 1 CACHE 50;


spool off
exit

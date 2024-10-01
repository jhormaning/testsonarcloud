SET SERVEROUTPUT ON
spool 80PatchEFT20240930CreatePartitionTxnlog.log

DECLARE
  p_schema VARCHAR2(30) := UPPER('&1.');
  p_tbs_dato VARCHAR2(50) := UPPER('&2.');
  p_tbs_indice VARCHAR2(50) := UPPER('&3.');
  p_partitions_antes NUMBER := &4-1; 
  p_partitions_desp NUMBER := &5;
  v_fecha_base date;
  v_sql VARCHAR2(8000);
  v_sql_ddlt VARCHAR2(20);
  v_sql_ddli VARCHAR2(20);
  v_sql_ddlc VARCHAR2(20);
  v_sql_ddlci VARCHAR2(20);
  v_countgeneral NUMBER;
  v_tabla VARCHAR2(30);
  v_tabla_legacy VARCHAR2(50);
  v_query_count VARCHAR2(200);
  v_out_mensaje VARCHAR2(400);
  TYPE varchar2_array IS VARRAY(2) OF VARCHAR2(30);
  my_array varchar2_array;

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

  v_tabla := 'TXNLOG';
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

  v_tabla := 'THTXNLOG';
  v_query_count := q'{SELECT COUNT(1) FROM all_tab_partitions WHERE table_owner= :1 AND table_name = :2}';
  EXECUTE IMMEDIATE v_query_count INTO v_countgeneral USING p_schema,v_tabla;
  IF v_countgeneral <> 0 THEN
  RAISE e_tablaparticionada;
  END IF;

    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = ''YYYYMMDD''';
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYYMMDD''';
    v_out_mensaje:= 'Resultado:';
    v_fecha_base:= sysdate;
    v_sql_ddlt:= 'ALTER TABLE';
    v_sql_ddli:= 'ALTER INDEX';
    v_sql_ddlc:= 'COMMENT ON COLUMN';
    v_sql_ddlci:= 'CREATE INDEX';

  my_array := varchar2_array('TXNLOG', 'THTXNLOG');

  FOR i IN 1 .. my_array.COUNT LOOP

          v_tabla := my_array(i);
          v_tabla_legacy:= my_array(i)||'_LEGACY';

          IF v_tabla='TXNLOG' THEN
          EXECUTE IMMEDIATE v_sql_ddlt||' '||p_schema||'.'||v_tabla||' RENAME TO '||v_tabla_legacy; 
          EXECUTE IMMEDIATE v_sql_ddlt||' '||p_schema||'.'||v_tabla_legacy||' RENAME CONSTRAINT PK_TXNLOG TO PK_TXNLOG_LEGACY'; 
          
          EXECUTE IMMEDIATE v_sql_ddli||' '||p_schema||'.PK_TXNLOG RENAME TO PK_TXNLOG_LEGACY'; 
          EXECUTE IMMEDIATE v_sql_ddli||' '||p_schema||'.IDX_LOG_1 RENAME TO IDX_LOG_1_LEGACY'; 
          END IF;

          v_sql := 'CREATE TABLE ' || p_schema || '.'||v_tabla||' (' ||
                    'TIMEIN NUMBER(10,0), '||
                    'TIMEOUT NUMBER(10,0), '||
                    'STATUS NUMBER(4,0), '||
                    'REVERSAL NUMBER(1,0), '||
                    'WHO_RSP NUMBER(1,0), '||
                    'REG_CONTROL VARCHAR2(12 BYTE), '||
                    'FTE_APPL VARCHAR2(8 BYTE), '||
                    'HEADER VARCHAR2(30 BYTE), '||
                    'ORG_DRV NUMBER(2,0), '||
                    'MSGTYPE NUMBER(4,0) NOT NULL ENABLE, '||
                    'BM_PRIMARIO VARCHAR2(16 BYTE), '||
                    'BM_SECUNDARIO VARCHAR2(16 BYTE), '||
                    'PAN VARCHAR2(19 BYTE) NOT NULL ENABLE, '||
                    'PRCODE NUMBER(6,0) NOT NULL ENABLE, '||
                    'AMOUNT_TXN VARCHAR2(12 BYTE) NOT NULL ENABLE, '||
                    'AMOUNT_STTL VARCHAR2(12 BYTE), '||
                    'AMOUNT_CARDHOLD VARCHAR2(12 BYTE), '||
                    'TRANSDATE VARCHAR2(8 BYTE), '||
                    'TRANSTIME VARCHAR2(6 BYTE), '||
                    'CONVRATE_STTL VARCHAR2(8 BYTE), '||
                    'CONVRATE_CARDHOLD VARCHAR2(8 BYTE), '||
                    'TRACE NUMBER(6,0) NOT NULL ENABLE, '||
                    'TIME_LOCAL VARCHAR2(6 BYTE) NOT NULL ENABLE, '||
                    'DATE_LOCAL VARCHAR2(8 BYTE) NOT NULL ENABLE, '||
                    'DATE_EXP VARCHAR2(4 BYTE), '||
                    'DATE_STTL VARCHAR2(8 BYTE), '||
                    'DATE_CONV VARCHAR2(8 BYTE), '||
                    'DATE_CAPTURE VARCHAR2(8 BYTE), '||
                    'MERCHANT NUMBER(4,0), '||
                    'ACQ_COUNTRY_CODE NUMBER(3,0), '||
                    'PAN_EXT_COUNTRY_CODE NUMBER(3,0), '||
                    'FORW_COUNTRY_CODE NUMBER(3,0), '||
                    'POS_ENTRY_MODE NUMBER(3,0), '||
                    'CARD_SEQ_NUM NUMBER(3,0), '||
                    'NET_INTERNAT VARCHAR2(3 BYTE), '||
                    'POINT_COND_CODE NUMBER(4,0) NOT NULL ENABLE, '||
                    'POINT_CAP_CODE NUMBER(4,0), '||
                    'AUTH_ID_RSP_LEN NUMBER(4,0), '||
                    'AMT_TXN_FEE VARCHAR2(9 BYTE), '||
                    'AMT_STTL_FEE VARCHAR2(9 BYTE), '||
                    'AMT_TXN_PROC_FEE VARCHAR2(9 BYTE), '||
                    'AMT_STTL_PROC_FEE VARCHAR2(9 BYTE), '||
                    'ACQ_INST VARCHAR2(11 BYTE) NOT NULL ENABLE, '||
                    'FORW_INST VARCHAR2(11 BYTE), '||
                    'PAN_EXT VARCHAR2(28 BYTE), '||
                    'TRACK2 VARCHAR2(37 BYTE), '||
                    'REFNUM VARCHAR2(12 BYTE), '||
                    'AUTH VARCHAR2(6 BYTE), '||
                    'RESP_CODE VARCHAR2(3 BYTE), '||
                    'SRV_RSTR_CODE NUMBER(3,0), '||
                    'TERM_ID VARCHAR2(16 BYTE) NOT NULL ENABLE, '||
                    'ACCEPTOR VARCHAR2(15 BYTE), '||
                    'NAME_LOCAL VARCHAR2(40 BYTE), '||
                    'ADD_RESP_DATA VARCHAR2(25 BYTE), '||
                    'ADD_RETAIL_DATA VARCHAR2(700 BYTE), '||
                    'CUR_CODE_TXN VARCHAR2(3 BYTE), '||
                    'CUR_CODE_STTL VARCHAR2(3 BYTE), '||
                    'CUR_CODE_CARDHOLD VARCHAR2(3 BYTE), '||
                    'ADDS_AMOUNTS VARCHAR2(80 BYTE), '||
                    'CARDH_DOC_NUM VARCHAR2(11 BYTE), '||
                    'FIED_59 VARCHAR2(25 BYTE), '||
                    'POS_CAPAB_CODE VARCHAR2(61 BYTE), '||
                    'CARD_ISS_CAT_RSP VARCHAR2(26 BYTE), '||
                    'INF_DATE VARCHAR2(100 BYTE), '||
                    'PRIV_USE VARCHAR2(90 BYTE), '||
                    'STTL_CODE NUMBER(1,0), '||
                    'EXTD_PAY_CODE NUMBER(2,0), '||
                    'REQ_INST_COUNTRY_CODE NUMBER(3,0), '||
                    'STTL_INST_COUNTRY_CODE NUMBER(3,0), '||
                    'NET_MGT_INF_CODE NUMBER(5,0), '||
                    'DATE_ACTION VARCHAR2(8 BYTE), '||
                    'ORG_DATA VARCHAR2(42 BYTE), '||
                    'FILE_UPDATE_CODE VARCHAR2(1 BYTE), '||
                    'FILE_SECURITY_CODE VARCHAR2(2 BYTE), '||
                    'REP_TXN_AMT VARCHAR2(12 BYTE), '||
                    'REP_STTL_AMT VARCHAR2(12 BYTE), '||
                    'REP_TXN_FEE VARCHAR2(12 BYTE), '||
                    'REP_STTL_FEE VARCHAR2(9 BYTE), '||
                    'MSG_SEC_COD VARCHAR2(16 BYTE), '||
                    'PAYEE VARCHAR2(25 BYTE), '||
                    'REQ_INST VARCHAR2(11 BYTE), '||
                    'FILE_NAME VARCHAR2(17 BYTE), '||
                    'ACCT_1 VARCHAR2(28 BYTE), '||
                    'ACCT_2 VARCHAR2(28 BYTE), '||
                    'ATM_TERM_ADDR_BR VARCHAR2(33 BYTE), '||
                    'AUTH_IND_CRT_DATA VARCHAR2(35 BYTE), '||
                    'BIN_CARD_ISS_ID_CODE VARCHAR2(13 BYTE), '||
                    'BATCH_SHIFT_DATA VARCHAR2(999 BYTE), '||
                    'SETTL_DATA VARCHAR2(999 BYTE), '||
                    'BIN_ACCT_1 VARCHAR2(11 BYTE), '||
                    'BIN_ACCT_2 VARCHAR2(11 BYTE), '||
                    'TRACK3 VARCHAR2(104 BYTE), '||
                    'TRACK1 VARCHAR2(79 BYTE), '||
                    'INTEG_CIRC_CARD VARCHAR2(1024 BYTE), '||
                    'INVOICE_DATA VARCHAR2(29 BYTE), '||
                    'PRE_AUTH_CHARGEBAK VARCHAR2(999 BYTE), '||
                    'PRIVATE_FIELD VARCHAR2(999 BYTE), '||
                    'INVOICE_DATA_0 VARCHAR2(20 BYTE), '||
                    'PRE_AUTH_CHARGEBAK_0 VARCHAR2(41 BYTE), '||
                    'USERIN VARCHAR2(24 BYTE) DEFAULT substr(user,1,24), '||
                    'DATEIN DATE, '||
                    'USERCHG VARCHAR2(24 BYTE), '||
                    'DATECHG DATE, '||
                    'TIME_STAMP VARCHAR2(17 BYTE), '||
                    'DEST_APPL VARCHAR2(9 BYTE), '||
                    'BMAP_EXTD VARCHAR2(16 BYTE), '||
                    'CRED_NUM VARCHAR2(10 BYTE), '||
                    'CRED_REV_NUM VARCHAR2(10 BYTE), '||
                    'DEB_NUM VARCHAR2(10 BYTE), '||
                    'DEB_REV_NUM VARCHAR2(10 BYTE), '||
                    'TRF_NUM VARCHAR2(10 BYTE), '||
                    'TRF_REV_NUM VARCHAR2(10 BYTE), '||
                    'INQ_NUM VARCHAR2(10 BYTE), '||
                    'AUTH_NUM VARCHAR2(10 BYTE), '||
                    'CRED_PROC_FEE_AMT VARCHAR2(12 BYTE), '||
                    'CRED_TXN_FEE_AMT VARCHAR2(12 BYTE), '||
                    'DEB_PROC_FEE_AMT VARCHAR2(12 BYTE), '||
                    'DEB_TXN_FEE_AMT VARCHAR2(12 BYTE), '||
                    'CRED_AMT VARCHAR2(16 BYTE), '||
                    'CRED_REV_AMT VARCHAR2(16 BYTE), '||
                    'DEB_AMT VARCHAR2(16 BYTE), '||
                    'DEB_REV_AMT VARCHAR2(16 BYTE), '||
                    'AMT_NET_STTL VARCHAR2(17 BYTE), '||
                    'STTL_INST_ID VARCHAR2(11 BYTE), '||
                    'RECORD_DATA VARCHAR2(999 BYTE), '||
                    'NET_MGT_INF_EXT NUMBER(4,0), '||
                    'MSGID VARCHAR2(40 BYTE), '||
                    'PAN_CIFRADO VARCHAR2(64 BYTE), '||
                    'BIN_KEY VARCHAR2(11 BYTE), '||
                    'SECUENCIAL_KEY NUMBER(12,0)'||
                   ') TABLESPACE ' || p_tbs_dato || ' PARTITION BY RANGE (DATEIN) (';

          FOR i IN -p_partitions_antes .. p_partitions_desp LOOP
               
          v_sql := v_sql || ' PARTITION P_'||v_tabla||'_'||TO_CHAR(ADD_MONTHS(v_fecha_base,i),'YYYYMM')||' VALUES LESS THAN ('''||TO_CHAR(TRUNC(ADD_MONTHS(v_fecha_base,i+1), 'MM'),'YYYYMMDD')||''')';

          IF i < p_partitions_desp THEN
              v_sql := v_sql || ', ';
          END IF;

          END LOOP;      
          v_sql := v_sql || ')';

          EXECUTE IMMEDIATE v_sql;

      IF v_tabla='TXNLOG' THEN
       -- CREATE/RECREATE primary, unique and foreign key constraints
          EXECUTE IMMEDIATE v_sql_ddlt||' '||p_schema||'.'||v_tabla||' ADD CONSTRAINT "PK_THTXNLOG" PRIMARY KEY ("MSGTYPE", "PAN", "PRCODE", "AMOUNT_TXN", "TRACE", "TIME_LOCAL", "DATE_LOCAL", "POINT_COND_CODE", "ACQ_INST", "TERM_ID") USING INDEX TABLESPACE '||p_tbs_indice;
       -- CREATE/RECREATE INDEXES
          EXECUTE IMMEDIATE v_sql_ddlci||' '||p_schema||'.IDX_THLOG_1 ON '||p_schema||'.'||v_tabla||' ("MSGTYPE", "PAN", "AMOUNT_TXN", "TRACE", "TRANSDATE", "TRANSTIME") LOCAL TABLESPACE '||p_tbs_indice;
      END IF;


      IF v_tabla='TXNLOG' THEN

       -- CREATE/RECREATE primary, unique and foreign key constraints
          EXECUTE IMMEDIATE v_sql_ddlt||' '||p_schema||'.'||v_tabla||' ADD CONSTRAINT "PK_TXNLOG" PRIMARY KEY ("MSGTYPE", "PAN", "PRCODE", "AMOUNT_TXN", "TRACE", "TIME_LOCAL", "DATE_LOCAL", "POINT_COND_CODE", "ACQ_INST", "TERM_ID") USING INDEX TABLESPACE '||p_tbs_indice;
       -- CREATE/RECREATE INDEXES
          EXECUTE IMMEDIATE v_sql_ddlci||' '||p_schema||'.IDX_LOG_1 ON '||p_schema||'.'||v_tabla||' ("MSGTYPE", "PAN", "AMOUNT_TXN", "TRACE", "TRANSDATE", "TRANSTIME") LOCAL TABLESPACE '||p_tbs_indice;

      --COMMENTS
      EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TIMEIN" IS ''Hora de registro de la transacciÃ¯Â¿Â½n de requerimiento (hora del sistema).''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TIMEOUT" IS ''Hora de registro de la transacciÃ¯Â¿Â½n de respuesta (hora del sistema).''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."STATUS" IS ''Indica el estado de la transacciÃ¯Â¿Â½n, este debe tomar un valor igual al de un tipo de mensaje de respuesta de la norma ISO-8583, en caso de que su valor sea cero significa que la transacciÃ¯Â¿Â½n a sido registrada pero por algÃ¯Â¿Â½n problema no se actualizÃ¯Â¿Â½.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."REVERSAL" IS ''Indica si la transaccion ya ha sido reversada. Valiores comunes: 0 -> transaccion no ha sido reversada, 1 -> transacciÃ¯Â¿Â½n ya fue reversada.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."WHO_RSP" IS ''Indica quien respondio la transacciÃ¯Â¿Â½n. Valores comunes:  0  -> Switch, 1  -> Host,  2  -> SAF. ''';     
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."REG_CONTROL" IS ''Campo interno del SWITCH que se utiliza para dar soporte a su funcionalidad. Tiene la siguiente estructura:'||
        'reg_control[0] = indica el fuente '||
        'reg_control[1] = indica el destino. Puede tener los siguientes valores: 1 -> Switch, 2 -> Autorizador, 3 -> Host.'||
        'reg_control[2] = indica la función. Puede tener los siguientes valores: 1 -> Pre-autorización, 2 -> Autorización, 3 -> Notificación.'||
        'reg_control[3] = indica el tipo de autorización. Puede tomar los siguientes valores soportados: 1 -> on host, 2 -> off host, 3 -> on/off host.'||
        'reg_control[4] = indica el método de autorización. Puede tomar los siguientes valores: 1 -> host en línea, 2 -> negativa con acumuladores, 3 -> positiva con acumuladores, 4 -> positiva con saldos, 5 -> negativa sin acumuladores.'||
        'reg_control[5] = Indica quién respondió la transacción. Puede tomar los siguientes valores: 0 -> Switch, 1 -> Host, 2 -> Store and Forward.'||
        'reg_control[6] = Indica si la respuesta es del host o del formateador. Puede tomar los siguientes valores: 0 -> Switch (La respuesta es del formateador - Reject), 1 -> Host (Respuesta es del Autorizador).''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."FTE_APPL" IS ''Nombre simbÃ¯Â¿Â½lico del que origina la transacciÃ¯Â¿Â½n.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."HEADER" IS ''Contiene la informaciÃ¯Â¿Â½n del header de la trama recibida''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."ORG_DRV" IS ''El cÃ¯Â¿Â½digo del origen del origen de la transacciÃ¯Â¿Â½n. Puede tomar los siguientes valores:'||
          'ATM -> 2 /* ATM */'||
          'BOX -> 4 /* BOX - electronic cash register */'||
          'IVR  -> 7 /* IVR - interactive voice response */'||
          'POS -> 14 /* POS */'||
          'WEB -> 15 /* WEB */'||
          'ADM -> 16 /* Terminal Administrativo */'||
          'NET -> 52 /* NET */'||
          'SWI -> 53 /* Switch */'||
          'KIO -> 54       /*      KIOSCO */'||
          'HPS -> 55 /* Heps */'||
          'BCO -> 56 /* BANCO */'||
          'UNI -> 57 /* Formateador Unico */'||
          'VEN ->90 '||
          'SAF -> 99   /*  Programa SNDSAF*/'||
          'SDM -> 10   /*      Canal SODIMAC CMR*/''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."MSGTYPE" IS ''El tipo de mensaje de la transacciÃ¯Â¿Â½n.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."BM_PRIMARIO" IS ''Bitmap primario recibido en la trama de requerimiento si esta fue ISO-8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."BM_SECUNDARIO" IS ''Bitmap secundario recibido en la trama de requerimiento si esta fue ISO-8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."PAN" IS ''Es el nÃ¯Â¿Â½mero de tarjeta utilizada en la transacciÃ¯Â¿Â½n.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."PRCODE" IS ''Codigo de proceso de la transacciÃ¯Â¿Â½n que se corresponde con el campo 3 de la norma ISO 8583. Su formato es "AAFFTT", donde:'||
      'AA -> Tipo de transacciÃ¯Â¿Â½n.'||
      'FF ->  Cuenta de origen.'||
      'TT -> Cuenta de destino.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AMOUNT_TXN" IS ''Monto de la transacciÃ¯Â¿Â½n, en una moneda determinada, que es procesada por el SWITCH. Se corresponde con el campo 4 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AMOUNT_STTL" IS ''Monto a transferirse del adquiriente al autorizador, en una moneda determinada, para equiparar los montos de las transacciones realizadas entre estas instituciones.Se corresponde con el campo 5 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AMOUNT_CARDHOLD" IS ''Monto de la transacciÃ¯Â¿Â½n que realiza el cardholder, en una moneda  determinada. Se corresponde con el campo 6 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TRANSDATE" IS ''Fecha en la que una determinada transacciÃ¯Â¿Â½n es recepcionada por el SWITCH. Formato: YYYYMMDD, donde: YYYY -> aÃ¯Â¿Â½o, MM -> mes, DD: dÃ¯Â¿Â½a.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TRANSTIME" IS ''Hora en la que una determinada transacciÃ¯Â¿Â½n es recepcionada por el SWITCH. Formato: HHMMSS, donde: HH -> hora, MM -> minuto, SS -> segundos.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CONVRATE_STTL" IS ''Tasa de conversiÃ¯Â¿Â½n para que el monto de la transacciÃ¯Â¿Â½n sea transformado a la moneda del seetlement. Se corresponde con el campo 9 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CONVRATE_CARDHOLD" IS ''Tasa de conversion para que el monto de la transaccion, procesada por el SWITCH, sea transformado a la moneda en la que el cardholder realiza la transacciÃ¯Â¿Â½n. Se corresponde con el campo 10 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TRACE" IS ''NÃ¯Â¿Â½mero asignado a la transacciÃ¯Â¿Â½n. Se corresponde con el campo 11 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TIME_LOCAL" IS ''Hora local del lugar en el que se encuentra el card acceptor y en la que se dio inicio a la transacciÃ¯Â¿Â½n. Se corresponde con el campo12 de la norma ISO 8583. Su formato es HHMMSS, donde: HH -> Horas, MM -> Minutos, SS -> Segundos.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DATE_LOCAL" IS ''Fecha local del punto donde se encuentra el card acepptor y en  la que la transaccion se dio inicio. Se corresponde con el campo 13 de la norma ISO 8583. Su formato es YYYYMMDD, donde: YYYY -> AÃ¯Â¿Â½os, MM -> Mes, DD -> DÃ¯Â¿Â½a.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DATE_EXP" IS ''Fecha de expiraciÃ¯Â¿Â½n de la tarjeta con la cual se realiza la transacciÃ¯Â¿Â½n Se corresponde con el campo 14 de la norma ISO 8583. . Su formato es YYMM, donde: YY -> AÃ¯Â¿Â½o, MM -> Mes.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DATE_STTL" IS ''Fecha en que se realiza la compensaciÃ¯Â¿Â½n.  Se corresponde con el campo 15 de la norma ISO 8583. Su formato es: MMDD, donde: MM -> Mes, DD -> DÃ¯Â¿Â½a.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DATE_CONV" IS ''Fecha de conversiÃ¯Â¿Â½n del monto de la transaccion al settlement. Se corresponde con el campo 16 de la norma ISO 8583. Su formato es: MMDD, donde: MM -> Mes, DD -> DÃ¯Â¿Â½a.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DATE_CAPTURE" IS ''La fecha en el que el adquiriente proceso la transacciÃ¯Â¿Â½n. Se corresponde con el campo 17 de la norma ISO 8583. Su formato es: MMDD, donde: MM -> Mes, DD -> DÃ¯Â¿Â½a.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."MERCHANT" IS ''ClasificaciÃ¯Â¿Â½n del rubro del establecimiento donde se inicia la transacciÃ¯Â¿Â½n. Se corresponde con el campo 18 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."ACQ_COUNTRY_CODE" IS ''El cÃ¯Â¿Â½digo del pais donde se encuentra la acquiring institution (adquiriente). Se corresponde con el campo 19 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."PAN_EXT_COUNTRY_CODE" IS ''El cÃ¯Â¿Â½digo del pais donde se encuentra la instituciÃ¯Â¿Â½n del autorizador. Se corresponde con el campo 20 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."FORW_COUNTRY_CODE" IS ''El cÃ¯Â¿Â½digo del pais donde se encuentra la forwarding institution. Se corresponde con el campo 21 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."POS_ENTRY_MODE" IS ''Campo compuesto por tres digitos, los primeros indicac la forma en la que el terminal obtuvo el PAN y el Ã¯Â¿Â½ltimo, si es que el terminal tiene la capacidad de leer PIN.  Se corresponde con el campo 22 de la norma ISO 8583. ''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CARD_SEQ_NUM" IS ''NÃ¯Â¿Â½mero que distingue entre dos tarjetas con el mismo PAN o PAN extendido. Se corresponde con el campo 23 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."NET_INTERNAT" IS ''Identifica a una Ã¯Â¿Â½nica red internacional de autorizadores. Se corresponde con el campo 24 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."POINT_COND_CODE" IS ''CÃ¯Â¿Â½digo de la condiciÃ¯Â¿Â½n bajo la cual la transacciÃ¯Â¿Â½n inicia en el punto de servicio. Se corresponde con el campo 25 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."POINT_CAP_CODE" IS ''CÃ¯Â¿Â½digo que indentifica el mÃ¯Â¿Â½todo o el mÃ¯Â¿Â½ximo nÃ¯Â¿Â½mero de caracteres del PIN aceptados por el punto de servicio (POS) los cuales son utilizados para construir la informaciÃ¯Â¿Â½n del PIN. Se corresponde con el campo 26 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AUTH_ID_RSP_LEN" IS ''Maxima longitud de la respuesta al requerimiento (transacciÃ¯Â¿Â½n) de  autorizacion que el adquiriente puede soportar. Solo es usado en una implementacion de Interbank.  Se corresponde con el campo 27 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AMT_TXN_FEE" IS ''ComisiÃ¯Â¿Â½n en la moneda que opera el SWITCH y que es cargada por realizar una transaccion. Se corresponde con el campo 28 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AMT_STTL_FEE" IS ''ComisiÃ¯Â¿Â½n a ser transferida entre el adquiriente y autorizador igual valor que el monto de las comisiones por transaccion realizadas por el SWITCH en la moneda del campo "AMOUNT_STL". Se corresponde con el campo 29 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AMT_TXN_PROC_FEE" IS ''ComisiÃ¯Â¿Â½n cobrada por una entidad por el manejo o ruteo de mensajes en la moneda en la que el SWITCH procesa la transacciÃ¯Â¿Â½n. Se corresponde con el campo 30 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AMT_STTL_PROC_FEE" IS ''ComisiÃ¯Â¿Â½n cobrada por una entidad por el manejo o ruteo de mensajes en la moneda de la cantidad que se envia entre el adquiriente y autorizador para realizar la equiparaciÃ¯Â¿Â½n de montos entre estos. Se corresponde con el campo 31 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."ACQ_INST" IS ''CÃ¯Â¿Â½digo de identificaciÃ¯Â¿Â½n de la acquiring institution (adquiriente) de una determinada transaccion. Se corresponde con el campo 32 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."FORW_INST" IS ''CÃ¯Â¿Â½digo de identificaciÃ¯Â¿Â½n de la forwarding  institution de una determinada transaccion. Se corresponde con el campo 33 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."PAN_EXT" IS ''Utilizado solo cuando el PAN empieza el 59. Es empleado para identificar una cuenta de un cliente o una interrelacion. Solo se utiliza con mensajes 500 de reconciliacion.  Se corresponde con el campo 34 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TRACK2" IS ''Es la informaciÃ¯Â¿Â½n codificada en el TRACK 2 de la banda magnetica de la tarjeta. Se corresponde con el campo 35 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."REFNUM" IS ''NÃ¯Â¿Â½mero de referencia de la transacciÃ¯Â¿Â½n. Se corresponde con el campo 37 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AUTH" IS ''Identificador de la respuesta asignado, por el autorizador, a la transacciÃ¯Â¿Â½n. Se corresponde con el campo 38 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."RESP_CODE" IS ''Codigo de respuesta de la transacciÃ¯Â¿Â½n. Se corresponde con el campo 39 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."SRV_RSTR_CODE" IS ''Codigo para identificar la disponibilidad geogrÃ¯Â¿Â½fica o del servicio. Se corresponde con el campo 40 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TERM_ID" IS ''CÃ¯Â¿Â½digo que identifica el terminal, en la localizaciÃ¯Â¿Â½n del card acceptor, de la transacciÃ¯Â¿Â½n. Se corresponde con el campo 41 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."ACCEPTOR" IS ''CÃ¯Â¿Â½digo que identifica al card acceptor de la transacciÃ¯Â¿Â½n. Se corresponde con el campo 42 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."NAME_LOCAL" IS ''Nombre y localizaciÃ¯Â¿Â½n del card acceptor. Se corresponde con el campo 43 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."ADD_RESP_DATA" IS ''Datos adicionales requeridos en la respuesta de una transacciÃ¯Â¿Â½n. Se corresponde con el campo 44 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."ADD_RETAIL_DATA" IS ''Campo de uso privado. Se corresponde con el campo 48 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CUR_CODE_TXN" IS ''CÃ¯Â¿Â½digo del tipo de moneda en que una transacciÃ¯Â¿Â½n es procesada por el SWITCH. Se corresponde con el campo 49 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CUR_CODE_STTL" IS ''Codigo del tipo de moneda en el que se realizarÃ¯Â¿Â½ la equiparaciÃ¯Â¿Â½n de montos entre el adquiriente y autorizador. Se corresponde con el campo 50 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CUR_CODE_CARDHOLD" IS ''Codigo de la moneda en la que el cardholder realiza una determinada transacciÃ¯Â¿Â½n. Se corresponde con el campo 51 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."ADDS_AMOUNTS" IS ''Contiene montos adicionales que son enviados en la respuesta del autorizador. Los montos adicionales son el saldo contable y disponible. Se corresponde con el campo 54 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CARDH_DOC_NUM" IS ''Campo de uso reservado. Se corresponde con el campo 58 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."FIED_59" IS ''Campo de uso reservado. Se corresponde con el campo 59 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."POS_CAPAB_CODE" IS ''Campo de uso privado. Se corresponde con el campo 60 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CARD_ISS_CAT_RSP" IS ''Campo de uso privado. Se corresponde con el campo 61 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."INF_DATE" IS ''Campo de uso privado. Se corresponde con el campo 62 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."PRIV_USE" IS ''Campo de uso privado. Se corresponde con el campo 63 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."STTL_CODE" IS ''Codigo que identifica al resultado de un requerimiento de concilianciÃ¯Â¿Â½n.  Se corresponde con el campo 66 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."EXTD_PAY_CODE" IS ''NÃ¯Â¿Â½nero de meses en el que el cardholder puede pagar una transacciÃ¯Â¿Â½n de crÃ¯Â¿Â½dito siempre que el autorizador lo permita. Se corresponde con el campo 67 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."REQ_INST_COUNTRY_CODE" IS ''CÃ¯Â¿Â½digo del paÃ¯Â¿Â½s donde se encuentra el autorizador. Se corresponde con el campo 68 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."STTL_INST_COUNTRY_CODE" IS ''CÃ¯Â¿Â½digo del paÃ¯Â¿Â½s donde se encuentra la settlement institution. Se corresponde con el campo 69 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."NET_MGT_INF_CODE" IS ''Utilizado para identificar el status de red. Se corresponde con el campo 70 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DATE_ACTION" IS ''Uso futuro. Campo 73 de la normas ISO 8583''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."ORG_DATA" IS ''Datos contenidos en el mensaje original (mensaje previo) cuyo propÃ¯Â¿Â½sito es identificar a una transaccion para un extorno. Se corresponde con el campo 70 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."FILE_UPDATE_CODE" IS ''IndicaciÃ¯Â¿Â½n al sistema que mantiene el archivo cual procedimiento seguir. Se corresponde con el campo 91 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."FILE_SECURITY_CODE" IS ''Un cÃ¯Â¿Â½digo de seguridad de actualizaciÃ¯Â¿Â½n de archivo para indicar que el que originÃ¯Â¿Â½ el mensaje 300 estÃ¯Â¿Â½ autorizado para actualizar el archivo.Se corresponde con el campo 92 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."REP_TXN_AMT" IS ''Nuevo monto actual correspondiente con el campo "AMOUNT_TXN" necesario para llevar a cabo un extorno. Se corresponde con el campo 95 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."REP_STTL_AMT" IS ''Nuevo monto actual correspondiente con el campo "AMOUNT_STTL" necesario para llevar a cabo un extorno. Se corresponde con el campo 95 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."REP_TXN_FEE" IS ''Nuevo monto actual correspondiente con el campo "AMT_TXN_FEE" necesario para llevar a cabo un extorno. Se corresponde con el campo 95 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."REP_STTL_FEE" IS ''Nuevo monto actual correspondiente con el campo "AMT_STTL_FEE" necesario para llevar a cabo un extorno. Se corresponde con el campo 95 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."MSG_SEC_COD" IS ''Una verificaciÃ¯Â¿Â½n entre el card acceptor y el autorizador de que un mensaje esta autorizado para actualizar un archivo especial. Se corresponde con el campo 96 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."PAYEE" IS ''La tercera parte beneficiada en una transacciÃ¯Â¿Â½n de pago. Se corresponde con el campo 98 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."REQ_INST" IS ''Bin de ruteo de la requesting institution (autorizador) de la transacciÃ¯Â¿Â½n. Se corresponde con el campo 100 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."FILE_NAME" IS ''El nombre real o abreviado del archivo que es accesado. Se corresponde con el campo 101 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."ACCT_1" IS ''NÃ¯Â¿Â½mero usado para identificar la cuenta FROM de una transacciÃ¯Â¿Â½n. Se corresponde con el campo 102 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."ACCT_2" IS ''NÃ¯Â¿Â½mero usado para identificar la cuenta TO de una transacciÃ¯Â¿Â½n. Se corresponde con el campo 103 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."ATM_TERM_ADDR_BR" IS ''DirecciÃ¯Â¿Â½n de ATM-Terminal.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AUTH_IND_CRT_DATA" IS ''Campo reservado para uso privado. Se corresponde con el campo 121 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."BIN_CARD_ISS_ID_CODE" IS ''Campo reservado para uso privado. Se corresponde con el campo 122 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."BATCH_SHIFT_DATA" IS ''Campo reservado para uso privado. Se corresponde con el campo 124 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."SETTL_DATA" IS ''Campo reservado para uso privado. Se corresponde con el campo 125 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."BIN_ACCT_1" IS ''Bin asociado a la cuenta FROM, el cual se utiliza para rutear el bin de ruteo del autorizador de la transacciÃ¯Â¿Â½n.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."BIN_ACCT_2" IS ''Bin asociado a la cuenta TO, el cual se utiliza para rutear el bin de ruteo del autorizador de la transacciÃ¯Â¿Â½n.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TRACK3" IS ''Es la informaciÃ¯Â¿Â½n codificada en el TRACK 3 de la banda magnetica de la tarjeta. Se corresponde con el campo 36 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TRACK1" IS ''Es la informaciÃ¯Â¿Â½n codificada en el TRACK 1 de la banda magnetica de la tarjeta. Se corresponde con el campo 44 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."INTEG_CIRC_CARD" IS ''Campo reservado para el uso de la ISO. Se corresponde con el campo 55  de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."INVOICE_DATA" IS ''Campo reservado para uso privado. Se corresponde con el campo 123 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."PRE_AUTH_CHARGEBAK" IS ''Campo reservado para uso privado. Se corresponde con el campo 126 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."PRIVATE_FIELD" IS ''Campo reservado para uso privado. Se corresponde con el campo 127 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."INVOICE_DATA_0" IS ''Campo de uso futuro.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."PRE_AUTH_CHARGEBAK_0" IS ''Campo de uso futuro.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."USERIN" IS ''Usuario que inserto el registro.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DATEIN" IS ''Fecha de inserciÃ¯Â¿Â½n del registro.  Formato: DD/MM/YY.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."USERCHG" IS ''Ultimo usuario que modificÃ¯Â¿Â½ el registro.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DATECHG" IS ''Fecha de la ultima modificaciÃ¯Â¿Â½n realizada.  Formato: DD/MM/YY.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TIME_STAMP" IS ''Tiempo que  pone el formateador que recibe la transacciÃ¯Â¿Â½n del adquiriente; el cual es utilizado para verificar el correcto procesamiento de la transacciÃ¯Â¿Â½n.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DEST_APPL" IS ''Nombre del proceso que enviarÃ¯Â¿Â½ la transaccion al autorizador.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."BMAP_EXTD" IS ''Bitmap 3 de la transaccion si es que este lo tuviera. Se corresponde con el campo 65 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CRED_NUM" IS ''Representa el total de transacciones de credito procesadas. Se corresponde con el campo 74 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CRED_REV_NUM" IS ''Representa el total de transacciones de debitos de extorno. Se corresponde con el campo 75 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DEB_NUM" IS ''Representa el total de transacciones de debito procesadas. Se corresponde con el campo 76 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DEB_REV_NUM" IS ''El total de transacciones de extorno de debito. Se corresponde con el campo 77 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TRF_NUM" IS ''Total de todas las transferencias procesadas. Se corresponde con el campo 78 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."TRF_REV_NUM" IS ''Representa el total de extornos de transferencias procesados. Se corresponde con el campo 79 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."INQ_NUM" IS ''Representa la suma de todos los requerimientos procesados cuyo codigo de procesamiento es 30. Se corresponde con el campo 80 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AUTH_NUM" IS ''Representa la suma de los requerimientos de autorizaciÃ¯Â¿Â½n y mensajes de notificaciÃ¯Â¿Â½n procesados. Se corresponde con el campo 81 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CRED_PROC_FEE_AMT" IS ''La suma de todos los montos de todos los pagos procesados asociados con el manejo y ruteo de  las transacciones de crÃ¯Â¿Â½dito. Se corresponde con el campo 82 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CRED_TXN_FEE_AMT" IS ''La suma de todos los montos de todos los pagos que resultan del procesamiento de todas las transacciones de crÃ¯Â¿Â½dito. Se corresponde con el campo 83 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DEB_PROC_FEE_AMT" IS ''La suma de los montos de todos los pagos procesados asociados con el control y ruteo de las transacciones de debito. Se corresponde con el campo 84 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DEB_TXN_FEE_AMT" IS ''Suma total de los montos de pagos hechos resultante de todas las transacciones de debito. Se corresponde con el campo 85 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CRED_AMT" IS ''Suma total de los montos de credito sin incluir ningun cobro adicional. Se corresponde con el campo 86 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."CRED_REV_AMT" IS ''Suma total de los montos de los reversos de credito  sin incluir ningun cobro adicional. Se corresponde con el campo 87 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DEB_AMT" IS ''Suma total de los montos de debito sin incluir ningun cobro adicional. Se corresponde con el campo 88 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."DEB_REV_AMT" IS ''Suma total de los montos de los reversos de debito sin incluir ningun cobro adicional. Se corresponde con el campo 89 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."AMT_NET_STTL" IS ''Valor de net de todos los montos totales. Se corresponde con el campo 97 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."STTL_INST_ID" IS ''Codigo para identificar la settlement institution. Se corresponde con el campo 99 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."RECORD_DATA" IS ''Campo reservado para uso privado. Se corresponde con el campo 120 de la norma ISO 8583.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."NET_MGT_INF_EXT" IS ''Campo de uso futuro.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."MSGID" IS ''CÃ¯Â¿Â½digo (empleado por el SIX/TCL) que sirve para rastrear la transacciÃ¯Â¿Â½n.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."PAN_CIFRADO" IS ''Pan cifrado con la clave DEK.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."BIN_KEY" IS ''BIN para identificar la clave(DEK) de cifrado de data.''';
         EXECUTE IMMEDIATE v_sql_ddlc||' '||p_schema||'.'||v_tabla||'."SECUENCIAL_KEY" IS ''Secuencial de llave(DEK) utilizada en el cifrado de Data.''';
         EXECUTE IMMEDIATE 'COMMENT ON TABLE '||p_schema||'.'||v_tabla||'  IS ''Esta tabla contiene el registro de todas las transacciones que han circulado por la red.''';
      END IF; 
      v_out_mensaje:= v_out_mensaje||''||CHR(10)||'OK: Tabla '||v_tabla||' Particionada creada';

END LOOP;

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

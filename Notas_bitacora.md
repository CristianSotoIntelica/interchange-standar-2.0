# Bitacora

## 2026-02-16

### Pendientes
1. Agregar logica de exclude flag
2. Agregar comentarios a todos los módulos 
3. Documentar proyecto
4. Realizar validación 1 a 1 en SBSA

### Entendimiento
#### Modulo Interchange
##### Paso 1:
1. Ejecutar calculate_pre_eval:
- Hace un group by a la tabla de MC_RULES por el campo "amount_transaction_currency" y filtrando por amount_transaction_currency IS NOT NULL y lo almacena como dataframe df_targerts (**Se podria agregar un filtro más de UNTIL IS NULL**)
- Generar en targets_ccys lista de monedas según el df_targets, lo mismo pero en lista.
- Hace todo un select a la tabla de currency completa y trae los campos currency_numeric_code y currency_alphabetic_code. Para poder hacer la "traduccion" del campo de monea. y lo almacena como dataframe df_curr
- Hace una consulta total a la tabla exchange_rate para el tipo de cambio por el filtro BRAND y trae los campos. Y lo almacena como dataframe df_ex (**por que se utiliza solo el currency_from_code numerico y no el alfanumerico y lo mismo con currency_to que solo es el alfabetico y no numerico**): 
    - rate_date (fecha del tipo de cambio), 
    * brand (marca)
    * currency_from_code (moneda origen numerica)
    * currency_to (moneda destino alfabetica)
    * exchange_value
- Registrar el df_curr con alias cur
- Registrar el df_ex con alias ex
- Inicializar lista llamada dyn_cols
- Inicializar lista llamada dyn_joins
- Recorrer el listado de target_ccys denominadno el puntero de iteracion como ccy:
    - Realizar append a dyn_cols con valor de la cadena de una parte de la consulta a la tabla realizando el calculo del monto de la transaccion (amount_transaction) con su conversión de tipo de cambio (exchange_value). Luego lo castea como VARCHAR. Y por ultimo realiza un COALACE por si es nulo, le pone el valor BLANK **No logro entender por qué ponerlo en BLANK, o por que realizar un COALACE y castearlo como VARCHAR**
    - Realizar append a dyn_joins con el join de la tabla exchange_rate (ex) con la tabla transaccional del parquet CLEAN usando como llaves **Se podria utizar el DE_12_1 que ya tiene la fecha convertida para no utilizar un convert/cast del DE_12 puro**
    **Aún no logro entender el propósito del currency_to alfanumerico, ya que en las transaccionales usamos numérico. A nivel transaccional se usa numerico**:
        - la fecha rate_date (ex) = fecha date_and_time_local_transactional (parquet)
        - el currecny_from_code (numerico ex) = currency_code_transaction (numerico parquet)
        - el currency_to (alfanumerico ex) = ccy (alfanumerico)
- Se concatenan los joins y los cols calculados de los montos con sus tipos de cambios:
    - dyn_cols_sql = "\n".join(dyn_cols)
    - dyn_joins_sql = "\n".join(dyn_joins)
- Se realiza query principal con with as:
    - Se genera la consulta alias TXN con los campos del parquet CLEAN:
        - file_id
        - ref_id
        - **Se deberia considerar traer tambien el fil_idn**
        - pan_de_2 (usado para issuer bin_8) **la ccolumna issuer_bin_8 no se utiliza en el set de reglas**
        - acquirer_reference_data_de_31 **no ubico donde se usaría**
        - electronic_commerce_indicator_3 (está comentado)
        - processing_code_de_3 **no ubico donde se usaría**
        - card_acceptor_business_code_[mcc]_de_26 alias card_acceptor_business_code
        - date_and_time_local_transaction_de_12 alias date_and_time_local_transaction
        - business_activity_4_pds_158_4 alias ird
        - amount_transaction_de_4 alias amount_transaction
        - currency_code_transaction_de_49 alias currency_code_transaction
        - mastercard_assigned_id_pds_176 alias mastercard_assigned_id (opcional para reglas) **Segun Fiorella, es obligatorio**
    - Se genera la consulta alias CALC coh los campos del parquet CALCULATION:
        - file_id
        - ref_id
        - **Se deberia considerar traer tambien el fil_idn**
        - jurisdiction_assigned as jurisdiction
        - gcms_product_identifier (opcional para reglas) **Según Fiorella, es obligatorio**
        - funding_source (opcional para reglas) **Según Fiorella, es obligatorio**
        - settlement_report_amount 
        - settlement_report_currency_code
    - Se hace la consulta principal:
        - FROM: TXN (parquet CLEAN)
        - INNER JOIN: CALC (parquet CALCULATION)
            - Keys: file_id y ref_id **Considerar el fil_idn**
        - LEFT JOIN: cur (df_curr) *Hace todo un select a la tabla de currency completa y trae los campos currency_numeric_code y currency_alphabetic_code. Para poder hacer la "traduccion" del campo de monea. y lo almacena como dataframe df_curr*
            - Keys: currency_numeric_code (cur) = currency_code_transaction (parquet CLEAN)
        - LEFT JOIN dinámicos: *dyn_joins* *{dyn_joins_sql}*
        - CAMPOS:
            - file_id
            - ref_id
            - **Se deberia considerar traer tambien el fil_idn**
            - COALESCE(TRIM(SUBSTR(CAST(t.pan AS VARCHAR), 1, 8)), 'BLANK') AS issuer_bin_8 **la ccolumna issuer_bin_8 no se utiliza en el set de reglas**
            - COALESCE(TRIM(SUBSTR(CAST(t.acquirer_reference_data AS VARCHAR), 2, 6)), 'BLANK') **no ubico donde se usaría**
            - -- COALESCE(TRIM(CAST(t.electronic_commerce_indicator_3 AS VARCHAR)), 'BLANK') AS electronic_commerce_indicator_3 (esta comentado)
            - COALESCE(TRIM(CAST(ca.jurisdiction AS VARCHAR)), 'BLANK') AS jurisdiction
            - COALESCE(TRIM(CAST(t.ird AS VARCHAR)), 'BLANK') AS ird
            - COALESCE(TRIM(SUBSTR(CAST(t.processing_code AS VARCHAR), 1, 2)), 'BLANK') AS processing_code **no ubico donde se usaría**
            - Montos/campos auxiliares (guardados como texto para compatibilidad legacy) **Warning: ya no usariamos campos como varchar, tood viene transformado a decimales**
                - COALESCE(CAST(t.amount_transaction AS VARCHAR), 'BLANK') AS amount_transaction
                - COALESCE(CAST(settlement_report_amount AS VARCHAR), 'BLANK') settlement_report_amount
                - COALESCE(CAST(settlement_report_currency_code AS VARCHAR), 'BLANK') settlement_report_currency_code
                - COALESCE(CAST(cur.currency_alphabetic_code AS VARCHAR), 'BLANK') AS amount_transaction_currency

                COALESCE(TRIM(CAST(t.card_acceptor_business_code AS VARCHAR)), 'BLANK') AS card_acceptor_business_code **Validar como lo traemos en standard 2.0, si es necesario pasarlo a varchar?**
            - Campos opcionales para reglas **WARNING: algunos campos de ahi (todos, si son obligatorios según Fiorella)**
                - COALESCE(TRIM(CAST(ca.gcms_product_identifier AS VARCHAR)), 'BLANK') AS gcms_product_identifier
                - COALESCE(TRIM(CAST(ca.funding_source AS VARCHAR)), 'BLANK') AS funding_source
                - COALESCE(CAST(t.mastercard_assigned_id AS VARCHAR), 'BLANK') AS mastercard_assigned_id
            
            - Campos para vigencia / FX posterior
                - CAST(strftime(t.date_and_time_local_transaction, '%Y-%m-%d') AS DATE) AS txn_date
                - try_cast(t.currency_code_transaction AS BIGINT) AS currency_code_transaction **No entiendo porque esto seria campo vigencia?**

            - Columnas dinámicas amount_transaction_{inserte aqui el alias dinámico}
                - {dyn_cols_sql}

- En resumen: te trae la combinación (joins tanto inner como left) para generar una tablá unica con todos los campos necesarios (montos, jurisdiccion, ird, fechas, productos, etc.) Para poder en un siguiente paso calcular el INTELICA_ID correcto.

##### Paso 2:
- Validar si el df_eval que es el datafame construido en el paso anterior no esté vacio.
- Copiar el dataframe constrido en el paso anterior (df_eval) y guardarlo como work
- Asignarle un id temproal (por el len) al work
- required = Set de columnas minimas necesarias: 
    - file_id
    - ref_id
    - **se deberia considerar el FILE_IDN**
    - jurisdiction
    - ird
    - txn_date (de_12 casteado como date)
- validar si alguna de esas columnas required faltan o no.
- Crear dataframe de todas las reglas de mc_rules en dataframe rules_raw
- Validar si no hay reglas.
    - llama a una funcionn local _merge_extras ? si no hay reglas
- llama a la funcion set_option de pandas 

### Consultas

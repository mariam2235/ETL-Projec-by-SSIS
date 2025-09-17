-- 06. Create staging table and ETL stored procedure
USE SSIS_Telecom_DB;
GO

-- Staging table to load raw CSV rows
IF OBJECT_ID('stg_telecom_raw','U') IS NOT NULL
    DROP TABLE stg_telecom_raw;
GO

CREATE TABLE stg_telecom_raw (
    raw_id INT IDENTITY(1,1) PRIMARY KEY,
    source_file_name VARCHAR(255),
    ID INT NULL,
    IMSI VARCHAR(50) NULL,
    IMEI VARCHAR(50) NULL,
    CELL INT NULL,
    LAC INT NULL,
    EVENT_TYPE VARCHAR(10) NULL,
    EVENT_TS VARCHAR(100) NULL,
    load_ts DATETIME DEFAULT GETDATE()
);
GO

-- Stored Procedure to process staging rows and apply business rules
IF OBJECT_ID('usp_ProcessStagingTelecom','P') IS NOT NULL
    DROP PROCEDURE usp_ProcessStagingTelecom;
GO

CREATE PROCEDURE usp_ProcessStagingTelecom
AS
BEGIN
    SET NOCOUNT ON;

    -- Cursor to process each staging row
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT raw_id, source_file_name, ID, IMSI, IMEI, CELL, LAC, EVENT_TYPE, EVENT_TS
        FROM stg_telecom_raw;

    DECLARE @raw_id INT,
            @source_file_name VARCHAR(255),
            @ID INT,
            @IMSI VARCHAR(50),
            @IMEI VARCHAR(50),
            @CELL INT,
            @LAC INT,
            @EVENT_TYPE VARCHAR(10),
            @EVENT_TS VARCHAR(100);

    OPEN cur;
    FETCH NEXT FROM cur INTO @raw_id, @source_file_name, @ID, @IMSI, @IMEI, @CELL, @LAC, @EVENT_TYPE, @EVENT_TS;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @reject BIT = 0;
        DECLARE @errorColumn INT = NULL;
        DECLARE @errorCode INT = 0;
        DECLARE @event_dt DATETIME = NULL;

        -- Validation rules
        IF @IMSI IS NULL OR LTRIM(RTRIM(@IMSI)) = '' BEGIN SET @reject=1; SET @errorCode=1; SET @errorColumn=2; END
        IF @reject=0 AND @CELL IS NULL BEGIN SET @reject=1; SET @errorCode=2; SET @errorColumn=4; END
        IF @reject=0 AND @LAC IS NULL BEGIN SET @reject=1; SET @errorCode=3; SET @errorColumn=5; END

        IF @reject=0 BEGIN
            SET @event_dt = TRY_CONVERT(DATETIME, @EVENT_TS, 103);
            IF @event_dt IS NULL SET @event_dt = TRY_CONVERT(DATETIME, @EVENT_TS, 120);
            IF @event_dt IS NULL BEGIN SET @reject=1; SET @errorCode=4; SET @errorColumn=8; END
        END

        IF @reject=1
        BEGIN
            INSERT INTO error_source_output ([Flat File Source Error Output Column], ErrorCode, ErrorColumn)
            VALUES (CONCAT('file=',ISNULL(@source_file_name,''),'|ID=',COALESCE(CONVERT(VARCHAR(20),@ID),'NULL'),
                           '|IMSI=',ISNULL(@IMSI,''),'|IMEI=',ISNULL(@IMEI,''),
                           '|CELL=',COALESCE(CONVERT(VARCHAR(20),@CELL),'NULL'),
                           '|LAC=',COALESCE(CONVERT(VARCHAR(20),@LAC),'NULL'),
                           '|EVENT_TYPE=',ISNULL(@EVENT_TYPE,''),'|EVENT_TS=',ISNULL(@EVENT_TS,'')),
                    @errorCode, @errorColumn);
        END
        ELSE
        BEGIN
            DECLARE @t_transaction_id INT = @ID;
            DECLARE @t_imsi VARCHAR(9) = LEFT(@IMSI,9);
            DECLARE @t_subscriber_id INT;
            SELECT TOP 1 @t_subscriber_id = subscriber_id FROM dim_imsi_reference WHERE imsi=@t_imsi;
            IF @t_subscriber_id IS NULL SET @t_subscriber_id = -99999;

            DECLARE @t_tac VARCHAR(8) = '-99999';
            DECLARE @t_snr VARCHAR(6) = '-99999';
            DECLARE @t_imei_final VARCHAR(14) = '-99999';

            IF @IMEI IS NOT NULL AND LEN(@IMEI) >= 14
            BEGIN
                SET @t_imei_final = LEFT(@IMEI,14);
                SET @t_tac = LEFT(@IMEI,8);
                SET @t_snr = RIGHT(@IMEI,6);
            END

            INSERT INTO fact_transaction (transaction_id, imsi, subscriber_id, tac, snr, imei, cell, lac, event_type, event_ts)
            VALUES (@t_transaction_id, @t_imsi, @t_subscriber_id, @t_tac, @t_snr, @t_imei_final, @CELL, @LAC, @EVENT_TYPE, @event_dt);
        END

        FETCH NEXT FROM cur INTO @raw_id, @source_file_name, @ID, @IMSI, @IMEI, @CELL, @LAC, @EVENT_TYPE, @EVENT_TS;
    END

    CLOSE cur; DEALLOCATE cur;
END
GO

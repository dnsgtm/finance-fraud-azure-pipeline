CREATE DATABASE finance_fraud_serving;
GO

USE finance_fraud_serving;
GO

CREATE VIEW vw_fraud_summary_by_state AS
SELECT *
FROM OPENROWSET(
    BULK 'https://storagefinancialfraud.dfs.core.windows.net/gold/fraud_summary_by_state/',
    FORMAT = 'DELTA'
) AS result;
GO

select * from vw_fraud_summary_by_state;


CREATE VIEW vw_fraud_summary_by_mcc AS
SELECT *
FROM OPENROWSET(
    BULK 'https://storagefinancialfraud.dfs.core.windows.net/gold/fraud_summary_by_mcc/',
    FORMAT = 'DELTA'
) AS result;
GO

CREATE VIEW vw_monthly_transaction_trends AS
SELECT *
FROM OPENROWSET(
    BULK 'https://storagefinancialfraud.dfs.core.windows.net/gold/monthly_transaction_trends/',
    FORMAT = 'DELTA'
) AS result;
GO

CREATE VIEW vw_customer_risk_profile AS
SELECT *
FROM OPENROWSET(
    BULK 'https://storagefinancialfraud.dfs.core.windows.net/gold/customer_risk_profile/',
    FORMAT = 'DELTA'
) AS result;
GO

SELECT TOP 10 * FROM vw_fraud_summary_by_mcc;
SELECT TOP 10 * FROM vw_monthly_transaction_trends;
SELECT TOP 10 * FROM vw_customer_risk_profile;
create database Bank_analytics;
use Bank_analytics;

CREATE TABLE Credit_Debit_data (
    CustomerID VARCHAR(36),
    CustomerName VARCHAR(255),
    AccountNumber VARCHAR(20),
    TransactionDate date,
    TransactionType ENUM('Credit', 'Debit'),
    Amount DECIMAL(10, 2),
    Balance DECIMAL(10, 2),
    Description VARCHAR(255),
    Branch VARCHAR(100),
    Transaction_method VARCHAR(50),
    Currency VARCHAR(3),
    BankName VARCHAR(100)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Credit and debit data.csv'
INTO TABLE Credit_Debit_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
CustomerID,
CustomerName,
AccountNumber,
@TransactionDate,
TransactionType,
Amount,
Balance,
Description,
Branch,
Transaction_method,
Currency,
BankName
)
SET TransactionDate = STR_TO_DATE(@TransactionDate, '%d-%m-%Y');


select * from credit_debit_data;

# 1. Total Credit Amount
Create table Credit_Amt as 
SELECT Concat(Round(SUM(Amount)/1000000,2),' Million') AS total_credit_amount
FROM credit_debit_data
WHERE TransactionType = 'Credit';

# 2. Total Debit Amount
Create table Debit_Amt as 
SELECT Concat(Round(SUM(Amount)/1000000,2),' Million') AS total_debit_amount
FROM credit_debit_data
WHERE TransactionType = 'Debit';

# 3. Credit to Debit Ratio
Create table Credit_Debit_ratio as
SELECT ROUND(
        (SELECT SUM(Amount) FROM credit_debit_data WHERE TransactionType = 'Credit') /
        (SELECT SUM(Amount) FROM credit_debit_data WHERE TransactionType = 'Debit'), 3
    ) AS Credit_to_Debit_Ratio;

## 4. Net Transaction Amount
CREATE TABLE Net_tranx_Amt AS
SELECT CONCAT(ROUND(((SELECT SUM(Amount) FROM credit_debit_data WHERE TransactionType = 'Credit') -
(SELECT SUM(Amount) FROM credit_debit_data WHERE TransactionType = 'Debit')) / 1000000,2),' Million') 
AS Net_Transaction_Amount;


# 5. Account Activity Ratio
create table Acc_activity_ratio as
SELECT COUNT(CustomerID) / SUM(Balance) AS Account_Activity_Ratio FROM credit_debit_data;

## 6. Transactions per Day / Week / Month
SELECT 
    DATE(TransactionDate) AS Transaction_Day,
    COUNT(*) AS Transactions_Count
FROM credit_debit_data
GROUP BY DATE(TransactionDate);


### Per Month
create table Monthly_Tranx as 
SELECT 
    DATE_FORMAT(TransactionDate, '%Y-%m') AS Transaction_Month,
    COUNT(*) AS Transactions_Count
FROM credit_debit_data
GROUP BY DATE_FORMAT(TransactionDate, '%Y-%m');


## 7. Total Transaction Amount by Branch

create table Trans_by_Branch as 
SELECT Branch,Concat(round(SUM(Amount)/1000000,2),' Millons') AS Total_Transaction_Amount FROM credit_debit_data GROUP BY Branch ORDER BY sum(Amount);

# 8. Transaction Volume by Bank
Create table Transaction_Volume as 
SELECT BankName,concat(round(SUM(Amount)/1000000,2),' Millions') AS Total_Transaction_Amount FROM credit_debit_data GROUP BY BankName Order by 
sum(Amount);

## 9. Transaction Method Distribution

CREATE TABLE Trans_method_Distribution AS
SELECT Transaction_method,COUNT(CustomerID) AS Tot_Count, CONCAT(ROUND(COUNT(CustomerID) * 100.0 /
(SELECT COUNT(CustomerID) FROM credit_debit_data),2),'%') AS Percentage FROM credit_debit_data 
 GROUP BY Transaction_method;


## 10. Branch Transaction Growth (% Change)

CREATE TABLE Branch_tranx_Growth AS
SELECT Branch,DATE_FORMAT(TransactionDate, '%Y-%m') AS Month,Concat(ROUND(SUM(Amount) / 1000000, 2),' Millions') AS Monthly_Amount,
IFNULL(Concat(Round(LAG(SUM(Amount)) OVER (PARTITION BY Branch ORDER BY DATE_FORMAT(TransactionDate, '%Y-%m'))/1000000,2),' Millions'),'NA')
 AS Previous_Month, IFNULL(CONCAT(ROUND(((SUM(Amount) - LAG(SUM(Amount)) 
OVER (PARTITION BY Branch ORDER BY DATE_FORMAT(TransactionDate, '%Y-%m')))/ LAG(SUM(Amount)) OVER (PARTITION BY Branch 
ORDER BY DATE_FORMAT(TransactionDate, '%Y-%m'))) * 100, 2),'%'),'NA') AS Growth_Percentage 
FROM credit_debit_data GROUP BY Branch, Month;

# 11. High-Risk Transaction Flag
SELECT *,
    CASE
        WHEN amount > 4000.00 THEN 'High Risk' 
        ELSE 'Normal'
    END AS risk_flag
FROM credit_debit_data
ORDER BY amount DESC
LIMIT 100;

create table  High_Risk_Count as
SELECT COUNT(*) AS High_Risk_Count
   FROM credit_debit_data WHERE Amount > 4000;

# 12. Suspicious Transaction Frequency
create table Suspicious_tranx_freq as 
SELECT COUNT(*) AS Suspicious_Transactions FROM credit_debit_data WHERE Amount > 4000;
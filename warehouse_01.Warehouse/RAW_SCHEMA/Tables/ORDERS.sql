CREATE TABLE [RAW_SCHEMA].[ORDERS] (
    [ORDER_ID]     INT             NOT NULL,
    [CUSTOMER_ID]  INT             NULL,
    [PRODUCT_ID]   INT             NULL,
    [ORDER_DATE]   DATE            NULL,
    [QUANTITY]     INT             NULL,
    [TOTAL_AMOUNT] DECIMAL (12, 2) NULL,
    [ORDER_STATUS] VARCHAR (30)    NULL
);


GO
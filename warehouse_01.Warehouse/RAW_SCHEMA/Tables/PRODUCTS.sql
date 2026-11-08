CREATE TABLE [RAW_SCHEMA].[PRODUCTS] (
    [PRODUCT_ID]     INT             NOT NULL,
    [PRODUCT_NAME]   VARCHAR (100)   NULL,
    [CATEGORY]       VARCHAR (100)   NULL,
    [UNIT_PRICE]     DECIMAL (12, 2) NULL,
    [STOCK_QUANTITY] INT             NULL,
    [CREATED_DATE]   DATE            NULL
);


GO
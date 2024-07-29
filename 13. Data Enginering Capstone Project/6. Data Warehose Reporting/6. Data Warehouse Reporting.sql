SELECT * FROM public."DimDate" LIMIT 5;

SELECT * FROM public."DimCategory" LIMIT 5;

SELECT * FROM public."DimCountry" LIMIT 5;

SELECT * FROM public."FactSales" LIMIT 5;

SELECT country, category, sum(amount) AS totalsales
FROM "FactSales"
LEFT JOIN "DimCountry"
ON "FactSales".countryid = "DimCountry".countryid
LEFT JOIN "DimCategory"
ON "FactSales".categoryid = "DimCategory".categoryid
GROUP BY GROUPING SETS (country,category)
ORDER BY country,category;

SELECT year, country, sum(amount) AS totalsales
FROM "FactSales"
LEFT JOIN "DimDate"
ON "FactSales".dateid = "DimDate".dateid
LEFT JOIN "DimCountry"
ON "FactSales".countryid = "DimCountry".countryid
GROUP BY ROLLUP (year,country)
ORDER BY year,country;

SELECT year, country, avg(amount) AS averagesales
FROM "FactSales"
LEFT JOIN "DimDate"
ON "FactSales".dateid = "DimDate".dateid
LEFT JOIN "DimCountry"
ON "FactSales".countryid = "DimCountry".countryid
GROUP BY CUBE (year,country)
ORDER BY year,country;

CREATE MATERIALIZED VIEW total_sales_per_country (country, total_sales) AS
    (
    SELECT country, sum(amount)
    FROM "FactSales"
    LEFT JOIN "DimCountry"
    ON "FactSales".countryid="DimCountry".countryid
    GROUP BY country
    );
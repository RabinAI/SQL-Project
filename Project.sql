SELECT *
FROM raw_file;

CREATE TABLE raw_staging
LIKE raw_file;

SELECT * 
FROM raw_staging;

INSERT raw_staging
SELECT *
FROM raw_file;

SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off,`date`) AS row_num
FROM raw_staging;

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM raw_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num >1;
-- those who has the value of row num greater than 1 is duplicate

SELECT * 
FROM raw_staging
WHERE company= 'Casper'

CREATE TABLE `raw_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



INSERT INTO raw_staging2
SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM raw_staging;

SELECT * FROM raw_staging2
WHERE row_num >1;

DELETE 
FROM raw_staging2
WHERE row_num>1;

-- standarizing the data
SELECT company, TRIM(company)
FROM raw_staging2;

UPDATE raw_staging2
SET company = TRIM(company)

SELECT * 
FROM raw_staging2
WHERE industry LIKE 'Crypto%';

UPDATE raw_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry FROM
raw_staging2
WHERE industry='Crypto'; 

SELECT DISTINCT Country, TRIM(TRAILING '.' FROM country)
FROM raw_staging2
ORDER BY 1;

UPDATE raw_staging2
SET Country = TRIM(TRAILING '.' FROM Country)
WHERE Country LIKE 'United States%';

SELECT *
FROM raw_staging2;


SELECT `date`,
STR_TO_DATE (`date`, '%m/%d/%Y')
FROM raw_staging2;

UPDATE raw_staging2
SET `date` = STR_TO_DATE (`date`, '%m/%d/%Y');

ALTER TABLE raw_staging2
MODIFY COLUMN `date` DATE;

SELECT * 
FROM raw_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT * 
FROM raw_staging2
WHERE industry IS NULL
OR industry = '';


UPDATE raw_staging2 
SET industry = NULL
WHERE industry = '';
SELECT * 
FROM raw_staging2
WHERE company= 'Airbnb';

SELECT t1.industry, t2.industry
FROM raw_staging2 t1
JOIN raw_staging2 t2
	ON t1.company = t2.company
    AND t1.location= t2.location
WHERE  t1.industry IS NULL 
AND t2.industry IS NOT NULL


UPDATE raw_staging2 t1
JOIN raw_staging2 t2
	ON t1.company = t2.company
SET t1.industry= t2.industry
WHERE 	t1.industry IS NULL
AND t2.industry IS NOT NULL;


SELECT total_laid_off, percentage_laid_off
FROM raw_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;
-- we have to delete all the null value row as we cannot populate these data based on our datasets

DELETE 
FROM raw_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


ALTER TABLE raw_staging2
DROP COLUMN row_num;

-- Exploratory Data Analysis

SELECT * FROM raw_staging2;


SELECT MAX(total_laid_off) , MAX(percentage_laid_off)
FROM raw_staging2;

SELECT * FROM raw_staging2
WHERE percentage_laid_off=1
ORDER BY funds_raised_millions DESC;

SELECT  company ,SUM(total_laid_off)
FROM raw_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM raw_staging2;

SELECT  industry ,SUM(total_laid_off)
FROM raw_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT  country ,SUM(total_laid_off)
FROM raw_staging2
GROUP BY country
ORDER BY 2 DESC;


SELECT  YEAR(`date`) ,SUM(total_laid_off)
FROM raw_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT  stage ,SUM(total_laid_off)
FROM raw_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT  company ,SUM(percentage_laid_off)
FROM raw_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`, 6,2) AS `MONTH` , SUM(total_laid_off)
FROM raw_staging2
GROUP BY `MONTH`;

SELECT SUBSTRING(`date`, 1,7) AS `MONTH` , SUM(total_laid_off)
FROM raw_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1,7) AS `MONTH` , SUM(total_laid_off) AS total_off
FROM raw_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off,SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

SELECT  company ,YEAR(`date`),SUM(total_laid_off)
FROM raw_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 desc;

-- finding the highest laid off using rank
WITH Company_Year(company, years,total_laid_off) AS
(
SELECT  company ,YEAR(`date`),SUM(total_laid_off)
FROM raw_staging2
GROUP BY company,YEAR(`date`)
), Company_Year_Rank AS

(SELECT * ,
DENSE_RANK()OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
WHERE Ranking<=5
;


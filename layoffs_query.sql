CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * from layoffs_staging;

INSERT layoffs_staging
SELECT * FROM layoffs;

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions)
AS row_num
FROM layoffs_staging
)

SELECT * FROM duplicate_cte where row_num > 1;

CREATE TABLE `layoffs2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions)
AS row_num
FROM layoffs_staging;

SELECT * from layoffs2 where row_num>1;

DELETE from layoffs2 where row_num>1;
-- STANDARDIZING DATA

SELECT company, TRIM(company) FROM layoffs2;

UPDATE layoffs2 SET company = TRIM(company);

SELECT * FROM layoffs2;

SELECT DISTINCT industry FROM layoffs2 ORDER BY 1;

SELECT DISTINCT industry FROM layoffs2 WHERE industry LIKE "crypto%";

UPDATE layoffs2 SET industry = 'crypto' where industry like "crypto%";

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs2
ORDER BY 1;

UPDATE layoffs2 SET country = TRIM(TRAILING '.' FROM country);

SELECT date, STR_TO_DATE(date,'%m/%d/%Y')
FROM layoffs2;

UPDATE layoffs2 SET date = STR_TO_DATE(date,'%m/%d/%Y');

ALTER TABLE layoffs2 MODIFY COLUMN date DATE;

SELECT * FROM 
layoffs2 WHERE total_laid_off is NULL
AND percentage_laid_off is NULL;

SELECT * FROM layoffs2
WHERE industry is NULL or industry = "";

SELECT * FROM layoffs2
WHERE company = "airbnb";

SELECT t1.company,t1.industry,t2.industry FROM layoffs2 t1 join layoffs2 t2 ON
t1.company = t2.company
WHERE (t1.industry is NULL OR t1.industry = '')
AND t2.industry is NOT NULL;

UPDATE layoffs2 SET industry = NULL 
WHERE industry = ''; 

UPDATE layoffs2 t1 join layoffs2 t2 ON
t1.company = t2.company 
SET t1.industry = t2.industry
WHERE t1.industry is NULL 
AND t2.industry is NOT NULL;

SELECT * from layoffs2 WHERE industry is NULL OR industry = '';

SELECT * FROM 
layoffs2 WHERE total_laid_off is NULL
AND percentage_laid_off is NULL;

DELETE FROM layoffs2
WHERE total_laid_off is NULL
AND percentage_laid_off is NULL;

ALTER TABLE layoffs2
DROP column row_num;

SELECT * FROM 
layoffs2;

-- Exploratory Data Analysis

SELECT MAX(total_laid_off) , MAX(percentage_laid_off)
FROM layoffs2;

SELECT * FROM layoffs2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT * FROM layoffs2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off) 
FROM layoffs2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(date), MAX(date)
FROM layoffs2;

SELECT industry, SUM(total_laid_off) 
FROM layoffs2
GROUP BY industry
ORDER BY 2 DESC;

SELECT date, SUM(total_laid_off) 
FROM layoffs2
GROUP BY date
ORDER BY 1 DESC;

SELECT SUBSTRING(date,1,7) month, SUM(total_laid_off) 
FROM layoffs2
GROUP BY month
ORDER BY 1;

with Rolling_total AS(
SELECT SUBSTRING(date,1,7) month, SUM(total_laid_off) AS total_off
FROM layoffs2
GROUP BY month
ORDER BY 1)

SELECT month, SUM(total_off) OVER(ORDER BY month)
FROM Rolling_total






















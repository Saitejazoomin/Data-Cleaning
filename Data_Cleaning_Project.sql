-- delecting duplicates

SELECT * FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT * FROM layoffs;

SELECT *, 
ROW_NUMBER() OVER( PARTITION BY company, location , industry, total_laid_off, percentage_laid_off, `date`
)  row_num
FROM layoffs_staging;

WITH dupilcate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER( PARTITION BY company, location , industry, total_laid_off, percentage_laid_off, `date`, country, stage, funds_raised_millions
)  row_num
FROM layoffs_staging
)
 SELECT * FROM dupilcate_cte 
 WHERE row_num >1;
 
 SELECT * FROM layoffs_staging
 WHERE company = 'Oda';
 
 SELECT * FROM layoffs_staging
 WHERE company = 'Casper';
 
 
 CREATE TABLE `layoffs_staging2` (
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


SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER( PARTITION BY company, location , industry, total_laid_off, percentage_laid_off, `date`, country, stage, funds_raised_millions
)  row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;

SELECT * 
FROM layoffs_staging2
WHERE country = 'India' Order by company;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT distinct(industry)
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT * 
FROM layoffs_staging2;

SELECT distinct country
FROM layoffs_staging2;

SELECT distinct  trim(TRAILING '.' FROM country)
FROM layoffs_staging2;

SELECT distinct(country)
FROM layoffs_staging2
WHERE country LIKE 'United%';

UPDATE  layoffs_staging2
SET country =  trim(TRAILING '.' FROM country)
WHERE country LIKE 'United%';

SELECT distinct country
FROM layoffs_staging2
ORDER BY 1;

SELECT `date`
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';


SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT t1.industry ,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
	WHERE (t1.industry IS NULL OR t1.industry = '')
    AND t2.industry IS NOT NULL;
    
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
    AND t2.industry IS NOT NULL;
    
SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;



-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

SELECT max(total_laid_off) , MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country ,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`) ,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

SELECT stage ,SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT company,SUM(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

WITH Rolling_total AS
(SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC)
SELECT `Month`,total_off, SUM(total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM Rolling_total;


SELECT company, YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_year (Company, Years,total_laid_off) AS
(SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
), Company_Year_Rank AS 
 (SELECT *,
DENSE_RANK () OVER( PARTITION BY Years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_year
WHERE Years IS NOT NULL
) SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;
 

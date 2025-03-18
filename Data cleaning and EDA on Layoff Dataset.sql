SELECT  * FROM layoffs;

CREATE  TABLE layoffs_staging LIKE layoffs;
SELECT * FROM layoff_staging;
 
 INSERT layoff_staging SELECT * FROM layoffs;
 
 CREATE TABLE layoff_staging LIKE layoffs;


#REMOVING DUPLICATES
SELECT * FROM layoff_staging;

SELECT * ,
ROW_NUMBER() over(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
 FROM layoff_staging;
 
 WITH duplicate_cte AS
( SELECT * ,
ROW_NUMBER() over(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
 FROM layoff_staging)
 SELECT * FROM duplicate_cte
 WHERE row_num>1;
 

CREATE TABLE `layoff_staging2` (
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

SELECT * FROM layoff_staging2;

INSERT INTO layoff_staging2
SELECT * ,
ROW_NUMBER() over(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
 FROM layoff_staging;

SELECT * FROM layoff_staging2
WHERE row_num>1; 

SET SQL_SAFE_UPDATES = 0;


DELETE FROM layoff_staging2
WHERE row_num>1;


-- standardizing data

SELECT company,TRIM(company)
FROM layoff_staging2;

UPDATE layoff_staging2 SET company=TRIM(company);
SELECT DISTINCT industry FROM layoff_staging2 ORDER BY 1;

SELECT * FROM layoff_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoff_staging2
SET industry='crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT country 
FROM layoff_staging2 ORDER BY 1;



UPDATE layoff_staging2
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'united states%';

SELECT `date`, 
STR_TO_DATE (`date`, '%m/%d/%Y')
FROM layoff_staging2;

UPDATE layoff_staging2
SET `date`=STR_TO_DATE (`date`, '%m/%d/%Y');

ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;


-- Handling Null values

SELECT * FROM layoff_staging2 
WHERE company IS NULL OR company ='';

SELECT * FROM layoff_staging2 
WHERE location IS NULL OR location='';

SELECT * FROM layoff_staging2 
WHERE industry IS NULL OR industry ='';


SELECT * FROM layoff_staging2 
WHERE total_laid_off IS NULL OR total_laid_off ='';

SELECT * FROM layoff_staging2 
WHERE percentage_laid_off IS NULL OR percentage_laid_off ='';

SELECT * FROM layoff_staging2 
WHERE `date` IS NULL OR `date` ='';

SELECT * FROM layoff_staging2 
WHERE stage IS NULL OR stage ='';

SELECT * FROM layoff_staging2 
WHERE funds_raised_millions IS NULL OR funds_raised_millions ='';

SELECT * 
FROM layoff_staging2 t1
JOIN  layoff_staging2 t2
      ON t1.company=t2.company
	
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL;

SELECT t1.industry,t2.industry
FROM layoff_staging2 t1
JOIN  layoff_staging2 t2
      ON t1.company=t2.company
	
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoff_staging2
SET industry=NULL 
WHERE industry='';

UPDATE layoff_staging2 t1
JOIN  layoff_staging2 t2
      ON t1.company=t2.company
      SET t1.industry=t2.industry
	
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT * FROM layoff_staging2
WHERE company='Airbnb';


DELETE FROM layoff_staging2
WHERE (total_laid_off IS NULL or total_laid_off='')
AND (percentage_laid_off IS NULL or percentage_laid_off='');

ALTER TABLE layoff_staging2
DROP COLUMN row_num;
 
SELECT * FROM layoff_staging2
WHERE (percentage_laid_off IS NULL or percentage_laid_off='');

SELECT * FROM layoff_staging2;



-- exploratory data analysis


SELECT max(total_laid_off),max(percentage_laid_off)
FROM layoff_staging2;

SELECT * FROM layoff_staging2
WHERE percentage_laid_off=1
ORDER BY total_laid_off DESC;

SELECT company,SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT industry,SUM(total_laid_off)
FROM layoff_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT `date`,SUM(total_laid_off)
FROM layoff_staging2
GROUP BY `date`
ORDER BY 2 DESC;

SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoff_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT company,SUM(percentage_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company,AVG(percentage_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT substring(`date`,7,1) AS `MONTH`,SUM(total_laid_off)
FROM layoff_staging2
WHERE substring(`date`,7,1) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

SELECT company, YEAR(`date`),SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC;

with company_year(company,years,total_laid_off) AS(
SELECT company, YEAR(`date`),SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company,YEAR(`date`)
)
SELECT *,DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE years IS NOT NULL
ORDER BY Ranking ASC;




 
 
 
 
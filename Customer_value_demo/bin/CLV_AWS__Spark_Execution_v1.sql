	 SELECT 
		TB_CLV_SDF.*,
	( CASE
	WHEN ( ((abs(`frequency` - 7.0e0) <= 10e-9) OR  ( `frequency`  >= 8.0e0 AND `frequency`  <= 1.3e1 ) ) AND ((abs(month(`dt_first_Invoice`) - 1.0e0) <= 10e-9) OR (abs(month(`dt_first_Invoice`) - 2.0e0) <= 10e-9) OR (abs(month(`dt_first_Invoice`) - 3.0e0) <= 10e-9) OR (abs(month(`dt_first_Invoice`) - 4.0e0) <= 10e-9) OR (abs(month(`dt_first_Invoice`) - 5.0e0) <= 10e-9) OR (abs(month(`dt_first_Invoice`) - 6.0e0) <= 10e-9) OR (abs(month(`dt_first_Invoice`) - 8.0e0) <= 10e-9) OR (abs(month(`dt_first_Invoice`) - 1.0e1) <= 10e-9) ) ) THEN 4
	WHEN ( ( ( `recency_now`  >= 4.109e3 AND `recency_now`  <= 4.113e3 ) ) AND ((abs(`frequency` - 6.0e0) <= 10e-9) OR (abs(`frequency` - 7.0e0) <= 10e-9) OR  ( `frequency`  >= 8.0e0 AND `frequency`  <= 1.3e1 ) ) ) THEN 9
	WHEN ( ((abs(year(`dt_first_Invoice`) - 2.01e3) <= 10e-9) OR ( (`dt_first_Invoice` IS NULL ) ) ) AND ((abs(`frequency` - 1.0e0) <= 10e-9) OR (abs(`frequency` - 2.0e0) <= 10e-9) OR (abs(`frequency` - 3.0e0) <= 10e-9) OR (abs(`frequency` - 4.0e0) <= 10e-9) OR (abs(`frequency` - 5.0e0) <= 10e-9) OR (abs(`frequency` - 6.0e0) <= 10e-9) OR ( (`frequency` IS NULL ) ) ) AND ( ( (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  >= 4.0e0 AND (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  <= 3.4e2 ) OR ( (`dt_last_Invoice` IS NULL ) ) ) ) THEN 2
	WHEN ( ((abs(`frequency` - 7.0e0) <= 10e-9) OR  ( `frequency`  >= 8.0e0 AND `frequency`  <= 1.3e1 ) ) AND ((abs(year(`dt_first_Invoice`) - 2.01e3) <= 10e-9) OR ( (`dt_first_Invoice` IS NULL ) ) ) ) THEN 1
	WHEN ( ( ( `frequency`  > 1.0e1 AND `frequency`  <= 1.14e2 ) ) ) THEN 10
	WHEN ( ( ( `recency_now`  >= 4.109e3 AND `recency_now`  <= 4.113e3 ) ) AND ((abs(`frequency` - 1.0e0) <= 10e-9) OR (abs(`frequency` - 2.0e0) <= 10e-9) OR (abs(`frequency` - 3.0e0) <= 10e-9) OR (abs(`frequency` - 4.0e0) <= 10e-9) OR (abs(`frequency` - 5.0e0) <= 10e-9) OR (abs(`frequency` - 6.0e0) <= 10e-9) OR ( (`frequency` IS NULL ) ) ) ) THEN 5
	WHEN ( ((abs(month(`dt_last_Invoice`) - 1.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 2.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 3.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 4.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 5.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 6.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 7.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 8.0e0) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 9.0e0) <= 10e-9) ) AND ((abs(day(`dt_last_Invoice`) - 1.0e0) <= 10e-9) OR  ( day(`dt_last_Invoice`)  >= 2.0e0 AND day(`dt_last_Invoice`)  <= 3.0e0 ) OR (abs(day(`dt_last_Invoice`) - 4.0e0) <= 10e-9) OR (abs(day(`dt_last_Invoice`) - 5.0e0) <= 10e-9) OR  ( day(`dt_last_Invoice`)  >= 9.0e0 AND day(`dt_last_Invoice`)  <= 1.0e1 ) OR (abs(day(`dt_last_Invoice`) - 1.1e1) <= 10e-9) OR  ( day(`dt_last_Invoice`)  >= 1.2e1 AND day(`dt_last_Invoice`)  <= 1.3e1 ) OR (abs(day(`dt_last_Invoice`) - 1.4e1) <= 10e-9) OR  ( day(`dt_last_Invoice`)  >= 1.5e1 AND day(`dt_last_Invoice`)  <= 1.6e1 ) OR  ( day(`dt_last_Invoice`)  >= 1.7e1 AND day(`dt_last_Invoice`)  <= 1.8e1 ) OR  ( day(`dt_last_Invoice`)  >= 1.9e1 AND day(`dt_last_Invoice`)  <= 2.0e1 ) OR (abs(day(`dt_last_Invoice`) - 2.1e1) <= 10e-9) OR (abs(day(`dt_last_Invoice`) - 2.2e1) <= 10e-9) OR  ( day(`dt_last_Invoice`)  >= 2.3e1 AND day(`dt_last_Invoice`)  <= 2.4e1 ) OR  ( day(`dt_last_Invoice`)  >= 2.5e1 AND day(`dt_last_Invoice`)  <= 2.7e1 ) OR  ( day(`dt_last_Invoice`)  >= 2.8e1 AND day(`dt_last_Invoice`)  <= 3.1e1 ) OR ( (`dt_last_Invoice` IS NULL ) ) ) AND ((abs(year(`dt_first_Invoice`) - 2.011e3) <= 10e-9) ) ) THEN 6
	WHEN ( ((abs(day(`dt_last_Invoice`) - 6.0e0) <= 10e-9) OR (abs(day(`dt_last_Invoice`) - 7.0e0) <= 10e-9) OR (abs(day(`dt_last_Invoice`) - 8.0e0) <= 10e-9) OR  ( day(`dt_last_Invoice`)  >= 9.0e0 AND day(`dt_last_Invoice`)  <= 1.0e1 ) ) AND ( ( (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  >= 4.0e0 AND (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  <= 2.52e2 ) OR  ( (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  > 2.79e2 AND (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  <= 2.91e2 ) OR  ( (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  > 3.08e2 AND (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  <= 3.13e2 ) OR ( (`dt_last_Invoice` IS NULL ) ) ) ) THEN 7
	WHEN ( ( ( (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  > 2.65e2 AND (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  <= 3.08e2 ) OR  ( (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  > 3.13e2 AND (datediff(concat(year(`dt_last_Invoice`),'-',month(`dt_last_Invoice`),'-',day(`dt_last_Invoice`)),concat(year(`dt_last_Invoice`),'-01-01')) + 1)  <= 3.28e2 ) ) AND ((abs(`frequency` - 1.0e0) <= 10e-9) OR (abs(`frequency` - 2.0e0) <= 10e-9) OR (abs(`frequency` - 3.0e0) <= 10e-9) OR (abs(`frequency` - 4.0e0) <= 10e-9) OR ( (`frequency` IS NULL ) ) ) AND ((abs(month(`dt_last_Invoice`) - 1.0e1) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 1.1e1) <= 10e-9) OR ( (`dt_last_Invoice` IS NULL ) ) ) ) THEN 8
	WHEN ( ( ( `recency_now`  >= 4.112e3 AND `recency_now`  <= 4.152e3 ) OR  ( `recency_now`  > 4.434e3 AND `recency_now`  <= 4.483e3 ) ) AND ((abs(`frequency` - 1.0e0) <= 10e-9) OR (abs(`frequency` - 2.0e0) <= 10e-9) OR (abs(`frequency` - 3.0e0) <= 10e-9) OR (abs(`frequency` - 4.0e0) <= 10e-9) OR (abs(`frequency` - 5.0e0) <= 10e-9) OR (abs(`frequency` - 6.0e0) <= 10e-9) OR ( (`frequency` IS NULL ) ) ) AND ((abs(month(`dt_last_Invoice`) - 1.1e1) <= 10e-9) OR (abs(month(`dt_last_Invoice`) - 1.2e1) <= 10e-9) OR ( (`dt_last_Invoice` IS NULL ) ) ) ) THEN 3
	ELSE 11
	END ) AS kc_monetary_value 	
	FROM TB_CLV_SDF

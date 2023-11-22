DROP TABLE IF EXISTS alerts2; 

CREATE TABLE alerts2 AS
SELECT 
    id, 
    tag
FROM alerts;

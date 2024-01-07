-- REMOVE COLUNA DATA DE GERAÇÃO DE TODAS AS TABELAS CASO EXISTA

-- Check if column exists in each table
SELECT DISTINCT(table_name)
INTO #temp_tables
FROM information_schema.columns
WHERE column_name = 'DatGeracaoConjuntoDados';

-- Generate and execute ALTER TABLE statements for each table
DECLARE @table_name sysname, @sql nvarchar(max);

WHILE (SELECT COUNT(*) FROM #temp_tables) > 0
BEGIN
  SELECT TOP 1 @table_name = table_name
  FROM #temp_tables;

  SET @sql = 'ALTER TABLE ' + QUOTENAME(@table_name) + ' DROP COLUMN DatGeracaoConjuntoDados;';
  EXEC sp_executesql @sql;

  DELETE FROM #temp_tables WHERE table_name = @table_name;
END;

-- Clean up temporary table
DROP TABLE #temp_tables;



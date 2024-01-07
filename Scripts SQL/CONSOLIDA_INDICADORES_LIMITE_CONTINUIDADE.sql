

DROP TABLE IF EXISTS DIS_ID_UF;

CREATE TABLE DIS_ID_UF AS
	SELECT DISTINCT IdeConjUnidConsumidoras,SigUF FROM DIS_ID_municipios;
	
DROP TABLE IF EXISTS DIS_ID_cont_consolidado;

CREATE TABLE DIS_ID_cont_consolidado AS
SELECT DISTINCT SigAgente,NumCNPJ,IdeConjUndConsumidoras,DscConjUndConsumidoras,SigIndicador,AnoIndice,NumPeriodoIndice,VlrIndiceEnviado,DscIndicador,SigUF
FROM(
	(SELECT *
		FROM(
			SELECT DatGeracaoConjuntoDados,SigAgente,NumCNPJ,IdeConjUndConsumidoras,DscConjUndConsumidoras,SigIndicador,AnoIndice,NumPeriodoIndice,VlrIndiceEnviado FROM DIS_ID_cont_2000_2009
			UNION
			SELECT DatGeracaoConjuntoDados,SigAgente,NumCNPJ,IdeConjUndConsumidoras,DscConjUndConsumidoras,SigIndicador,AnoIndice,NumPeriodoIndice,VlrIndiceEnviado FROM DIS_ID_cont_2010_2019
			UNION
			SELECT DatGeracaoConjuntoDados,SigAgente,NumCNPJ,IdeConjUndConsumidoras,DscConjUndConsumidoras,SigIndicador,AnoIndice,NumPeriodoIndice,VlrIndiceEnviado FROM DIS_ID_cont_2020_2029) AS subquery
		JOIN DIS_ID_indqual_dominio
		ON subquery.SigIndicador = DIS_ID_indqual_dominio.SigIndicador )) AS subquery2
	JOIN DIS_ID_UF
	ON subquery2.IdeConjUndConsumidoras = DIS_ID_UF.IdeConjUnidConsumidoras

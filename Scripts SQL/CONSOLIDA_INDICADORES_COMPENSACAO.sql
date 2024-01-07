DROP TABLE IF EXISTS DIST_IND_COMPENSACAO_CONSOLIDADO;

CREATE TABLE DIST_IND_COMPENSACAO_CONSOLIDADO AS
SELECT *
FROM(
	(SELECT *
		FROM(
			SELECT DatGeracaoConjuntoDados,SigAgente,NumCNPJ,IdeConjUndConsumidoras,DscConjUndConsumidoras,SigIndicador,AnoIndice,NumPeriodoIndice,VlrIndiceEnviado FROM DIST_IND_CONT_COL_COMP_2010_2019
			UNION
			SELECT DatGeracaoConjuntoDados,SigAgente,NumCNPJ,IdeConjUndConsumidoras,DscConjUndConsumidoras,SigIndicador,AnoIndice,NumPeriodoIndice,VlrIndiceEnviado FROM DIST_IND_CONT_COL_COMP_2020_2029) as subquery
			
			JOIN DIST_IND_UF
			ON subquery.IdeConjUndConsumidoras = DIST_IND_UF.IdeConjUnidConsumidoras));

ALTER TABLE DIST_IND_COMPENSACAO_CONSOLIDADO
DROP COLUMN IdeConjUnidConsumidoras;


DROP TABLE IF EXISTS DIS_Int_emer_consolidado;

CREATE TABLE DIS_Int_emer_consolidado AS
SELECT NomAgente,NumCPFCNPJ,NumOcorrencia,NumConjunto,DthInicioOcorrenciaAberta,DscCanalAtendimento,DthFimOcorrenciaAberta,DscOcorrenciaAberta,DscNumInterrupcao,MdaPreparo,MdaDeslocamento,MdaExecucao,NumVeiculo,CodIBGE
FROM(
	(SELECT *
		FROM(
			SELECT NomAgente,NumCPFCNPJ,NumOcorrencia,NumConjunto,DthInicioOcorrenciaAberta,DscCanalAtendimento,DthFimOcorrenciaAberta,DscOcorrenciaAberta,DscNumInterrupcao,MdaPreparo,MdaDeslocamento,MdaExecucao,NumVeiculo,CodIBGE FROM DIS_Int_emer_dist_energy_2017
			UNION
			SELECT NomAgente,NumCPFCNPJ,NumOcorrencia,NumConjunto,DthInicioOcorrenciaAberta,DscCanalAtendimento,DthFimOcorrenciaAberta,DscOcorrenciaAberta,DscNumInterrupcao,MdaPreparo,MdaDeslocamento,MdaExecucao,NumVeiculo,CodIBGE FROM DIS_Int_emer_dist_energy_2018
			UNION
			SELECT NomAgente,NumCPFCNPJ,NumOcorrencia,NumConjunto,DthInicioOcorrenciaAberta,DscCanalAtendimento,DthFimOcorrenciaAberta,DscOcorrenciaAberta,DscNumInterrupcao,MdaPreparo,MdaDeslocamento,MdaExecucao,NumVeiculo,CodIBGE FROM DIS_Int_emer_dist_energy_2019
			UNION
			SELECT NomAgente,NumCPFCNPJ,NumOcorrencia,NumConjunto,DthInicioOcorrenciaAberta,DscCanalAtendimento,DthFimOcorrenciaAberta,DscOcorrenciaAberta,DscNumInterrupcao,MdaPreparo,MdaDeslocamento,MdaExecucao,NumVeiculo,CodIBGE FROM DIS_Int_emer_dist_energy_2020
			UNION
			SELECT NomAgente,NumCPFCNPJ,NumOcorrencia,NumConjunto,DthInicioOcorrenciaAberta,DscCanalAtendimento,DthFimOcorrenciaAberta,DscOcorrenciaAberta,DscNumInterrupcao,MdaPreparo,MdaDeslocamento,MdaExecucao,NumVeiculo,CodIBGE FROM DIS_Int_emer_dist_energy_2021)))
		
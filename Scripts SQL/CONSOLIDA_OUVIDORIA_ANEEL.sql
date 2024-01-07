DROP TABLE IF EXISTS DIST_OUVID_ANEEL_CONSOLIDADO;

CREATE TABLE DIST_OUVID_ANEEL_CONSOLIDADO AS
SELECT *
FROM( SELECT SigAgente,NumCPFCNPJAgente,SigUF,CodigoMunicipio,NomMunicipio,NomCategoria,NomSubCategoria,NomTipologia,NomDecisao,DscSituacao,DtCriacao,NumQtdReclamacoesDia  FROM DIST_OUVID_ANEEL_2014
	  UNION
	  SELECT SigAgente,NumCPFCNPJAgente,SigUF,CodigoMunicipio,NomMunicipio,NomCategoria,NomSubCategoria,NomTipologia,NomDecisao,DscSituacao,DtCriacao,NumQtdReclamacoesDia  FROM DIST_OUVID_ANEEL_2015
	  UNION
	  SELECT SigAgente,NumCPFCNPJAgente,SigUF,CodigoMunicipio,NomMunicipio,NomCategoria,NomSubCategoria,NomTipologia,NomDecisao,DscSituacao,DtCriacao,NumQtdReclamacoesDia  FROM DIST_OUVID_ANEEL_2016
	  UNION
	  SELECT SigAgente,NumCPFCNPJAgente,SigUF,CodigoMunicipio,NomMunicipio,NomCategoria,NomSubCategoria,NomTipologia,NomDecisao,DscSituacao,DtCriacao,NumQtdReclamacoesDia  FROM DIST_OUVID_ANEEL_2017
	  UNION
	  SELECT SigAgente,NumCPFCNPJAgente,SigUF,CodigoMunicipio,NomMunicipio,NomCategoria,NomSubCategoria,NomTipologia,NomDecisao,DscSituacao,DtCriacao,NumQtdReclamacoesDia  FROM DIST_OUVID_ANEEL_2018
	  UNION
	  SELECT SigAgente,NumCPFCNPJAgente,SigUF,CodigoMunicipio,NomMunicipio,NomCategoria,NomSubCategoria,NomTipologia,NomDecisao,DscSituacao,DtCriacao,NumQtdReclamacoesDia  FROM DIST_OUVID_ANEEL_2019
	  UNION
	  SELECT SigAgente,NumCPFCNPJAgente,SigUF,CodigoMunicipio,NomMunicipio,NomCategoria,NomSubCategoria,NomTipologia,NomDecisao,DscSituacao,DtCriacao,NumQtdReclamacoesDia  FROM DIST_OUVID_ANEEL_2020
	  UNION
	  SELECT SigAgente,NumCPFCNPJAgente,SigUF,CodigoMunicipio,NomMunicipio,NomCategoria,NomSubCategoria,NomTipologia,NomDecisao,DscSituacao,DtCriacao,NumQtdReclamacoesDia  FROM DIST_OUVID_ANEEL_2021
	  UNION
	  SELECT SigAgente,NumCPFCNPJAgente,SigUF,CodigoMunicipio,NomMunicipio,NomCategoria,NomSubCategoria,NomTipologia,NomDecisao,DscSituacao,DtCriacao,NumQtdReclamacoesDia  FROM DIST_OUVID_ANEEL_2022)
	  
	  

-- ADICIONA A COLUNA DE REGIOES NA TABELA COM OS SIGAGENTES E OS ESTADOS DE ATUACAO

DROP TABLE IF EXISTS ref_regioes_estado;

CREATE TABLE ref_regioes_estado (
			UF TEXT,
			regiao TEXT );
			
INSERT INTO ref_regioes_estado (UF,regiao)
VALUES ('AC','Norte'),
		('AM','Norte'),
		('AP','Norte'),
		('PA','Norte'),
		('RO','Norte'),
		('RR','Norte'),
		('TO','Norte'),
		('AL','Nordeste'),
		('BA','Nordeste'),
		('CE','Nordeste'),
		('MA','Nordeste'),
		('PB','Nordeste'),
		('PE','Nordeste'),
		('PI','Nordeste'),
		('RN','Nordeste'),
		('SE','Nordeste'),
		('DF','Centro-Oeste'),
		('MS','Centro-Oeste'),
		('GO','Centro-Oeste'),
		('MT','Centro-Oeste'),
		('ES','Sudeste'),
		('RJ','Sudeste'),
		('MG','Sudeste'),
		('SP','Sudeste'),
		('PR','Sul'),
		('RS','Sul'),
		('SC','Sul');

		
ALTER TABLE SIGAGENTE_UF
ADD COLUMN RegUF TEXT;

UPDATE SIGAGENTE_UF
SET RegUF = (
	SELECT regiao FROM ref_regioes_estado WHERE ref_regioes_estado.UF = SIGAGENTE_UF.SigUF);
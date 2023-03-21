INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext)
values
(
55555,
'IBGE',
55555,
'+proj=aea +lat_1=-2 +lat_2=-22 +lat_0=-12 +lon_0=-54 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs ',
'PROJCS["Brazil_Albers_Equal_Area_Conic",GEOGCS["SIRGAS 2000",DATUM["Sistema_de_Referencia_Geocentrico_para_las_AmericaS_2000",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6674"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4674"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["False_Easting",0],PARAMETER["False_Northing",0],PARAMETER["longitude_of_center",-54],PARAMETER["Standard_Parallel_1",-2],PARAMETER["Standard_Parallel_2",-22],PARAMETER["latitude_of_center",-12],UNIT["Meter",1],AUTHORITY["IBGE","55555"]]'
);


-- Criando a tabela onde os dados serão dissolvidos
--DROP TABLE CASCADE monitoramento_dissolve;

-- Deletando os registros da tabela
--DELETE FROM monitoramento_dissolve;

CREATE TABLE monitoramento_dissolve AS
SELECT (select unnest(array_agg(m2.object_id)) as id_array2 order by id_array2 limit 1) AS id,
		round((st_area(st_transform(geom, 55555))/10000)::NUMERIC, 2) AS area_ha, t1.class_name, view_date, geom  
FROM (
	SELECT m.class_name,ts.view_date, (st_dump((st_buffer(st_union(m.spatial_data), 0.000000001, 'join=mitre')))).geom AS geom
	FROM monitoramento m 
	INNER JOIN terraamazon.ta_task tt ON tt.scene_id = m.scene_id  
	INNER JOIN terraamazon.ta_project tp ON tp.id = tt.project_id 
	INNER JOIN terraamazon.ta_scene ts ON  ts.id = tt.scene_id 
	WHERE tp.id = 2 AND m.class_name <> 'Erro_T0'
	GROUP BY m.class_name,ts.view_date
) t1
JOIN monitoramento m2 ON ST_Intersects(st_pointonsurface(m2.spatial_data), t1.geom)
GROUP BY t1.class_name, view_date, geom;


ALTER TABLE monitoramento_dissolve ADD PRIMARY KEY (id);

SELECT UpdateGeometrySRID('monitoramento_dissolve','geom',4674);

CREATE INDEX monitoramento_dissolve_geom_idx
  ON monitoramento_dissolve
  USING GIST (geom);

----------------------------------------------------------------------------------------------------------------
DROP TABLE public.log_monitoramento;
 -- Criando a tabela de log no schema terraamazon que registrará as modificações na tabela monitoramento
CREATE TABLE IF NOT EXISTS public.log_monitoramento (
              id                      	SERIAL PRIMARY KEY,
              data_hora_utc           	timestamp WITH time ZONE DEFAULT now(),
              monitoramento_dissolve_id int,
              monitoramento_object_id   int,
              operacao                	varchar(1)
);


CREATE OR REPLACE FUNCTION public.func_log_monitoramento()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    SECURITY DEFINER
AS
$$
DECLARE
    f_tg_op            constant text    := substring(tg_op, 1, 1);
	-- Esta é a lógica de como gravar os dados no log

	--x = inseriu/deletou na tabela
	                                        
	---------------------------------------------------------------------
	-- monitoramento_dissolve_id |  monitoramento_object_id  | operacao |
	---------------------------------------------------------------------
	--           NULL            |             X             |    I     | --> Poligono inserido não dissolvido (totalmente novo).
	---------------------------------------------------------------------
	--            X              |            NULL           |    I     | --> Caso inexistente.
	---------------------------------------------------------------------
	--            X              |             X             |    I     | --> Poligono monitoramento e monitoramento_dissolvido modificado.
	---------------------------------------------------------------------
	--           NULL            |            NULL           |    I     | --> Caso inexistente. Poligono inexistente (nunca foi feito nunca pode ser dissolvido)
	---------------------------------------------------------------------
	---------------------------------------------------------------------
	--           NULL            |             X             |    D     | --> Poligono deletado que não foi dissolvido (criou e apagou antes de dissolver)
	---------------------------------------------------------------------
	--            X              |            NULL           |    D     | --> Caso inexistente. Poligono inexistente (nunca foi feito num pode ser deletado)
	---------------------------------------------------------------------
	--            X              |             X             |    D     | --> Poligono deletado dissolvido (mas a trigger não representa esta caso na tabela log) --> Mudar a trigger do log. Não esta gravando desta maneira quando deletar (ctrl+X) em monitoramento. 
	---------------------------------------------------------------------
	--           NULL            |            NULL           |    D     | --> Caso inexistente, mas esta registrado no log atualmente. Tem um erro aqui no log.
	---------------------------------------------------------------------
	---------------------------------------------------------------------
	--           NULL            |             X             |    U     | --> Poligono que foi classificado e reclassificado antes de dissolver 
	---------------------------------------------------------------------
	--            X              |            NULL           |    U     | --> Caso inexistente 
	---------------------------------------------------------------------
	--            X              |             X             |    U     | --> Poligono que mudou de classe que já dissolvido 
	---------------------------------------------------------------------
	--           NULL            |            NULL           |    U     | --> Não faz update em poligono inexistente
	---------------------------------------------------------------------
    monitoramento_dissolve_id int :=  
    	CASE 
		    WHEN f_tg_op = 'D' THEN
		    	-- dissolve |monitor|
		    	-----------------------------
				--    NULL  |   X   |   D   | --> Poligono deletado que não foi dissolvido (criou e apagou antes de dissolver)
				-----------------------------
				--      X   |   X   |   D   | --> Poligono deletado dissolvido (mas a trigger não representa esta caso na tabela log) --> Mudar a trigger do log. Não esta gravando desta maneira quando deletar (ctrl+X) em monitoramento.
		        -----------------------------
		    
		    	-----------------------------
				--    NULL  |  OLD  |   D   | 
				-----------------------------
				--    X     |  OLD  |   D   | 
		        -----------------------------
				 (SELECT md.id
				 FROM monitoramento_dissolve md
				 WHERE st_intersects(st_pointonsurface(OLD.spatial_data), md.geom)
				 LIMIT 1)
			WHEN f_tg_op = 'I' THEN
		    	-- dissolve |monitor|
				-----------------------------
				--   NULL   |   X   |   I   | --> Poligono inserido não dissolvido (totalmente novo).
				-----------------------------
				--    X     |   X   |   I   | --> Poligono monitoramento e monitoramento_dissolvido modificado.
				-----------------------------
				-----------------------------
				--   NULL   |  NEW  |   I   | 
				-----------------------------
				--    X     |  NEW  |   I   | 
				-----------------------------
				(SELECT md.id
				FROM monitoramento_dissolve md
				WHERE st_intersects(st_pointonsurface(NEW.spatial_data), md.geom)
				LIMIT 1)
			WHEN f_tg_op = 'U' THEN
				-- dissolve|monitor|
				----------------------------
				--  NULL   |   X   |   U   | --> Poligono que foi classificado e reclassificado antes de dissolver 
				----------------------------
				--   X     |   X   |   U   | --> Poligono que mudou de classe que já dissolvido 
				----------------------------
				----------------------------
				--  NULL   |  NEW  |   U   | --> Poligono que foi classificado e reclassificado antes de dissolver 
				----------------------------
				--   X     |  NEW  |   U   | --> Poligono que mudou de classe que já dissolvido 
				----------------------------
				(SELECT md.id
				FROM monitoramento_dissolve md
				WHERE st_intersects(st_pointonsurface(NEW.spatial_data), md.geom)
				LIMIT 1)
		END;
   monitoramento_object_id int := 
   		CASE
	   		WHEN f_tg_op = 'D' THEN
		    	-- dissolve |monitor|
		    	-----------------------------
				--    NULL  |   X   |   D   | --> Poligono deletado que não foi dissolvido (criou e apagou antes de dissolver)
				-----------------------------
				--      X   |   X   |   D   | --> Poligono deletado dissolvido (mas a trigger não representa esta caso na tabela log) --> Mudar a trigger do log. Não esta gravando desta maneira quando deletar (ctrl+X) em monitoramento.
		        -----------------------------
		    
		    	-----------------------------
				--    NULL  |  OLD  |   D   | 
				-----------------------------
				--    X     |  OLD  |   D   | 
		        -----------------------------
				(SELECT OLD.object_id)
	   		WHEN f_tg_op = 'I' THEN
		    	-- dissolve |monitor|
				-----------------------------
				--   NULL   |   X   |   I   | --> Poligono inserido não dissolvido (totalmente novo).
				-----------------------------
				--    X     |   X   |   I   | --> Poligono monitoramento e monitoramento_dissolvido modificado.
				-----------------------------
				-----------------------------
				--   NULL   |  NEW  |   I   | 
				-----------------------------
				--    X     |  NEW  |   I   | 
				-----------------------------
				(SELECT NEW.object_id)
	   		WHEN f_tg_op = 'U' THEN
				-- dissolve|monitor|
				----------------------------
				--  NULL   |   X   |   U   | --> Poligono que foi classificado e reclassificado antes de dissolver 
				----------------------------
				--   X     |   X   |   U   | --> Poligono que mudou de classe que já dissolvido 
				----------------------------
				----------------------------
				--  NULL   |  NEW  |   U   | --> Poligono que foi classificado e reclassificado antes de dissolver 
				----------------------------
				--   X     |  NEW  |   U   | --> Poligono que mudou de classe que já dissolvido 
				----------------------------
				(SELECT NEW.object_id)
		END;

BEGIN

    EXECUTE format(
            'INSERT INTO log_monitoramento (operacao, monitoramento_dissolve_id, monitoramento_object_id)
            VALUES (%L, %L, %L);',
            f_tg_op,
           	monitoramento_dissolve_id,
	        monitoramento_object_id);
    RETURN NULL;
END;
$$;

CREATE TRIGGER tg_func_log_monitoramento
AFTER INSERT OR UPDATE OR DELETE 
ON monitoramento
FOR EACH ROW EXECUTE PROCEDURE public.func_log_monitoramento();

-----------------------------------------------------------------------------------------------------------------------
--Função para realização do dissolve à partir do log_monitoramento
CREATE OR REPLACE FUNCTION processamento_dado_diario()
RETURNS void AS $$
DECLARE

	str text;

BEGIN
	RAISE NOTICE '%', 'Testando a função';

	str :=	concat(
			'
			-- Removendo poligonos deletados 
			DELETE FROM monitoramento_dissolve md 
			WHERE md.id IN (
				SELECT DISTINCT monitoramento_dissolve_id 
				FROM log_monitoramento lm 
				WHERE lm.data_hora_utc::date = current_date 
				AND operacao = ''D'' 
				AND monitoramento_object_id IS NULL
			);
			
			-- Removendo as chaves primarias que não são apagadas no delete anterior e que quebram o código do insert
			DELETE FROM monitoramento_dissolve md 
			WHERE md.id IN (
				SELECT DISTINCT lm.monitoramento_object_id 
				FROM log_monitoramento lm 
				JOIN monitoramento m ON m.object_id = lm.monitoramento_object_id
				WHERE lm.data_hora_utc::date = current_date
				AND operacao = ''I'' 
				AND monitoramento_dissolve_id IS NULL --Poligono não dissolvido (novo), que não esta em dissolve
				AND m.class_name != ''Erro_T0''
			);
				
			-- Removendo as chaves primarias dos poligonos que foram dissolvidos
			DELETE FROM monitoramento_dissolve md 
			WHERE md.id IN (
				SELECT DISTINCT lm.monitoramento_object_id 
				FROM log_monitoramento lm 
				JOIN monitoramento m ON m.object_id = lm.monitoramento_object_id
				WHERE lm.data_hora_utc::date = current_date
				AND operacao = ''I'' 
				AND monitoramento_dissolve_id IS NOT NULL -- Poligono velho que já foi dissolvido
				AND m.class_name != ''Erro_T0''
			);
			-- Dissolvendo os dados e inserindo na tabela monitoramento_dissolve
			INSERT INTO monitoramento_dissolve (id, class_name, view_date, area_ha, geom)
			SELECT (select unnest(array_agg(m2.object_id)) as id_array2 order by id_array2 limit 1) AS id, 
				t1.class_name, view_date, 	round((st_area(st_transform(geom, 55555))/10000)::NUMERIC, 2) AS area_ha, geom
			FROM (
				SELECT class_name, view_date, (st_dump((st_buffer(st_union(spatial_data), 0.000000001, ''join=mitre'')))).geom AS geom
				FROM (
					SELECT m.class_name, ts.view_date, m.spatial_data 
					FROM log_monitoramento lm 
					JOIN monitoramento m ON m.object_id = lm.monitoramento_object_id
					JOIN terraamazon.ta_scene ts ON ts.id = m.scene_id
					WHERE lm.data_hora_utc::date = current_date
						AND operacao = ''I'' 
						AND monitoramento_dissolve_id IS NOT NULL --Poligono não dissolvido (novo), dividido pela celula ou tile que não esta em dissolve
						AND m.class_name != ''Erro_T0''
					UNION 
					SELECT m.class_name, ts.view_date, m.spatial_data
					FROM log_monitoramento lm 
					JOIN monitoramento m ON m.object_id = lm.monitoramento_object_id
					JOIN terraamazon.ta_scene ts ON ts.id = m.scene_id
					WHERE lm.data_hora_utc::date = current_date 
						AND operacao = ''I'' 
						AND monitoramento_dissolve_id IS NULL --Poligono não dissolvido (novo), que não esta em dissolve
						AND m.class_name != ''Erro_T0''
				) t
				GROUP BY class_name, view_date
			) t1
			JOIN monitoramento m2 ON ST_Intersects(st_pointonsurface(m2.spatial_data), t1.geom)
			GROUP BY t1.class_name, t1.view_date, t1.geom;	
			'
			);
	EXECUTE str;
END;
$$ LANGUAGE plpgsql;

--=============================================================================================================
-- Executando a função que processa os poligonos
SELECT processamento_dado_diario();
--=============================================================================================================



--=============================================================================================================
--                                                  DESENVOLVIMENTO
--=============================================================================================================
-- Esta é a lógica de como gravar os dados no log

--x = inseriu/deletou na tabela
                                        
---------------------------------------------------------------------
-- monitoramento_dissolve_id |  monitoramento_object_id  | operacao |
---------------------------------------------------------------------
--           NULL            |             X             |    I     | --> Poligono inserido não dissolvido (totalmente novo).
---------------------------------------------------------------------
--            X              |            NULL           |    I     | --> Caso inexistente.
---------------------------------------------------------------------
--            X              |             X             |    I     | --> Poligono monitoramento e monitoramento_dissolvido modificado.
---------------------------------------------------------------------
--           NULL            |            NULL           |    I     | --> Caso inexistente. Poligono inexistente (nunca foi feito nunca pode ser dissolvido)
---------------------------------------------------------------------
---------------------------------------------------------------------
--           NULL            |             X             |    D     | --> Poligono deletado que não foi dissolvido (criou e apagou antes de dissolver)
---------------------------------------------------------------------
--            X              |            NULL           |    D     | --> Caso inexistente. Poligono inexistente (nunca foi feito num pode ser deletado)
---------------------------------------------------------------------
--            X              |             X             |    D     | --> Poligono deletado dissolvido 
---------------------------------------------------------------------
--           NULL            |            NULL           |    D     | --> Caso inexistente, mas esta registrado no log atualmente. Tem um erro aqui no log.
---------------------------------------------------------------------
---------------------------------------------------------------------
--           NULL            |             X             |    U     | --> Poligono que foi classificado e reclassificado antes de dissolver 
---------------------------------------------------------------------
--            X              |            NULL           |    U     | --> Caso inexistente 
---------------------------------------------------------------------
--            X              |             X             |    U     | --> Poligono que mudou de classe que já dissolvido 
---------------------------------------------------------------------
--           NULL            |            NULL           |    U     | --> Não faz update em poligono inexistente
---------------------------------------------------------------------


--=============================================================================================================
--                                              DESENVOLVIMENTO
--=============================================================================================================
--                                          O QUE FAZER PARA CADA CASO
--=============================================================================================================
--                                                  DELETE
--=============================================================================================================

-- O que fazer Para os casos do da operação delete
-- dissolve |monitor|
-----------------------------
--    NULL  |   X   |   D   | --> Poligono deletado que não foi dissolvido (criou e apagou antes de dissolver) ==> Não fazer nada
-----------------------------
--      X   |   X   |   D   | --> Poligono deletado dissolvido ==> Deletar o poligono em monitoramento_dissolve
-----------------------------
-- Deletando poligono que foi deletado e estava em monitoramento_dissolve
--DELETE FROM monitoramento_dissolve md 
SELECT md.id FROM monitoramento_dissolve md 
WHERE md.id IN (
	SELECT DISTINCT monitoramento_dissolve_id 
	FROM log_monitoramento lm 
	WHERE lm.data_hora_utc::date = current_date 
	AND operacao = 'D' 
	AND monitoramento_dissolve_id IS NOT NULL
);

--=============================================================================================================
--                                                  INSERT
--=============================================================================================================

-- O que fazer para os casos da operação insert
-- dissolve |monitor|
-----------------------------
--   NULL   |   X   |   I   | --> Poligono inserido não dissolvido (totalmente novo). 1º Caso
-----------------------------
--    X     |   X   |   I   | --> Poligono monitoramento e monitoramento_dissolvido modificado.
-----------------------------

-- 1º Caso (primeiro tratamento)
-- Poligono não dissolvido e que não tem divisão de celulas ou tiles (não toca nenhum poligono com a mesma data)
WITH a AS (
	SELECT m.object_id, m.class_name, m.spatial_data AS geom, m.scene_id 
	FROM log_monitoramento lm 
	JOIN monitoramento m ON lm.monitoramento_object_id = m.object_id
	WHERE lm.data_hora_utc::date = current_date 
	AND operacao = 'I' 
	AND monitoramento_dissolve_id IS NULL
)
SELECT m2.object_id, m2.class_name, ts.view_date, m2.spatial_data AS geom 
FROM (
	SELECT object_id, class_name, geom, scene_id FROM a 
	WHERE a.object_id NOT IN (
		SELECT DISTINCT object_id
		FROM (
			SELECT  
				a_object_id AS object_id, b_object_id, 
				a_scene_id, b_scene_id, 
				ts.view_date AS a_view_date, ts1.view_date AS b_view_date
			FROM (
				SELECT a.object_id AS a_object_id, b.object_id AS b_object_id, a.scene_id AS a_scene_id, b.scene_id AS b_scene_id
				FROM a, a b
				WHERE a.object_id < b.object_id
				AND ST_INTERSECTS(a.geom, b.geom)
			) i
			JOIN terraamazon.ta_scene ts ON i.a_scene_id = ts.id
			JOIN terraamazon.ta_scene ts1 ON i.b_scene_id = ts1.id
			-- Somente para as mesmas datas
			WHERE ts.view_date = ts1.view_date 
		) i1
		UNION
		SELECT DISTINCT object_id
		FROM (
			SELECT  
				a_object_id, b_object_id AS object_id,
				a_scene_id, b_scene_id, 
				ts.view_date AS a_view_date, ts1.view_date AS b_view_date
			FROM (
				SELECT a.object_id AS a_object_id, b.object_id AS b_object_id, a.scene_id AS a_scene_id, b.scene_id AS b_scene_id
				FROM a, a b
				WHERE a.object_id < b.object_id
				AND ST_INTERSECTS(a.geom, b.geom)
			) i
			JOIN terraamazon.ta_scene ts ON i.a_scene_id = ts.id
			JOIN terraamazon.ta_scene ts1 ON i.b_scene_id = ts1.id
			-- Somente para as mesmas datas
			WHERE ts.view_date = ts1.view_date 
		) i1
	)
) t
JOIN monitoramento m2 ON t.object_id = m2.object_id
JOIN terraamazon.ta_scene ts ON m2.scene_id = ts.id;


-- 1º Caso (segundo tratamento)
--Versão que verifica se AS datas são iguais
-- Poligono não dissolvido e que tem divisão de celulas ou tiles (tocam poligonos com a mesma data)
WITH a AS (
	SELECT m.object_id, m.class_name, m.spatial_data AS geom, m.scene_id 
	FROM log_monitoramento lm 
	JOIN monitoramento m ON lm.monitoramento_object_id = m.object_id
	WHERE lm.data_hora_utc::date = current_date 
	AND operacao = 'I' 
	AND monitoramento_dissolve_id IS NULL
)
SELECT (select unnest(array_agg(m3.object_id)) as id_array2 order by id_array2 limit 1) AS id, 
	t2.class_name, view_date, 	round((st_area(st_transform(geom, 55555))/10000)::NUMERIC, 2) AS area_ha, geom
FROM (
	SELECT t1.class_name, t1.view_date, (st_dump((st_buffer(st_union(t1.geom), 0.00000000001, 'join=mitre')))).geom AS geom
	FROM (
		SELECT m.object_id, m.class_name, view_date, m.spatial_data AS geom 
		FROM (
			SELECT DISTINCT object_id
			FROM (
				SELECT  
					a_object_id AS object_id, b_object_id, 
					a_scene_id, b_scene_id, 
					ts.view_date AS a_view_date, ts1.view_date AS b_view_date
				FROM (
					SELECT a.object_id AS a_object_id, b.object_id AS b_object_id, a.scene_id AS a_scene_id, b.scene_id AS b_scene_id
					FROM a, a b
					WHERE a.object_id < b.object_id
					AND ST_INTERSECTS(a.geom, b.geom)
				) i
				JOIN terraamazon.ta_scene ts ON i.a_scene_id = ts.id
				JOIN terraamazon.ta_scene ts1 ON i.b_scene_id = ts1.id
				-- Somente para as mesmas datas
				WHERE ts.view_date = ts1.view_date 
			) i1
			UNION
			SELECT DISTINCT object_id
			FROM (
				SELECT  
					a_object_id, b_object_id AS object_id,
					a_scene_id, b_scene_id, 
					ts.view_date AS a_view_date, ts1.view_date AS b_view_date
				FROM (
					SELECT a.object_id AS a_object_id, b.object_id AS b_object_id, a.scene_id AS a_scene_id, b.scene_id AS b_scene_id
					FROM a, a b
					WHERE a.object_id < b.object_id
					AND ST_INTERSECTS(a.geom, b.geom)
				) i
				JOIN terraamazon.ta_scene ts ON i.a_scene_id = ts.id
				JOIN terraamazon.ta_scene ts1 ON i.b_scene_id = ts1.id
				-- Somente para as mesmas datas
				WHERE ts.view_date = ts1.view_date 
			) i1
		) t
		JOIN monitoramento m ON t.object_id = m.object_id
		JOIN terraamazon.ta_scene ts ON m.scene_id = ts.id
	) t1
	GROUP BY t1.class_name, t1.view_date
) t2
JOIN monitoramento m3 ON ST_Intersects(st_pointonsurface(m3.spatial_data), t2.geom)
GROUP BY t2.class_name, t2.view_date, t2.geom;


--=============================================================================================================
--                                                  UPDATE
--=============================================================================================================
-- dissolve|monitor|
----------------------------
--  NULL   |   X   |   U   | --> Poligono que foi classificado e reclassificado antes de dissolver 
----------------------------
--   X     |   X   |   U   | --> Poligono que mudou de classe que já dissolvido
----------------------------

UPDATE monitoramento_dissolve
SET class_name = 'Desmatamento'
WHERE object_id IN ( 
19439, 46686);

UPDATE monitoramento_dissolve
SET class_name = 'Desmatamento'
WHERE id IN (
	SELECT md.id FROM monitoramento_dissolve md 
	WHERE md.id IN (
		SELECT DISTINCT monitoramento_dissolve_id 
		FROM log_monitoramento lm 
		WHERE lm.data_hora_utc::date = current_date 
		AND operacao = 'U' 
		AND monitoramento_dissolve_id IS NOT NULL
	)
);


--=============================================================================================================
--                                                  Qgis
--=============================================================================================================
-- Consulta para verificar se os poligonos foram processados corretamente
-- Poligonos que foram finalizados no dia 15
SELECT object_id AS id, m.scene_id, spatial_data AS geom FROM monitoramento m
JOIN terraamazon.ta_tasklog tt ON tt.task_id = m.task_id
WHERE tt.final_time BETWEEN to_date('2023-03-14', 'YYYY-MM-DD') AND to_date('2023-03-15', 'YYYY-MM-DD') AND tt.status = 'CLOSED' AND m.class_name <> 'Erro_T0'
GROUP BY spatial_data, object_id, scene_id;

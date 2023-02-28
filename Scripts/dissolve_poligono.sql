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
DROP TABLE monitoramento_dissolve;

CREATE TABLE monitoramento_dissolve1 AS
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
-- Criando a tabela de log no schema terraamazon que registrará as modificações na tabela monitoramento
CREATE TABLE IF NOT EXISTS public.log_monitoramento (
              id                      	int PRIMARY KEY,
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

    monitoramento_dissolve_id   int     := CASE WHEN f_tg_op = 'D' THEN 	
    												 (SELECT md.id
													 FROM monitoramento_dissolve md
													 WHERE st_intersects(st_pointonsurface(OLD.spatial_data), md.geom)
													 LIMIT 1)
										   WHEN f_tg_op = 'I' THEN
													(SELECT md.id
													FROM monitoramento_dissolve md
													WHERE st_intersects(st_pointonsurface(NEW.spatial_data), md.geom)
													LIMIT 1)
        								   END;
   monitoramento_object_id   int     := CASE WHEN f_tg_op = 'I' THEN 	
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
			-- Este script deleta todos os poligonos (tanto os que precisaram dissolver quanto os que tem um único registro)
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

---------------------------------------------------------------------
-- monitoramento_dissolve_id |  monitoramento_object_id  | operacao |
----------------------------------------------------------
--           NULL            |             x             |    I     | --> Poligono novo não dissolvido
---------------------------------------------------------------------
--           x               |            NULL           |    D     | --> Delete de poligono que foi dissolvido (não cortado por celulas ou tiles)
----------------------------------------------------------
--            x              |             x             |    |     | --> Tratamento de poligonos que foram cortados por celulas ou tiles
----------------------------------------------------------



--=============================================================================================================
--                                                  DESENVOLVIMENTO
--=============================================================================================================
--                                                  DELETE
--=============================================================================================================

-- Tem de ser a primeira ação e deve ser seguida do Caso 1 para recolocar os poligonos que 
-- sobraram quando apagou o poligono que precisou dissolver
-- Removendo poligonos deletados 
-- Este script deleta todos os poligonos (tanto os que precisaram dissolver quanto os que tem um único registro)
--DELETE FROM monitoramento_dissolve md 
SELECT FROM monitoramento_dissolve md 
WHERE md.id IN (
	SELECT DISTINCT monitoramento_dissolve_id 
	FROM log_monitoramento lm 
	WHERE lm.data_hora_utc::date = current_date 
	AND operacao = 'D' 
	AND monitoramento_object_id IS NULL
);
 

-- Removendo as chaves primarias que quebram o codigo de insert
--DELETE FROM monitoramento_dissolve md
SELECT FROM monitoramento_dissolve md 
WHERE md.id IN (
	SELECT DISTINCT lm.monitoramento_object_id 
	FROM log_monitoramento lm 
	JOIN monitoramento m ON m.object_id = lm.monitoramento_object_id
	WHERE lm.data_hora_utc::date = current_date
	AND operacao = 'I' 
	AND monitoramento_dissolve_id IS NULL --Poligono não dissolvido (novo), que não esta em dissolve
	AND m.class_name != 'Erro_T0'
);


--=============================================================================================================
--                                            DISSOLVE
--=============================================================================================================

-- Fazendo o dissolve dos poligonos que foram modificados no dia
--INSERT INTO monitoramento_dissolve (id, class_name, view_date, area_ha, geom)
SELECT (select unnest(array_agg(m2.object_id)) as id_array2 order by id_array2 limit 1) AS id, 
	t1.class_name, view_date, 	round((st_area(st_transform(geom, 55555))/10000)::NUMERIC, 2) AS area_ha, geom
FROM (
	SELECT class_name, view_date, (st_dump((st_buffer(st_union(spatial_data), 0.0000001, 'join=mitre')))).geom AS geom
	FROM (
		SELECT m.class_name, ts.view_date, m.spatial_data 
		FROM log_monitoramento lm 
		JOIN monitoramento m ON m.object_id = lm.monitoramento_object_id
		JOIN terraamazon.ta_scene ts ON ts.id = m.scene_id
		WHERE lm.data_hora_utc::date = current_date
			AND operacao = 'I' 
			AND monitoramento_dissolve_id IS NOT NULL --Poligono não dissolvido (novo), dividido pela celula ou tile que não esta em dissolve
			AND m.class_name != 'Erro_T0'
		UNION 
		SELECT m.class_name, ts.view_date, m.spatial_data
		FROM log_monitoramento lm 
		JOIN monitoramento m ON m.object_id = lm.monitoramento_object_id
		JOIN terraamazon.ta_scene ts ON ts.id = m.scene_id
		WHERE lm.data_hora_utc::date = current_date 
			AND operacao = 'I' 
			AND monitoramento_dissolve_id IS NULL --Poligono não dissolvido (novo), que não esta em dissolve
			AND m.class_name != 'Erro_T0'
	) t
	GROUP BY class_name, view_date
) t1
JOIN monitoramento m2 ON ST_Intersects(st_pointonsurface(m2.spatial_data), t1.geom)
GROUP BY t1.class_name, t1.view_date, t1.geom;
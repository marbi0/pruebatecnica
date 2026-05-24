MERGE `project-ee2f21b1-cde8-4ee4-b57.INTEGRATION.integration_prueba_tecnica` AS target
USING (
  SELECT 
    id,
    userId,
    title,
    body,
    fecha_extraccion,
    CURRENT_TIMESTAMP() AS fecha_transformacion
  FROM `project-ee2f21b1-cde8-4ee4-b57.SANDBOX_PRUEBA_ETL.api_posts_raw`

  QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY fecha_extraccion DESC) = 1
) AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET 
    target.userId = source.userId,
    target.title = source.title,
    target.body = source.body,
    target.fecha_extraccion = source.fecha_extraccion
WHEN NOT MATCHED THEN
  INSERT (id, userId, title, body, fecha_extraccion, fecha_transformacion)
  VALUES (source.id, source.userId, source.title, source.body, source.fecha_extraccion, source.fecha_transformacion);
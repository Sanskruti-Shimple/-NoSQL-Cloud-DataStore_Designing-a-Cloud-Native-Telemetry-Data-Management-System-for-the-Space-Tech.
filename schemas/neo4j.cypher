CREATE CONSTRAINT satellite_id IF NOT EXISTS
FOR (n:Satellite) REQUIRE n.id IS UNIQUE;

CREATE CONSTRAINT module_id IF NOT EXISTS
FOR (n:Module) REQUIRE (n.satellite_id, n.id) IS UNIQUE;

CREATE CONSTRAINT sensor_id IF NOT EXISTS
FOR (n:Sensor) REQUIRE (n.satellite_id, n.id) IS UNIQUE;

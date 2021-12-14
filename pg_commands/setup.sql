DROP TABLE IF EXISTS water_data;
DROP TABLE IF EXISTS weather_recordings;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE water_data (
  id            uuid DEFAULT uuid_generate_v4 () PRIMARY KEY,
  station_code  VARCHAR NOT NULL,
  station_name  VARCHAR NOT NULL,
  discharge     DECIMAL NOT NULL,
  recorded_at   TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE weather_recordings (
  id            uuid DEFAULT uuid_generate_v4 () PRIMARY KEY,
  weather       JSONB
);

CREATE TABLE test_table (
  LogKey INT IDENTITY(1,1) PRIMARY KEY,
  LogDate DATETIME DEFAULT GETDATE(),
  detailedforecast VARCHAR(1000),
  endtime VARCHAR(1000),
  icon VARCHAR(1000),
  isdaytime BIT,
  name VARCHAR(1000),
  number INT,
  probabilityofprecipitation_unitcode VARCHAR(1000),
  probabilityofprecipitation_value INT,
  shortforecast VARCHAR(1000),
  starttime VARCHAR(1000),
  temperature INT,
  temperaturetrend VARCHAR(1000),
  temperatureunit VARCHAR(1000),
  winddirection VARCHAR(1000),
  windspeed VARCHAR(1000)
);
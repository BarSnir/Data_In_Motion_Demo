CREATE TABLE orders_extract (
  `OrderId` STRING,
  `SiteToken` STRING,
  `Price` INT,
  `StatusId` INT,
  `CustomerId` INT,
  PRIMARY KEY(`OrderId`) NOT ENFORCED
) WITH (
  'changelog.mode'='upsert',
  'kafka.cleanup-policy'='compact',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);

INSERT INTO orders_extract (
  `OrderId`,
  `SiteToken`,
  `Price`,
  `StatusId`,
  `CustomerId`
)
SELECT 
  Orders.after.OrderId,
  Orders.after.SiteToken,
  Orders.after.Price,
  Orders.after.StatusId,
  Orders.after.CustomerId 
FROM Orders;


CREATE TABLE vehicles_extract (
    `VehicleId` STRING,
    `KM` INT,
    `PrevOwnerNumber` INT,
    `OrderId` STRING,
    `MarketInfoId` STRING,
    `MediaTypeId` INT,
    `YearOnRoad` INT,
    `TestDate` INT,
    `ImproveId` INT,
    PRIMARY KEY (`VehicleId`) NOT ENFORCED
) WITH (
  'changelog.mode'='upsert',
  'kafka.cleanup-policy'='compact',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);

INSERT INTO vehicles_extract (
    `VehicleId`,
    `KM`,
    `PrevOwnerNumber`,
    `OrderId`,
    `MarketInfoId`,
    `MediaTypeId`,
    `YearOnRoad`,
    `TestDate` ,
    `ImproveId`
)
SELECT 
  Vehicles.after.VehicleId,
  Vehicles.after.KM,
  Vehicles.after.PrevOwnerNumber,
  Vehicles.after.OrderId,
  Vehicles.after.MarketInfoId,
  Vehicles.after.MediaTypeId,
  Vehicles.after.YearOnRoad,
  Vehicles.after.TestDate,
  Vehicles.after.ImproveId
FROM Vehicles;


CREATE TABLE market_info_extract (
    `MarketInfoId` STRING,
    `AirBags` INT,
    `SunRoof` INT,
    `MagnesiumWheels` INT,
    `ReversSensors` INT,
    `ABS` INT,
    `Hybrid` INT,
    `Doors` INT,
    `EnvironmentFriendlyLevel` INT,
    `SecurityTestLevel` INT,
    `ManufacturerId` INT,
    `ManufacturerText` STRING,
    `ModelId` INT,
    `ModelText` STRING,
    `SubModelId` INT,
    `SubModelText` STRING,
    `Year` INT,
    `HorsePower` INT,
    `CruseControl` INT,
    `PowerWheel` INT,
    `FullyAutonomic` INT,
    `MarketPrice` INT,
    PRIMARY KEY (MarketInfoId) NOT ENFORCED
) WITH (
  'changelog.mode'='upsert',
  'kafka.cleanup-policy'='compact',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);


INSERT INTO market_info_extract (
    `MarketInfoId`,
    `AirBags`,
    `SunRoof`,
    `MagnesiumWheels`,
    `ReversSensors`,
    `ABS`,
    `Hybrid`,
    `Doors`,
    `EnvironmentFriendlyLevel`,
    `SecurityTestLevel`,
    `ManufacturerId`,
    `ManufacturerText`,
    `ModelId`,
    `ModelText`,
    `SubModelId`,
    `SubModelText`,
    `Year`,
    `HorsePower`,
    `CruseControl`,
    `PowerWheel`,
    `FullyAutonomic`,
    `MarketPrice`
)
SELECT 
  MarketInfo.after.MarketInfoId,
  MarketInfo.after.AirBags,
  MarketInfo.after.SunRoof,
  MarketInfo.after.MagnesiumWheels,
  MarketInfo.after.ReversSensors,
  MarketInfo.after.`ABS`,
  MarketInfo.after.Hybrid,
  MarketInfo.after.Doors,
  MarketInfo.after.EnvironmentFriendlyLevel,
  MarketInfo.after.SecurityTestLevel,
  MarketInfo.after.ManufacturerId,
  MarketInfo.after.ManufacturerText,
  MarketInfo.after.ModelId,
  MarketInfo.after.ModelText,
  MarketInfo.after.SubModelId,
  MarketInfo.after.SubModelText,
  MarketInfo.after.`Year`,
  MarketInfo.after.HorsePower,
  MarketInfo.after.CruseControl,
  MarketInfo.after.PowerWheel,
  MarketInfo.after.FullyAutonomic,
  MarketInfo.after.MarketPrice
FROM MarketInfo;

-- Multi way joins none primary key



CREATE TABLE full_fields (
    `OrderId` STRING,
    `SiteToken` STRING,
    `Price` INT,
    `StatusId` INT,
    `CustomerId` INT,
    `VehicleId` STRING,
    `KM` INT,
    `PrevOwnerNumber` INT,
    `MediaTypeId` INT NULL,
    `YearOnRoad` INT,
    `TestDate` INT,
    `ImproveId` INT,
    `MarketInfoId` STRING,
    `AirBags` INT,
    `SunRoof` INT,
    `MagnesiumWheels` INT,
    `ReversSensors` INT,
    `ABS` INT,
    `Hybrid` INT,
    `Doors` INT,
    `EnvironmentFriendlyLevel` INT,
    `SecurityTestLevel` INT,
    `ManufacturerId` INT,
    `ManufacturerText` STRING,
    `ModelId` INT,
    `ModelText` STRING,
    `SubModelId` INT,
    `SubModelText` STRING,
    `Year` INT,
    `HorsePower` INT,
    `CruseControl` INT,
    `PowerWheel` INT,
    `FullyAutonomic` INT,
    `MarketPrice` INT,
    PRIMARY KEY (OrderId) NOT ENFORCED
) WITH (
  'changelog.mode'='upsert',
  'kafka.cleanup-policy'='compact',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);

-- Usecase 1 - Full enrich with primary & foreign keys
INSERT INTO full_fields (
    `OrderId`,
    `SiteToken`,
    `Price`,
    `StatusId`,
    `CustomerId`,
    `VehicleId`,
    `KM`,
    `PrevOwnerNumber`,
    `MediaTypeId`,
    `YearOnRoad`,
    `TestDate`,
    `ImproveId`,
    `MarketInfoId`,
    `AirBags`,
    `SunRoof`,
    `MagnesiumWheels`,
    `ReversSensors`,
    `ABS`,
    `Hybrid`,
    `Doors`,
    `EnvironmentFriendlyLevel`,
    `SecurityTestLevel`,
    `ManufacturerId`,
    `ManufacturerText`,
    `ModelId`,
    `ModelText`,
    `SubModelId`,
    `SubModelText`,
    `Year`,
    `HorsePower`,
    `CruseControl`,
    `PowerWheel`,
    `FullyAutonomic`,
    `MarketPrice`
)
SELECT 
    orders_extract.`OrderId` AS OrderId,
    orders_extract.`SiteToken` AS SiteToken,
    orders_extract.`Price` AS Price,
    orders_extract.`StatusId` AS StatusId,
    orders_extract.`CustomerId` AS CustomerId,
    vehicles_extract.`VehicleId` AS VehicleId,
    vehicles_extract.`KM` AS KM,
    vehicles_extract.`PrevOwnerNumber` AS PrevOwnerNumber,
    vehicles_extract.`MediaTypeId` AS MediaTypeId,
    vehicles_extract.`YearOnRoad` AS YearOnRoad,
    vehicles_extract. `TestDate` AS TestDate,
    vehicles_extract.`ImproveId` AS ImproveId,
    market_info_extract.`MarketInfoId` AS MarketInfoId,
    market_info_extract.`AirBags` AS AirBags,
    market_info_extract.`SunRoof` AS SunRoof,
    market_info_extract.`MagnesiumWheels` AS MagnesiumWheels,
    market_info_extract. `ReversSensors` AS ReversSensors,
    market_info_extract.`ABS` AS `ABS`,
    market_info_extract.`Hybrid` AS Hybrid,
    market_info_extract.`Doors` ASDoors ,
    market_info_extract.`EnvironmentFriendlyLevel` AS EnvironmentFriendlyLevel,
    market_info_extract.`SecurityTestLevel` AS SecurityTestLevel,
    market_info_extract.`ManufacturerId` AS ManufacturerId,
    market_info_extract.`ManufacturerText` AS ManufacturerText,
    market_info_extract.`ModelId` AS ModelId,
    market_info_extract.`ModelText` AS ModelText,
    market_info_extract.`SubModelId` AS SubModelId,
    market_info_extract.`SubModelText` AS SubModelText,
    market_info_extract.`Year` AS `Year`,
    market_info_extract.`HorsePower` AS HorsePower,
    market_info_extract.`CruseControl` AS CruseControl,
    market_info_extract.`PowerWheel` AS PowerWheel,
    market_info_extract.`FullyAutonomic` AS FullyAutonomic,
    market_info_extract.`MarketPrice` AS MarketPrice
FROM 
orders_extract 
INNER JOIN vehicles_extract ON 
    orders_extract.OrderId = vehicles_extract.OrderId
INNER JOIN market_info_extract ON 
    vehicles_extract.MarketInfoId = market_info_extract.MarketInfoId;

CREATE TABLE price_change_log (
  `OrderId` String,
  `CurrentPrice` INT,
  `PrevPrice` INT,
  PRIMARY KEY (`OrderId`) NOT ENFORCED
) WITH (
  'changelog.mode'='append',
  'kafka.cleanup-policy'='delete',
  'value.format' = 'avro-registry',
  'scan.startup.mode' = 'earliest-offset'
);


INSERT INTO price_change_log (
  `OrderId`,
  `CurrentPrice`,
  `PrevPrice`
)
SELECT 
  Orders.after.OrderId,
  Orders.after.Price AS CurrentPrice,
  Orders.before.Price AS PrevPrice
FROM Orders
WHERE Orders.op = 'u';


SELECT 
 OrderID,
 FIRST_VALUE(PrevPrice) AS FirstPrice,
 LAST_VALUE(PrevPrice) AS PrevPrice,
 LAST_VALUE(CurrentPrice) AS CurrentPrice
FROM price_change_log
GROUP BY OrderID;
 
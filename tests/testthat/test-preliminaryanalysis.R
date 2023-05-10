library(testthat)

pathToDriver <- './Drivers'
dbms <- "sqlite"
schema <- "main"
pathToResults <<- dirname(dirname(getwd())) #
data <- readr::read_csv(paste(pathToResults,"/TestSchemaTrajectories.csv", sep =""))

test_that("Quering all trajectories' statistics table", {
  connection <- createConnectionSQLite()
  createTrajectoriesTable(conn = connection, dbms = dbms, data = data, schema = schema)
  pathToFile <- "/inputUI.csv"
  result <- getDistinctTrajectoriesTable(connection = connection, dbms = dbms, schema = schema)
  DatabaseConnector::disconnect(connection)
  expect_equal(result[5,]$TRAJECTORY, "State1->>State4->>State5")
})
#> Test passed 🥇

test_that("Quering all trajectories' statistics table with no settings", {
  connection <- createConnectionSQLite()
  createTrajectoriesTable(conn = connection, dbms = dbms, data = data, schema = schema)
  pathToFile <- "/inputUI.csv"
  dataTable <- getDistinctTrajectoriesTable(connection = connection, dbms = dbms, schema = schema)
  result <- outputTrajectoryStatisticsTables(dataTable = dataTable)
  DatabaseConnector::disconnect(connection)
  expect_equal(result$notMatching[3,]$PERC, "10%")
})
#> Test passed 🥇

test_that("Quering all trajectories' statistics table with settings", {
  connection <- createConnectionSQLite()
  createTrajectoriesTable(conn = connection, dbms = dbms, data = data, schema = schema)
  pathToFile = "/inputUI.csv"
  trajSettings = loadUITrajectories((paste(pathToResults,pathToFile, sep ="")))
  dataTable <- getDistinctTrajectoriesTable(connection = connection, dbms = dbms, schema = schema)
  result <- outputTrajectoryStatisticsTables(dataTable = dataTable, settings = trajSettings)
  DatabaseConnector::disconnect(connection)
  expect_equal(result$matching[1,]$TOTAL, 2000)
})
#> Test passed 🥇

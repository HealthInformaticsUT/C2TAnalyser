library(testthat)

pathToDriver <- './Drivers'
dbms <- "sqlite"
schema <- "main"
pathToResults <<- dirname(dirname(getwd())) #
data <- readr::read_csv(paste(pathToResults,"/TestSchemaTrajectories.csv", sep =""))


test_that("Quering all patient data with specified settings", {
  connection <- createConnectionSQLite()
  createTrajectoriesTable(conn = connection, dbms = dbms, data = data, schema = schema)
  pathToFile = "/inputUI.csv"
  trajSettings = loadUITrajectories((paste(pathToResults,pathToFile, sep ="")))
  dataTable <- getDistinctTrajectoriesTable(connection = connection, dbms = dbms, schema = schema)
  result <- outputTrajectoryStatisticsTables(dataTable = dataTable, settings = trajSettings)
  returnedData <- importTrajectoryData(connection, dbms, schema, result$matching, settings = trajSettings)
  DatabaseConnector::disconnect(connection)
  expect_equal(nrow(returnedData), 19000)
})
#> Test passed 🥇

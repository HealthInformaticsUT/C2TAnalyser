library(testthat)

pathToDriver <- './Drivers'
dbms <- "sqlite"
schema <- "main"
pathToResults <<- dirname(dirname(getwd())) #
data = readr::read_csv(paste(pathToResults,"/TestSchemaTrajectories.csv", sep =""))

test_that("Connect", {
  connection <- createConnectionSQLite()
  expect_s4_class(connection, "DatabaseConnectorDbiConnection")
  DatabaseConnector::disconnect(connection)
})
#> Test passed 🥇

connection <- createConnectionSQLite()

test_that("Insert table", {
  createTrajectoriesTable(conn = connection, dbms = dbms, data = data, schema = schema)
  personCount <- DatabaseConnector::querySql(connection, "SELECT COUNT(DISTINCT SUBJECT_ID) FROM main.patient_trajectories;")
  expect_gt(personCount, 0)
})
#> Test passed 🥇

test_that("Disconnect", {
  DatabaseConnector::disconnect(connection)
  expect_false(DatabaseConnector::dbIsValid(connection))
})
#> Test passed 🥇

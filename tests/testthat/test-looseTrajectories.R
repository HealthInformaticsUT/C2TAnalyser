# library(testthat)
#
# pathToDriver <- './Drivers'
# dbms <- "sqlite"
# schema <- "main"
# pathToResults <<- dirname(dirname(getwd())) #
# data = readr::read_csv(paste(pathToResults,"/TestSchemaTrajectories.csv", sep =""))
#
# test_that("Quering exact trajectories from DB", {
#   connection <- createConnectionSQLite()
#   createTrajectoriesTable(conn = connection, dbms = dbms, data = data, schema = schema)
#   pathToFile = "/inputUI.csv"
#   trajSettings = loadUITrajectories((paste(pathToResults,pathToFile, sep ="")))
#   result = looseTrajectories(
#     connection = connection,
#     dbms = dbms,
#     schema = schema,
#     svector = trajSettings[[1]]$STATE_LABEL
#   )
#   DatabaseConnector::disconnect(connection)
#   expect_equal(length(unique(result$SUBJECT_ID)), 4000)
# })
# #> Test passed 🥇

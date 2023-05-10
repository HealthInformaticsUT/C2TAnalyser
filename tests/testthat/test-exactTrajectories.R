# library(testthat)
#
# pathToDriver <- './Drivers'
# dbms <- "sqlite"
# schema <- "main"
# pathToResults <<- dirname(dirname(getwd())) #
# data = readr::read_csv(paste(pathToResults,"/TestSchemaTrajectories.csv", sep =""))
#
# test_that("Quering all trajectories' statistics table", {
#   connection <- createConnectionSQLite()
#   createTrajectoriesTable(conn = connection, dbms = dbms, data = data, schema = schema)
#   pathToFile = "/inputUI.csv"
#   trajSettings = loadUITrajectories((paste(pathToResults,pathToFile, sep ="")))
#   result = exactTrajectories(
#     connection = connection,
#     dbms = dbms,
#     schema = schema,
#     ivector = trajSettings[[1]]$INDEX,
#     svector = trajSettings[[1]]$STATE_LABEL
#   )
#   DatabaseConnector::disconnect(connection)
#   expect_equal(length(unique(result$SUBJECT_ID)), 2000)
# })
# #> Test passed 🥇

################################################################################
#
# Example
#
################################################################################
devtools::install_github("HealthInformaticsUT/C2TAnalyser")
# Loading UI output trajectory settings
library(C2TAnalyser)
pathToFile = "./inputUI.csv"
trajSettings = loadUITrajectories(pathToFile)


################################################################################
#
# Create connection to database
#
################################################################################

dbExists <- FALSE #If false a local SQLITE server will be created, else fill in database credentials and change to TRUE

connection <- NULL

if(dbExists) {
  ################################################################################
  #
  # Database credentials
  #
  ################################################################################
  pathToDriver <- './Drivers'
  dbms <- "postgresql" #TODO
  user <- 'user' #TODO
  password <- "password" #TODO
  server <- 'localhost' #TODO
  database <- "database"
  port <- '5432' #TODO
  schema <- 'ohdsi_temp' #TODO

  connection <- createConnection(server, database, password, user, dbms, port, pathToDriver)
} else {
  dbms = "sqlite"
  schema = "main"
  # Creating connection to database
  connection <- createConnectionSQLite()
}

# Write datatable into database C2T result

data = readr::read_csv("TestSchemaTrajectories.csv")

data = tranformData(data)

data = dplyr::select(data, SUBJECT_ID, STATE, STATE_START_DATE, STATE_END_DATE)
#data = dplyr::filter(data, (STATE %in% c("START", "EXIT")))
createTrajectoriesTable(conn = connection,dbms = dbms ,schema = schema, data = data)

head(getEdgesDataset(connection, dbms,schema))

queryEdgesDatasetGroup(connection = connection, dbms = dbms,schema = schema, groupId = NULL)
queryNodesDatasetGroup(connection = connection, dbms = dbms,schema = schema, groupId = NULL)

filterBySettings(connection = connection, dbms = dbms, schema = schema, settings = trajSettings)

################################################################################
#
# Trajectory inclusion statistics tables
#
################################################################################

initialTable <- getDistinctTrajectoriesTable(connection = connection,dbms = dbms,schema = schema)
matchTable <- outputTrajectoryStatisticsTables(dataTable = initialTable, settings = trajSettings)

################################################################################
#
# Output all trajectories defined in inputUI.csv
#
################################################################################
result = importTrajectoryData(connection = connection, dbms = dbms, schema = schema, trajectories = matchTable$matching, settings = trajSettings)



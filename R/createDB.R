################################################################################
#
# Create database, sqlite
#
################################################################################

#' Create a SQLite server for using when processing the data
#' @export
createConnectionSQLite <- function(){
  return(DatabaseConnector::connect(dbms = "sqlite", server = tempfile()))
}


################################################################################
#
# Create database connection for remote database
#
################################################################################
#' Create a DB connection for a remote or local data processing
#' @param dbms The database management system
#' @param server IP of the database server
#' @param database Name of the database
#' @param user Username of the database user
#' @param password Password of the database user
#' @param port Port of the database
#' @param pathToDriver Path to drivers' repository
#' @export
createConnection <- function(server = "localhost", database, password, user, dbms, port, pathToDriver){
  connectionDetails <-
    DatabaseConnector::createConnectionDetails(
      dbms = dbms,
      server = paste(server, database, sep = "/"),
      user = user,
      password = password,
      port = port,
      pathToDriver = pathToDriver
    )
  connection <- DatabaseConnector::connect(connectionDetails)
 return(connection)
}

################################################################################
#
# Create table 'patient_trajectories' into the database
#
################################################################################
#' Create support tables and dump data into the database if it is not present
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param data Imported data
#' @param schema Name of the used schema
#' @param tableName Name of the datatable if it exists in the database
#' @param removeID If true the original id of subject will be lost
#' @param removeSTART If true all the "START" states will be removed
#' @param removeEXIT If true all the "EXIT" states will be removed
#' @param fixGender If true all the concept codes will be converted to "MALE", "FEMALE" or "OTHER"
#' @param showOccurrance If true all the states will have an ordinal indicating how many passings for a patient have occurred
#' @export
createTrajectoriesTable <- function(connection, dbms, data = NULL, schema, tableName = NULL, removeID = FALSE, removeSTART = FALSE, removeEXIT = FALSE, fixGender = FALSE, showOccurrance = FALSE){

  if(!(is.null(data) | is.null(tableName))){
    return(print("No data provided! ABORTING!"))
  } else if(is.null(data)) {
    data <- selectTable(connection = connection, dbms = dbms, schema = schema, table = tableName)
  }
  data <- transformData(data, removeID = removeID, removeSTART = removeSTART, removeEXIT = removeEXIT, fixGender = fixGender, showOccurrance = showOccurrance)
  ##############################################################################
  #
  # Lets switch the name of "STATE" in colnames to "STATE_LABEL" or "GROUP" to
  # "GROUP_LABEL" for avoiding possible SQL compiling problems
  #
  ##############################################################################

  # Change labelnames if needed
  lookup <- c(STATE_LABEL = "STATE", GROUP_LABEL = "GROUP")
  data <- dplyr::rename(data, dplyr::any_of(lookup))

  # Add missing columns if needed
  # Add column AGE if needed
  if (!("AGE" %in% colnames(data))) {
    data$AGE = 0
  }
  # Add column GENDER if needed
  if (!("GENDER" %in% colnames(data))) {
    data$GENDER = "OTHER"
  }
  # Add column GROUP_LABEL if needed
  if (!("GROUP_LABEL" %in% colnames(data))) {
    data$GROUP_LABEL = "NA"
  }

    # Select only columns used in the package

  data <- dplyr::select(data, SUBJECT_ID, STATE_LABEL, STATE_START_DATE, STATE_END_DATE, AGE, GENDER, GROUP_LABEL)


  ##############################################################################
  #
  # DROP tables
  #
  ##############################################################################
  dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories')
  dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp')
  dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_combined')
  dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_edges')

  ##############################################################################
  #
  # CREATE tables
  #
  ##############################################################################

  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "patient_trajectories",
                                 databaseSchema = schema,
                                 data = data)

  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "patient_trajectories_temp",
                                 databaseSchema = schema,
                                 data = data)
  ##############################################################################
  #
  # Create trajectory statistics tables
  #
  ##############################################################################

  sql <- "CREATE TABLE @schema.@table AS SELECT trajectory, ARRAY_AGG(subject_id) AS subject_ids, count(*) AS total FROM (SELECT SUBJECT_ID, GROUP_CONCAT(STATE_LABEL, '->>') TRAJECTORY FROM (SELECT SUBJECT_ID, STATE_LABEL, STATE_START_DATE FROM @schema.patient_trajectories ORDER BY SUBJECT_ID, STATE_START_DATE, STATE_END_DATE) tmp1 GROUP BY SUBJECT_ID) aggTrajectories GROUP BY trajectory;"

  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = 'patient_trajectories_combined'
  )
  DatabaseConnector::executeSql(connection = connection, sql)

  ##############################################################################
  #
  # Create table containing information for transitions and edges
  #
  ##############################################################################
  table <- "patient_trajectories_edges"
  if(dbms == "postgresql") {
    sql <- paste("CREATE TABLE ",schema,".",table," AS WITH sorted_states AS (SELECT subject_id, state_label, state_start_date::timestamp, state_end_date::timestamp, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM  ",schema,".patient_trajectories), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG(EXTRACT(EPOCH FROM (s2.state_start_date - s1.state_start_date)/86400)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(time_elapsed)::numeric, 2) AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
  } else if (dbms == "sqlite") {
    sql <- paste("CREATE TABLE ",schema,".",table," AS WITH sorted_states AS (SELECT subject_id, state_label, state_start_date as state_start_date, state_end_date as state_end_date, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM ",schema,".patient_trajectories), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG((s2.state_start_date -s1.state_start_date)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(CAST(time_elapsed AS numeric)), 2)/86400 AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
    } else {return(print("Your DBMS is not supported, please contact package maintainer for an update!"))}

  DatabaseConnector::executeSql(connection = connection, sql)

}
################################################################################
#
# Load UI settings
#
################################################################################
#' Function for loading configuration of selected trajectories
#' @param pathToFile The path to the settings file
#' @export
loadUITrajectories <- function(pathToFile = NULL,
                               settings = NA) {
  if (!is.null(pathToFile)) {
    settings <- readr::read_csv(pathToFile)
    names(settings)[names(settings) == 'STATE'] <- 'STATE_LABEL'
  }

  trajList <- list()

  for (i in unique(settings$TRAJECTORY_ID)) {
    trajData <- dplyr::filter(settings, TRAJECTORY_ID == i)
    trajData <- dplyr::arrange(trajData, TIME)
    trajData <- dplyr::select(trajData, STATE_LABEL, TYPE)

    trajSettings <- list(
      type = unique(trajData$TYPE)[1],
      STATE_LABEL = trajData$STATE_LABEL
    )

    trajList[[i]] <- trajSettings
  }

  # Remove all NULL values
  trajList <- Filter(function(x) !is.null(x), trajList)

  return(trajList)
}



################################################################################
#
# Drop a table
#
################################################################################
#' @keywords internal
dropTable <- function(connection, dbms, schema, table) {
sql_drop <- "IF OBJECT_ID('table', 'U') IS NOT NULL DROP TABLE @schema.@table;"
sql_drop_rendered <- loadRenderTranslateSql(
  dbms = dbms,
  sql = sql_drop,
  schema = schema,
  table = table
)
DatabaseConnector::executeSql(connection = connection, sql_drop_rendered)
}
################################################################################
#
# Select * from table
#
################################################################################
#' @keywords internal
selectTable <- function(connection, dbms, schema, table) {
  sql <- "SELECT * FROM @schema.@table;"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = table
  )
  returnData <- DatabaseConnector::querySql(connection,
                                            sql = sql)
return(returnData)
 }


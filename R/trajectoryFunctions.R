#' Load and translate SQL file or an explicit SQL query to desired dialect.
#'
#' @param sql SQL file name or SQL query
#' @param warnOnMissingParameters Should a warning be raised when parameters provided to this function do not appear in the parameterized SQL that is being rendered? By default, this is TRUE.
#' @param output Should there be a .sql file created of the result
#' @param outputFile Name of the output file
#' @keywords internal
loadRenderTranslateSql <- function(sql,
                                   dbms = "postgresql",
                                   warnOnMissingParameters = TRUE,
                                   output = FALSE,
                                   outputFile,
                                   ...) {
  if (grepl('.sql', sql)) {
    pathToSql <- paste("inst/SQL/", sql, sep = "")
    parameterizedSql <-
      readChar(pathToSql, file.info(pathToSql)$size)[1]
  }
  else {
    parameterizedSql <- sql
  }
 # FIX FOR
 # STRING_AGG(postgrsql) and GROUP_CONCAT(sqlite)
 # TODO add other distinctions
 if (dbms == "postgresql") {
   parameterizedSql <- stringr::str_replace(parameterizedSql, "GROUP_CONCAT", "STRING_AGG")
 }
  else if (dbms == "sqlite") {
    parameterizedSql <- stringr::str_replace(parameterizedSql, "STRING_AGG", "GROUP_CONCAT")
    parameterizedSql <- stringr::str_replace(parameterizedSql, "ARRAY_AGG", "GROUP_CONCAT")
  }
  renderedSql <-
    SqlRender::render(sql = parameterizedSql, warnOnMissingParameters = warnOnMissingParameters, ...)
  renderedSql <-
    SqlRender::translate(sql = renderedSql, targetDialect = dbms)

  if (output == TRUE) {
    SqlRender::writeSql(renderedSql, outputFile)
    writeLines(paste("Created file '", outputFile, "'", sep = ""))
  }
  return(renderedSql)
}

#' Query all trajectories defined in the matching table as matches
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param trajectories The matches in the matching table
#' @param settings The settings of generating trajectories
#' @export
importTrajectoryData <- function(connection, dbms, schema, trajectories, settings) {
################################################################################
# Create a set with eligible patients
################################################################################
  # eligiblePatients <- unique(unlist(returnList))

  # Drop temp table
  dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp')
  dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp_filtered')
################################################################################
# Querying the data
################################################################################

  if(dbms != 'sqlite') {
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = "SELECT subject_id, state_label, state_start_date, state_end_date, age, gender, group_label INTO @schema.patient_trajectories_temp FROM (SELECT * FROM @schema.patient_trajectories WHERE subject_id = ANY(SELECT unnest(subject_ids) from @schema.patient_trajectories_combined where trajectory IN (@trajectories)));",
    schema = schema,
    trajectories = paste0("'", paste(trajectories$TRAJECTORY, collapse = "','"), "'")
  )
  } else {
    sql <- loadRenderTranslateSql(
      dbms = dbms,
      sql = "SELECT subject_id, state_label, state_start_date, state_end_date, age, gender, group_label INTO @schema.patient_trajectories_temp FROM(SELECT * FROM @schema.patient_trajectories WHERE subject_id IN (SELECT CAST(subject_id AS INTEGER) FROM (SELECT DISTINCT subject_ids FROM @schema.patient_trajectories_combined WHERE trajectory IN (@trajectories)  ) AS x WHERE ',' || x.subject_ids || ',' LIKE '%,' || CAST(patient_trajectories.subject_id AS TEXT) || ',%'));",
      schema = schema,
      trajectories = paste0("'", paste(trajectories$TRAJECTORY, collapse = "','"), "'")
    )
  }
  DatabaseConnector::executeSql(connection,
                                  sql = sql)

  if(any(sapply(settings, function(x) x$type == 1))){

    filterBySettings(connection = connection, dbms = dbms, schema = schema, settings = settings)

  }

  returnData <- importTempData(connection, dbms, schema)
  return(returnData)
}

#' Query all patients' data satifying the criteria
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param trajectories The matches in the matching table
#' @export
importTempData <- function(connection, dbms, schema) {
  returnData <- selectTable(connection, dbms, schema, table = 'patient_trajectories_temp')
  colnames(returnData) <- c("SUBJECT_ID", "STATE_LABEL", "STATE_START_DATE", "STATE_END_DATE", "AGE", "GENDER", "GROUP_LABEL")
  return(returnData)
}

#' Remove all instances for patient which occur before selected state occurrence
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param selectedState Selected state
#' @param active Boolean indicating whether or not the function is used in TrajectoryMappings package
#' @export
removeBeforeDatasetDB <- function(connection, dbms, schema, selectedState, active = TRUE) {
  # Drop table
    dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp_temp')

  # If there is no such state it will return zero rows
  sql <- "SELECT subject_id, state_label, state_start_date, state_end_date, age, gender, group_label INTO @schema.patient_trajectories_temp_temp FROM (SELECT pt.subject_id as subject_id, state_label, state_start_date, state_end_date, age, gender, group_label FROM @schema.@table pt JOIN (SELECT subject_id, MIN(state_start_date) AS min_start_date FROM @schema.@table WHERE state_label = '@selectedState' GROUP BY subject_id) s ON pt.subject_id = s.subject_id WHERE pt.subject_id IN (SELECT DISTINCT subject_id FROM @schema.@table WHERE state_label = '@selectedState') AND pt.state_start_date > s.min_start_date OR (pt.state_start_date = s.min_start_date AND state_label = '@selectedState') ORDER BY pt.subject_id, pt.state_start_date, pt.state_end_date) foo;"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = 'patient_trajectories_temp',
    selectedState = selectedState
  )
  DatabaseConnector::executeSql(connection = connection, sql)

  # Drop table
  if(active){
    dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp')

    sql <- "ALTER TABLE @schema.@tableB RENAME TO @tableA;"
    sql <- loadRenderTranslateSql(
      dbms = dbms,
      sql = sql,
      schema = schema,
      tableA = 'patient_trajectories_temp',
      tableB = 'patient_trajectories_temp_temp'
    )
    DatabaseConnector::executeSql(connection = connection, sql)
  }
  # colnames(returnData) <- c("SUBJECT_ID", "STATE_LABEL", "STATE_START_DATE", "STATE_END_DATE", "AGE", "GENDER", "GROUP_LABEL")
  #
  # return(returnData)
}

#' Remove all instances for patient which occur after selected state occurrence
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param selectedState Selected state
#' @param active Boolean indicating whether or not the function is used in TrajectoryMappings package
#' @export
removeAfterDatasetDB <- function(connection, dbms, schema, selectedState, active = TRUE) {
  # Drop table
  dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp_temp')

  # If there is no such state it will return zero rows
  sql <- "SELECT subject_id, state_label, state_start_date, state_end_date, age, gender, group_label INTO @schema.patient_trajectories_temp_temp FROM (SELECT pt.subject_id as subject_id, state_label, state_start_date, state_end_date, age, gender, group_label FROM @schema.@table pt JOIN (SELECT subject_id, MAX(state_start_date) AS max_start_date FROM @schema.@table WHERE state_label = '@selectedState' GROUP BY subject_id) s ON pt.subject_id = s.subject_id WHERE pt.subject_id IN (SELECT DISTINCT subject_id FROM @schema.@table WHERE state_label = '@selectedState') AND pt.state_start_date <= s.max_start_date ORDER BY pt.subject_id, pt.state_start_date, pt.state_end_date) foo;"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = 'patient_trajectories_temp',
    selectedState = selectedState
  )
  DatabaseConnector::executeSql(connection = connection, sql)

  # Drop table
  if(active){
    dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp')

    sql <- "ALTER TABLE @schema.@tableB RENAME TO @tableA;"
    sql <- loadRenderTranslateSql(
      dbms = dbms,
      sql = sql,
      schema = schema,
      tableA = 'patient_trajectories_temp',
      tableB = 'patient_trajectories_temp_temp'
    )
    DatabaseConnector::executeSql(connection = connection, sql)
  }
}

#' Query all data about edges and mean elapsed time
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @export
getEdgesDataset <- function(connection, dbms,schema) {
  returnData <- selectTable(connection, dbms, schema, table = 'patient_trajectories_edges')
  colnames(returnData) <- c("FROM", "TO", "AVG_TIME_BETWEEN", "COUNT")
  return(returnData)
  }

#' Query data about edges from specified group
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param groupId Group id for patients
#' @export
queryEdgesDatasetGroup <- function(connection, dbms, schema, groupId = NULL) {
  if (is.null(groupId)) {
    if(dbms == "postgresql") {
      sql <- paste("WITH sorted_states AS (SELECT subject_id, state_label, state_start_date::timestamp, state_end_date::timestamp, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM  ",schema,".patient_trajectories_temp), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG(EXTRACT(EPOCH FROM (s2.state_start_date - s1.state_start_date)/86400)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(time_elapsed)::numeric, 2) AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
    } else if (dbms == "sqlite") {
      sql <- paste("WITH sorted_states AS (SELECT subject_id, state_label, state_start_date as state_start_date, state_end_date as state_end_date, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM ",schema,".patient_trajectories_temp), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG((s2.state_start_date -s1.state_start_date)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(CAST(time_elapsed AS numeric)), 2)/86400 AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
    } else {return(print("Your DBMS is not supported, please contact package maintainer for an update!"))}
  } else {
  if(dbms == "postgresql") {
    sql <- paste("WITH sorted_states AS (SELECT subject_id, state_label, state_start_date::timestamp, state_end_date::timestamp, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM  ",schema,".patient_trajectories_temp WHERE group_label = '", groupId ,"'), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG(EXTRACT(EPOCH FROM (s2.state_start_date - s1.state_start_date)/86400)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(time_elapsed)::numeric, 2) AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
  } else if (dbms == "sqlite") {
    sql <- paste("WITH sorted_states AS (SELECT subject_id, state_label, state_start_date as state_start_date, state_end_date as state_end_date, ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY state_start_date, state_end_date) AS row_num FROM ",schema,".patient_trajectories_temp WHERE group_label = '", groupId ,"'), state_transitions AS (SELECT s1.subject_id, s1.state_label AS from_state, s2.state_label AS to_state, s1.subject_id AS subject_ids, AVG((s2.state_start_date -s1.state_start_date)) AS time_elapsed FROM sorted_states s1 JOIN sorted_states s2 ON s1.subject_id = s2.subject_id AND s1.row_num = s2.row_num - 1 GROUP BY s1.subject_id, from_state, to_state, s1.subject_id) SELECT from_state AS source, to_state AS target, ROUND(AVG(CAST(time_elapsed AS numeric)), 2)/86400 AS avg_time_elapsed, COUNT(*) AS total_transitions FROM state_transitions GROUP BY from_state, to_state;", sep = "")
  } else {return(print("Your DBMS is not supported, please contact package maintainer for an update!"))}
}
  returnData <- DatabaseConnector::querySql(connection,
                                            sql = sql)
  colnames(returnData) <- c("FROM", "TO", "AVG_TIME_BETWEEN", "COUNT")
  return(returnData)
}

#' Query data about nodes from specified group
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param groupId Group id for patients
#' @export
queryNodesDatasetGroup <- function(connection, dbms, schema, groupId = NULL) {
  if (is.null(groupId)) {
    if(dbms == "postgresql") {
      sql <- paste("WITH sorted_states AS (SELECT state_label, state_start_date::timestamp, state_end_date::timestamp FROM ", schema, ".patient_trajectories_temp), state_durations AS (SELECT state_label, AVG(EXTRACT(EPOCH FROM (state_end_date - state_start_date))/86400) AS avg_duration FROM sorted_states GROUP BY state_label) SELECT state_label AS state, ROUND(avg_duration::numeric, 2) AS avg_duration FROM state_durations ORDER BY state_label", sep = "")
    } else if (dbms == "sqlite") {
      sql <- paste("WITH sorted_states AS (SELECT state_label, datetime(state_start_date, 'unixepoch') as state_start_date, datetime(state_end_date, 'unixepoch') as state_end_date FROM ", schema, ".patient_trajectories_temp), state_durations AS (SELECT state_label, AVG((strftime('%s', state_end_date) - strftime('%s', state_start_date))/86400.0) AS avg_duration FROM sorted_states GROUP BY state_label) SELECT state_label AS state, ROUND(avg_duration, 2) AS avg_duration FROM state_durations ORDER BY state_label", sep = "")        } else {
      return(print("Your DBMS is not supported, please contact package maintainer for an update!"))
    }
  } else {
    if(dbms == "postgresql") {
      sql <- paste("WITH sorted_states AS (SELECT state_label, state_start_date::timestamp, state_end_date::timestamp FROM ", schema, ".patient_trajectories_temp WHERE group_label = '", groupId ,"'), state_durations AS (SELECT state_label, AVG(EXTRACT(EPOCH FROM (state_end_date - state_start_date))/86400) AS avg_duration FROM sorted_states GROUP BY state_label) SELECT state_label AS state, ROUND(avg_duration::numeric, 2) AS avg_duration FROM state_durations ORDER BY state_label", sep = "")
    } else if (dbms == "sqlite") {
      sql <- paste("WITH sorted_states AS (SELECT state_label, datetime(state_start_date, 'unixepoch') as state_start_date, datetime(state_end_date, 'unixepoch') as state_end_date FROM ", schema, ".patient_trajectories_temp WHERE group_label = '", groupId ,"'), state_durations AS (SELECT state_label, AVG((strftime('%s', state_end_date) - strftime('%s', state_start_date))/86400.0) AS avg_duration FROM sorted_states GROUP BY state_label) SELECT state_label AS state, ROUND(avg_duration, 2) AS avg_duration FROM state_durations ORDER BY state_label", sep = "")    } else {
      return(print("Your DBMS is not supported, please contact package maintainer for an update!"))
    }
  }
  returnData <- DatabaseConnector::querySql(connection, sql = sql)
  return(returnData)
}

#' Remove all instances for patient which occur after selected state occurrence
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param settings The settings of generating trajectories
#' @keywords internal
filterBySettings <-
  function(connection,
           dbms,
           schema,
           settings) {
    sql <- "SELECT TOP 0 * INTO @schema.@tableB FROM @schema.@tableA;"
    sql <- loadRenderTranslateSql(
      dbms = dbms,
      sql = sql,
      schema = schema,
      tableA = 'patient_trajectories_temp',
      tableB = "patient_trajectories_temp_filtered"
    )
    DatabaseConnector::executeSql(connection = connection, sql)
    unlist(lapply(settings, function(setting){
      # Exact trajectory case meaning we will cut trajectories starting and ending points
      if (setting$type == 1) {
        firstState = setting$STATE_LABEL[1]
        lastState = setting$STATE_LABEL[length(setting$STATE_LABEL)]

        # Drop table
        dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp_filtering1')
        dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp_filtering2')
        # Remove before. If there is no such state it will return zero rows

        sql <- "SELECT subject_id, state_label, state_start_date, state_end_date, age, gender, group_label INTO @schema.patient_trajectories_temp_filtering1 FROM (SELECT pt.subject_id as subject_id, state_label, state_start_date, state_end_date, age, gender, group_label FROM @schema.@table pt JOIN (SELECT subject_id, MIN(state_start_date) AS min_start_date FROM @schema.@table WHERE state_label = '@selectedState' GROUP BY subject_id) s ON pt.subject_id = s.subject_id WHERE pt.subject_id IN (SELECT DISTINCT subject_id FROM @schema.@table WHERE state_label = '@selectedState') AND pt.state_start_date >= s.min_start_date OR (pt.state_start_date = s.min_start_date AND state_label = '@selectedState') ORDER BY pt.subject_id, pt.state_start_date, pt.state_end_date) foo;"
        sql <- loadRenderTranslateSql(
          dbms = dbms,
          sql = sql,
          schema = schema,
          table = 'patient_trajectories_temp',
          selectedState = firstState
        )


        DatabaseConnector::executeSql(connection = connection, sql)

        # Remove after. If there is no such state it will return zero rows.
        # If there is no such state it will return zero rows
        sql <- "SELECT subject_id, state_label, state_start_date, state_end_date, age, gender, group_label INTO @schema.patient_trajectories_temp_filtering2 FROM (SELECT pt.subject_id as subject_id, state_label, state_start_date, state_end_date, age, gender, group_label FROM @schema.@table pt JOIN (SELECT subject_id, MAX(state_start_date) AS max_start_date FROM @schema.@table WHERE state_label = '@selectedState' GROUP BY subject_id) s ON pt.subject_id = s.subject_id WHERE pt.subject_id IN (SELECT DISTINCT subject_id FROM @schema.@table WHERE state_label = '@selectedState') AND pt.state_start_date <= s.max_start_date ORDER BY pt.subject_id, pt.state_start_date, pt.state_end_date) foo;"
        sql <- loadRenderTranslateSql(
          dbms = dbms,
          sql = sql,
          schema = schema,
          table = 'patient_trajectories_temp_filtering1',
          selectedState = lastState
        )
        DatabaseConnector::executeSql(connection = connection, sql)

        dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp_filtering1')



        mergeNewSubjects(connection = connection, dbms = dbms, schema = schema, tableA = "patient_trajectories_temp_filtered", tableB = "patient_trajectories_temp_filtering2")

      }
    }))
    dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp')

    sql <- "ALTER TABLE @schema.@tableB RENAME TO @tableA;"
    sql <- loadRenderTranslateSql(
      dbms = dbms,
      sql = sql,
      schema = schema,
      tableA = 'patient_trajectories_temp',
      tableB = 'patient_trajectories_temp_filtered'
    )
    DatabaseConnector::executeSql(connection = connection, sql)
    dropTable(connection = connection, dbms = dbms, schema = schema, table = 'patient_trajectories_temp_filtered')
  }

#' Merge two tables, only updating subject_ids not present in tableA
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Schema in the database where the tables are located
#' @param tableA Target table (Being updated for new subject_ids)
#' @param tableA Source table (Source of new subject_ids)
#' @keywords internal
mergeNewSubjects <- function(connection, dbms, schema, tableA, tableB){

  sql <- "IF OBJECT_ID('table', 'U') IS NOT NULL DROP TABLE #temp_table;"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql
  )
  DatabaseConnector::executeSql(connection = connection, sql)
  # Insert all data from patient_trajectories_temp_filtering2 into a temporary table
  sql <- "SELECT * INTO #temp_table FROM @schema.@table;"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = tableB
  )
  DatabaseConnector::executeSql(connection = connection, sql)
  # Delete rows from patient_trajectories_temp that have a matching subject_id in #temp_table
  sql <- "DELETE FROM  #temp_table WHERE subject_id IN (SELECT subject_id FROM @schema.@table);"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = tableA
  )
  DatabaseConnector::executeSql(connection = connection, sql)
  # Insert all remaining rows from #temp_table into patient_trajectories_temp
  sql <- "INSERT INTO @schema.@table SELECT * FROM #temp_table;"
  sql <- loadRenderTranslateSql(
    dbms = dbms,
    sql = sql,
    schema = schema,
    table = tableA
  )
  DatabaseConnector::executeSql(connection = connection, sql)
}

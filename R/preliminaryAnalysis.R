################################################################################
#
# Summarization functions
#
################################################################################

#' Create a table summarizing the total count of all distinct trajectories
#'
#' @param connection Connection to the database (DatabaseConnector)
#' @param dbms The database management system
#' @param schema Name of the used schema
#' @param table Name of the used table
#' @export
getDistinctTrajectoriesTable <- function(connection, dbms, schema, table = "patient_trajectories_combined") {
sql <- "SELECT trajectory, total FROM @schema.@table;"

sql <- loadRenderTranslateSql(
  dbms = dbms,
  sql = sql,
  schema = schema,
  table = table
)
resultTable <- DatabaseConnector::querySql(connection, sql)

amountTrajectories <- sum(resultTable$TOTAL)
resultTable$PERC <- paste(round(resultTable$TOTAL*100/amountTrajectories,2), "%",sep = "")

return(resultTable)
}


#' Query data tables (matching, partially matching, not matching) defined in the settings for providing inclusion statistics
#'
#' @param dataTable Result of getDistinctTrajectoriesTable function
#' @param settings The settings of generating trajectories
#' @export
outputTrajectoryStatisticsTables <- function(dataTable, settings = NULL) {
  if (is.null(settings)){
    result <- list(
      "matching" = dataTable[0,],
      "partiallyMatching" = dataTable[0,],
      "notMatching" = dataTable
    )
    return(result)
  }

  type1_present <- any(sapply(settings, function(x) x$type == 1))

  settings <- settings[order(sapply(settings, function(x) x$type), decreasing = TRUE)]

  trajDefined <- unlist(lapply(settings, function(setting){
    table <- setting$STATE_LABEL
    type <- setting$type

    if (type1_present && type == 0) {
      return(NULL)
    }

    trajStates <- NULL
    trajStates <- c() # Return vector with all relevant trajectories
    for (trajectoryPresent in dataTable$TRAJECTORY) { # We start from looping over all present trajectories
      trajectoryPresentAtomicOriginal <-  stringr::str_split(trajectoryPresent, pattern = "->>")[[1]] # Break present trajectory down to states
      trajectoryPresentAtomic <- trajectoryPresentAtomicOriginal
      i <- 0
      k <- 0 # for iterating the trajectoryPresent variable
      trajectorySelectedAtomic <- table
      for (state in trajectorySelectedAtomic) { # Break selected trajectory down to states and loop over states
        if(state %in% trajectoryPresentAtomic){ # Check if state present in present trajectory
          index <- match(state,trajectoryPresentAtomic) # Find first occurrance index
          i <- i + 1
          k <- k + index
          if (i == length(trajectorySelectedAtomic)) { # If we are observing the last element of present trajectory and the state of selected trajectory is also the last one -- return
            trajStates <- c(trajStates, paste0( trajectoryPresentAtomicOriginal[1:k], collapse = "->>") ) # Add trajectory to return vector
            break
          }
          else if (index == length(trajectoryPresentAtomic)) {break}
          trajectoryPresentAtomic <- trajectoryPresentAtomic[(match(state,trajectoryPresentAtomic)+1):length(trajectoryPresentAtomic)] # Lets keep the tail of present trajectory
        }
        else {break}
      }
    }

    if (type == 1) {
      trajStates <- trajStates[sapply(trajStates, function(x) {
        trajectoryStates <- stringr::str_split(x, pattern = "->>")[[1]]
        all(sapply(table, function(state) sum(state == trajectoryStates) >= sum(state == table)))
      })]
    }

    return(unique(trajStates))
  }))

  trajDefined <- Filter(Negate(is.null), trajDefined) # Remove NULL values from trajDefined

  indexes <- 1:nrow(dataTable)
  matchingVec <- as.logical(Reduce("+",lapply(trajDefined, function(x){
    grepl(x, dataTable$TRAJECTORY)
  })))
  partiallyMatchingVec <- as.logical(Reduce("+", lapply(trajDefined,function(x){
    split_string <- strsplit(x, "->>")
    resulting_array <- unlist(split_string)
    sapply(strsplit(dataTable$TRAJECTORY, "->>"), function(x) any(x %in% resulting_array))
  })))
  if (length(matchingVec)==0) {
    matchingVec <- rep(FALSE, nrow(dataTable))
    partiallyMatchingVec <- rep(FALSE, nrow(dataTable))
  }
  result <- list(
    "matching" = dataTable[indexes[matchingVec],],
    "partiallyMatching" = dataTable[indexes[as.logical(partiallyMatchingVec - matchingVec)],],
    "notMatching" = dataTable[indexes[!partiallyMatchingVec],]
  )
  return(result)
}


################################################################################
#
# Prepare dataset
#
################################################################################

#' Prepare Cohort2Trajectory output to be used in the analyser
#'
#' @param data Trajectories dataset from Cohort2Trajectory
#' @param removeID If true the original id of subject will be lost
#' @param removeSTART If true all the "START" states will be removed
#' @param removeEXIT If true all the "EXIT" states will be removed
#' @param fixGender If true all the concept codes will be converted to "MALE", "FEMALE" or "OTHER"
#' @export
tranformData <-
  function(data,
           removeID = TRUE,
           removeSTART = TRUE,
           removeEXIT = TRUE,
           fixGender = TRUE) {
    returnData <- data
    cols_to_check <-
      c(
        "SUBJECT_ID",
        "STATE_LABEL",
        "STATE_START_DATE",
        "STATE_END_DATE",
        "GENDER_CONCEPT_ID",
        "AGE"
      )

    for (col in cols_to_check) {
      if (!(col %in% colnames(returnData))) {
        returnData[[col]] <- 0
      }
    }
    returnData <-
      dplyr::select(
        returnData,
        SUBJECT_ID,
        STATE_LABEL,
        STATE_START_DATE,
        STATE_END_DATE,
        GENDER_CONCEPT_ID,
        AGE
      )

    colnames(returnData) <-
      c(
        "SUBJECT_ID",
        "STATE_LABEL",
        "STATE_START_DATE",
        "STATE_END_DATE",
        "GENDER",
        "AGE"
      )

    if (removeID)
    {
      returnData$SUBJECT_ID <- as.numeric(factor(returnData$SUBJECT_ID))
    }
    if (removeSTART)
    {
      returnData <- dplyr::filter(returnData, STATE_LABEL != "START")
    }
    if (removeEXIT)
    {
      returnData <- dplyr::filter(returnData, STATE_LABEL != "EXIT")
    }
    if (fixGender)
    {
      returnData <-
        dplyr::mutate(returnData, GENDER = ifelse(
          GENDER == 8507,
          "MALE",
          ifelse(GENDER == 8532, "FEMALE",
                 "OTHER")
        ))
    }
    return(returnData)
  }

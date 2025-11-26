#' @title Load multiple PRO files
#'
#' @description Batch processes one to many MOCNESS PRO files in a directory
#' with error handling and comprehensive reporting.
#'
#' @note See \code{ingest_pro_file} in this package to harvest information from
#' a one, specific PRO file.
#'
#' @param directory Character string. Directory path containing PRO files.
#'   Default is current directory (".").
#' @param daynight Boolean. Whether to annotate the data with day-night 
#'   designation based on ship position and time of collection. Default is FALSE.
#'
#' @return A list of data frames, one for each successfully processed PRO file.
#'   The list has attributes containing processing summary information:
#'   \item{failed_files}{Character vector of filenames that failed to process}
#'   \item{error_messages}{Character vector of corresponding error messages}
#'   \item{processing_summary}{List with detailed processing information}
#'
#' @details
#' This function:
#' \itemize{
#'   \item Searches for all .pro files in the specified directory
#'   \item Processes each file using \code{\link{ingest_pro_file}}
#'   \item Provides detailed progress reporting
#'   \item Handles errors gracefully and continues processing
#'   \item Returns comprehensive summary of successes and failures
#' }
#'
#' @examples
#' \dontrun{
#' # Process all PRO files in current directory
#' all_data <- load_pro_files()
#'
#' # Process files with day/night annotation
#' all_data <- load_pro_files(daynight = TRUE)
#'
#' # Process files in specific directory
#' all_data <- load_pro_files("path/to/pro/files")
#'
#' # Combine all successful files into single data frame
#' combined_data <- dplyr::bind_rows(all_data)
#'
#' # Check for any failed files
#' attr(all_data, "failed_files")
#' }
#'
#' @importFrom purrr imap safely compact
#' @importFrom tools file_ext
#'
#' @seealso \code{\link{ingest_pro_file}} for processing individual files
#'
#' @export
load_pro_files <- function(
  directory = ".",
  daynight  = FALSE
  ) {

  # initialize tracking variables for failed files
  failed_files   <- character(0)
  error_messages <- character(0)

  all_files <- list.files(directory, full.names = TRUE)
  pro_files <- all_files[tolower(tools::file_ext(all_files)) == "pro"]

  if (length(pro_files) == 0) {
    stop("DNF PRO files in this directory: ", directory)
  }

  cat("Found", length(pro_files), "PRO files to process\n")
  if (daynight) {
    cat("Day/night annotation enabled\n")
  }
  cat("\n")

  # process each file using purrr::map with safe error handling
  pro_data_list <- purrr::imap(
    pro_files,
    ~ {
      file_name <- basename(.x)
      cat(
        "=== Processing file",
        .y,
        "of",
        length(pro_files),
        ":",
        file_name,
        "===\n"
      )

      # use safely to handle errors gracefully
      safe_ingest <- purrr::safely(ingest_pro_file)
      result      <- safe_ingest(.x, daynight = daynight)  # Pass daynight parameter

      if (!is.null(result$error)) {
        error_msg <- result$error$message
        cat("Error processing", file_name, ":", error_msg, "\n\n")

        # track failed files (using <<- to modify parent scope variables)
        failed_files <<- c(failed_files, file_name)
        error_messages <<- c(error_messages, error_msg)

        return(NULL)
      }

      return(result$result)
    }
  )

  # set names and remove NULL entries (failed files)
  names(pro_data_list) <- basename(pro_files)
  pro_data_list        <- purrr::compact(pro_data_list)

  # report processing summary
  cat("=== PROCESSING SUMMARY ===\n")
  cat("Total files found:", length(pro_files), "\n")
  cat("Successfully processed:", length(pro_data_list), "\n")
  cat("Failed to process:", length(failed_files), "\n")

  if (length(failed_files) > 0) {
    cat("\nFailed files:\n")
    for (i in seq_along(failed_files)) {
      cat(sprintf(
        "  %d. %s\n     Error: %s\n",
        i,
        failed_files[i],
        error_messages[i]
      ))
    }
  } else {
    cat("All files processed successfully!\n")
  }
  cat("\n")

  # add failed files information as attributes to the result
  attr(pro_data_list, "failed_files") <- failed_files
  attr(pro_data_list, "error_messages") <- error_messages
  attr(pro_data_list, "processing_summary") <- list(
    total_files      = pro_files,
    successful_files = pro_data_list,
    failed_files     = failed_files
  )

  return(pro_data_list)
}


#' @title Load and process MOCNESS PRO file
#'
#' @description Function to ingest a MOCNESS PRO file, extracting metadata and
#' data, converting time formats, and adding timezone information based on
#' coordinates.
#'
#' @param file_path Character string. Path to the PRO file to process.
#' @param daynight (boolean) Boolean indicating whether to annotate the data
#'   with a day-night designation based on ship position and time of collection.
#'
#' @return A data frame containing the processed PRO file data with additional columns:
#'   \item{datetime_gmt}{POSIXct. Datetime in GMT timezone}
#'   \item{datetime_local}{POSIXct. Datetime in local timezone}
#'   \item{timezone}{Character. Local timezone identifier}
#'   \item{file_name}{Character. Original filename}
#'   Plus all metadata fields and original data columns.
#'
#' @details
#' The function performs the following operations:
#' \itemize{
#'   \item Extracts metadata from the file header
#'   \item Reads tabular data starting from the "time" header line
#'   \item Converts decimal day-of-year time to proper datetime objects
#'   \item Determines local timezone from coordinates
#'   \item Adds comprehensive processing information
#' }
#'
#' @examples
#' \dontrun{
#' # Load a single PRO file
#' data <- ingest_pro_file("MOC1_01A.PRO")
#'
#' # View the structure
#' str(data)
#' }
#'
#' @importFrom readr read_lines read_table
#' @importFrom stringr str_detect
#' @importFrom purrr iwalk
#' @importFrom lubridate year with_tz
#'
#' @export
ingest_pro_file <- function(
  file_path,
  daynight = FALSE
  ) {
  cat("Processing file:", file_path, "\n")

  # extract metadata
  metadata <- extract_pro_metadata(file_path)
  cat(
    "Extracted metadata for Tow",
    metadata$tow_number,
    "on",
    as.character(metadata$date),
    "\n"
  )

  # Find the header line (starts with "time")
  all_lines       <- readr::read_lines(file_path)
  header_line_idx <- which(stringr::str_detect(all_lines, "^time\\s+"))

  if (length(header_line_idx) == 0) {
    stop("Could not find data header line starting with 'time'")
  }

  # read the data starting from the line after the header
  data <- readr::read_table(
    file = file_path,
    skip = header_line_idx - 1, # skip header lines
    col_names = TRUE,
    show_col_types = FALSE
  )

  cat("Read", nrow(data), "data rows with", ncol(data), "columns\n")

  # add header metadata as columns (repeat for each data row)
  if (length(metadata) > 0) {
    purrr::iwalk(
      metadata,
      ~ {
        data[[.y]] <<- .x
      }
    )
  }

  # add file metadata
  filename <- basename(file_path)
  data$file_name <- filename

  # extract MOC number from filename
  # Handle both "MOC" and "M" prefixes
  if (
    (grepl("(?i)moc", filename) || grepl("(?i)^m[0-9]+", filename)) &&
      grepl("[0-9]+", filename)
  ) {
    # Extract the first number found in the filename
    moc_number <- as.numeric(stringr::str_extract(filename, "[0-9]+"))
    data$moc <- moc_number
    cat("Extracted MOC number:", moc_number, "from filename\n")
  } else {
    data$moc <- NA_real_
    cat("MOC number not found in filename\n")
  }

  # convert time column to datetime
  if ("time" %in% names(data)) {
    # convert to gmt datetime
    data$datetime_gmt <- convert_doy_to_datetime(
      data$time,
      lubridate::year(metadata$date),
      "GMT"
    )

    # get local timezone from coordinates
    local_tz <- get_timezone_from_coords(data$lat, data$lon)
    cat("Detected local timezone as:", local_tz, "\n")

    # convert to local time
    data$datetime_local <- lubridate::with_tz(data$datetime_gmt, local_tz)
    data$timezone <- local_tz

    if (daynight == TRUE) {
      data <- annotate_daytime(data)
    }

  }

  cat("processed PRO file with", nrow(data), "records\n")
  cat(
    "Time range:",
    as.character(min(data$datetime_gmt, na.rm = TRUE)),
    "to",
    as.character(max(data$datetime_gmt, na.rm = TRUE)),
    "(GMT)\n"
  )
  cat(
    "Depth range:",
    round(min(data$pres, na.rm = TRUE), 2),
    "to",
    round(max(data$pres, na.rm = TRUE), 2),
    "meters\n"
  )
  cat(
    "Location range: Lat",
    round(min(data$lat, na.rm = TRUE), 4),
    "to",
    round(max(data$lat, na.rm = TRUE), 4),
    ", Lon",
    round(min(data$lon, na.rm = TRUE), 4),
    "to",
    round(max(data$lon, na.rm = TRUE), 4),
    "\n\n"
  )

  return(data)
}


#' @title Extract metadata from PRO file header
#'
#' @description Parses the header section of a MOCNESS PRO file to extract
#' metadata including tow information, date, instrument serial numbers, and
#' calibration data. Handles different tow line formats.
#'
#' @param file_path Character string. Path to the PRO file to process.
#'
#' @return A list containing extracted metadata with elements:
#'   \item{tow_number}{Numeric. The tow number}
#'   \item{vessel}{Character. Vessel name}
#'   \item{cruise_id}{Character. Cruise identifier}
#'   \item{date}{Date. Sampling date}
#'   \item{temperature_probe}{Character. Temperature probe serial number}
#'   \item{conductivity_probe}{Character. Conductivity probe serial number}
#'   \item{pressure_probe}{Character. Pressure probe serial number}
#'   \item{oxygen_probe}{Character. Oxygen probe serial number}
#'   \item{fluorometer}{Character. Fluorometer identifier}
#'   \item{flow_meter_calibration}{Numeric. Flow meter calibration value}
#'   \item{flow_meter_units}{Character. Flow meter units}
#'
#' @importFrom readr read_lines
#' @importFrom stringr str_split str_extract str_trim
#' @importFrom lubridate mdy
#'
#' @keywords internal
extract_pro_metadata <- function(file_path) {
  # read first few lines to get header information
  header_lines <- readr::read_lines(file_path, n_max = 10)

  # initialize metadata list
  metadata <- list()

  # extract tow information - handle different formats
  tow_line <- header_lines[grepl("^%.*Tow:", header_lines)]
  if (length(tow_line) > 0) {
    # Split by whitespace and remove empty elements
    tow_parts <- stringr::str_split(tow_line, "\\s+")[[1]]
    tow_parts <- tow_parts[tow_parts != ""]  # Remove empty strings
    
    if (length(tow_parts) >= 4) {
      metadata$tow_number <- as.numeric(tow_parts[3])
      
      # Handle different vessel/cruise formats
      if (length(tow_parts) == 5) {
        # Format: %	Tow:	1  AE  AE2214
        # tow_parts[4] = vessel (e.g., "AE")
        # tow_parts[5] = cruise (e.g., "AE2214")
        metadata$vessel <- stringr::str_trim(tow_parts[4])
        metadata$cruise_id <- stringr::str_trim(tow_parts[5])
      } else if (length(tow_parts) >= 6) {
        # Format: %	Tow:	13  Sally Ride  SR2408
        # tow_parts[4:5] = vessel (e.g., "Sally", "Ride")
        # tow_parts[6] = cruise (e.g., "SR2408")
        metadata$vessel <- paste(stringr::str_trim(tow_parts[4:5]), collapse = " ")
        metadata$cruise_id <- stringr::str_trim(tow_parts[6])
      } else {
        # Fallback: treat everything after tow number as vessel
        remaining_parts <- tow_parts[4:length(tow_parts)]
        if (length(remaining_parts) > 1) {
          # Assume last part is cruise_id, rest is vessel
          metadata$vessel <- paste(remaining_parts[1:(length(remaining_parts)-1)], collapse = " ")
          metadata$cruise_id <- remaining_parts[length(remaining_parts)]
        } else {
          # Only one remaining part - treat as vessel
          metadata$vessel <- remaining_parts[1]
          metadata$cruise_id <- NA_character_
        }
      }
    }
  }

  # extract date
  date_line <- header_lines[grepl("^%.*Date:", header_lines)]
  if (length(date_line) > 0) {
    date_str <- stringr::str_extract(date_line, "\\d+/\\d+/\\d+")
    metadata$date <- lubridate::mdy(date_str)
  }

  # extract instrument serial numbers
  temp_line <- header_lines[grepl("Temperature.*Probe", header_lines)]
  if (length(temp_line) > 0) {
    metadata$temperature_probe <- stringr::str_extract(temp_line, "\\d+")
  }

  cond_line <- header_lines[grepl("Conductivity.*Probe", header_lines)]
  if (length(cond_line) > 0) {
    metadata$conductivity_probe <- stringr::str_extract(cond_line, "\\d+$")
  }

  press_line <- header_lines[grepl("Pressure.*Probe", header_lines)]
  if (length(press_line) > 0) {
    metadata$pressure_probe <- stringr::str_extract(press_line, "\\d+")
  }

  oxygen_line <- header_lines[grepl("Oxygen.*Probe", header_lines)]
  if (length(oxygen_line) > 0) {
    metadata$oxygen_probe <- stringr::str_extract(oxygen_line, "\\d+$")
  }

  fluor_line <- header_lines[grepl("Fluorometer", header_lines)]
  if (length(fluor_line) > 0) {
    metadata$fluorometer <- stringr::str_extract(fluor_line, "[A-Z]+\\d+")
  }

  # extract flow meter calibration
  flow_line <- header_lines[grepl("Flow.*Meter.*Calibration", header_lines)]
  if (length(flow_line) > 0) {
    metadata$flow_meter_calibration <- as.numeric(stringr::str_extract(
      flow_line,
      "\\d+\\.\\d+"
    ))
    metadata$flow_meter_units <- stringr::str_extract(flow_line, "\\([^)]+\\)")
  }

  return(metadata)
}


#' @title Convert day of year decimal time to datetime
#'
#' @description Converts a decimal day of year format (e.g., 123.5 = day 123,
#' 12:00) to a proper datetime object.
#'
#' @param doy_decimal Numeric vector. Decimal day of year values.
#' @param year Numeric. The year for the conversion.
#' @param timezone Character. Timezone for the conversion. Default is "GMT".
#'
#' @return POSIXct datetime vector.
#'
#' @examples
#' \dontrun{
#' # Convert day 100.5 (April 10th, 12:00) of 2024 to datetime
#' convert_doy_to_datetime(100.5, 2024, "GMT")
#' }
#'
#' @importFrom lubridate as_datetime days seconds
#' @export
convert_doy_to_datetime <- function(
  doy_decimal,
  year,
  timezone = "GMT"
) {
  # split into day of year and decimal time
  doy <- floor(doy_decimal)
  decimal_time <- doy_decimal - doy

  # convert to datetime
  start_of_year <- lubridate::as_datetime(paste0(year, "-01-01"), tz = timezone)
  days_to_add <- doy - 1 # DOY 1 = Jan 1
  seconds_to_add <- decimal_time * 24 * 3600 # Convert decimal day to seconds

  datetime <- start_of_year +
    lubridate::days(days_to_add) +
    lubridate::seconds(seconds_to_add)

  return(datetime)
}


#' @title Get timezone from coordinates
#'
#' @description Determines the timezone based on latitude and longitude
#' coordinates using the first valid coordinate pair in the provided vectors.
#'
#' @param lat Numeric vector. Latitude coordinates.
#' @param lon Numeric vector. Longitude coordinates.
#'
#' @return Character string representing the timezone (e.g., "America/New_York").
#'   Returns "GMT" if no valid coordinates are found.
#'
#' @examples
#' # Get timezone for coordinates in the Pacific
#' # ecotaxaLoadR::get_timezone_from_coords(lat = 34.0522, lon = -118.2437)
#'
#' @importFrom lutz tz_lookup_coords
#'
#' @keywords internal
get_timezone_from_coords <- function(
  lat,
  lon
) {
  # use the first non-na coordinate pair to determine timezone
  valid_coords <- which(!is.na(lat) & !is.na(lon))

  if (length(valid_coords) > 0) {
    first_valid <- valid_coords[1]
    tz <- lutz::tz_lookup_coords(
      lat = lat[first_valid],
      lon = lon[first_valid],
      method = "accurate"
    )

    return(tz)
  }

  return("GMT") # Default to GMT if no valid coordinates
}
#' @title Batch Process Multiple PID Files
#' @description Main function to batch process multiple Particle Image Data (PID) files
#'   from a directory, combining results into structured datasets for analysis.
#'
#' @param directory_path Character string. Path to directory containing PID files.
#' @param file_pattern Character string. Regular expression pattern to match PID files.
#'   Default is "\\.pid$" to match files ending in .pid.
#'
#' @return A list containing:
#'   \item{metadata}{Tibble with combined metadata from all files, including normalized
#'     \code{sample_id} for each row}
#'   \item{records}{Tibble with combined iage records from all files}
#'   \item{attributes}{Tibble with combined image attributes from all files}
#'   \item{file_count}{Integer. Number of files processed}
#'   \item{processed_files}{Character vector. Names of successfully processed files}
#'
#' @details
#' This function provides comprehensive batch processing of PID files:
#' \itemize{
#'   \item Automatically discovers PID files in the specified directory
#'   \item Processes each file using \code{\link{parse_pid_file}}
#'   \item Handles errors gracefully with detailed reporting
#'   \item Combines results into unified datasets
#'   \item Provides processing summary and statistics
#' }
#'
#' The function uses safe error handling to ensure that processing continues
#' even if individual files fail to parse.
#'
#' @examples
#' \dontrun{
#' # Process all PID files in current directory
#' results <- load_pid_files(".")
#' 
#' # Process files in specific directory
#' results <- load_pid_files("/path/to/pid/files")
#' 
#' # Access combined datasets
#' metadata <- results$metadata
#' images <- results$records
#' attributes <- results$attributes
#' 
#' # Check processing summary
#' cat("Processed", results$file_count, "files\n")
#' print(results$processed_files)
#' }
#'
#' @importFrom purrr map map_dfr safely
#' @importFrom tibble tibble
#'
#' @export
#'
#' @seealso \code{\link{parse_pid_file}} for processing individual files
load_pid_files <- function(
  directory_path,
  file_pattern = "\\.pid$"
) {
  # Find all PID files in the directory
  pid_files <- list.files(
    path = directory_path,
    pattern = file_pattern,
    full.names = TRUE,
    recursive = FALSE
  )

  if (length(pid_files) == 0) {
    warning("No PID files found in directory: ", directory_path)
    return(list(
      metadata = tibble::tibble(),
      records = tibble::tibble(),
      attributes = tibble::tibble()
    ))
  }

  cat("Found", length(pid_files), "PID files to process\n")

  # Create a safe version of parse_pid_file
  safe_parse_pid <- purrr::safely(parse_pid_file)

  # Process each file and combine results
  all_results <- purrr::map(pid_files, function(file_path) {
    cat("Processing:", basename(file_path), "\n")

    result <- safe_parse_pid(file_path)

    if (is.null(result$error)) {
      # Success - return the result
      return(result$result)
    } else {
      # Error - log it and return empty tibbles
      warning(
        "Failed to process ",
        basename(file_path),
        ": ",
        result$error$message
      )
      return(list(
        image_metadata = tibble::tibble(),
        image_records = tibble::tibble(),
        image_attributes = tibble::tibble()
      ))
    }
  })

  # Combine all metadata
  combined_metadata <- purrr::map_dfr(all_results, ~ .x$image_metadata)

  # Combine all image records
  combined_records <- purrr::map_dfr(all_results, ~ .x$image_records)

  # Combine all image attributes
  combined_attributes <- purrr::map_dfr(all_results, ~ .x$image_attributes)

  cat("Processing complete!\n")
  cat("Total metadata records:", nrow(combined_metadata), "\n")
  cat("Total image records:", nrow(combined_records), "\n")
  cat("Total attribute records:", nrow(combined_attributes), "\n")

  return(list(
    metadata = combined_metadata,
    records = combined_records,
    attributes = combined_attributes,
    file_count = length(pid_files),
    processed_files = basename(pid_files)
  ))
}


#' @title Parse a Single PID File
#' @description Parses a single Particle Image Data (PID) file, extracting
#'   metadata sections and image data into structured tibbles. This function
#'   handles the complete parsing workflow for individual PID files.
#'
#' @param file_path Character string. Path to the PID file to parse.
#'
#' @return A list containing three elements:
#'   \item{image_metadata}{Tibble with metadata sections from the file header,
#'     including normalized \code{sample_id} for each row}
#'   \item{image_records}{Tibble with basic image information (scan_id, image_number)}
#'   \item{image_attributes}{Tibble with detailed image attributes and measurements}
#'
#' @details
#' The function performs the following operations:
#' \itemize{
#'   \item Reads the entire PID file line by line
#'   \item Extracts scan ID from filename
#'   \item Parses metadata sections using \code{\link{parse_metadata_sections_long}}
#'   \item Parses data section using \code{\link{parse_data_section_long}}
#'   \item Returns structured data in long format for easy analysis
#' }
#'
#' @importFrom readr read_lines
#' @importFrom stringr str_remove str_detect str_split str_remove
#' @importFrom tools file_path_sans_ext
#' @importFrom tibble tibble
#'
#' @keywords internal
#'
#' @seealso \code{\link{load_pid_files}} for batch processing multiple files
parse_pid_file <- function(file_path) {

  lines    <- readr::read_lines(file_path)
  filename <- basename(file_path)
  scan_id  <- stringr::str_remove(
    tools::file_path_sans_ext(filename),
    "_dat\\d+$"
  )

  data_start       <- which(stringr::str_detect(lines, "^\\[Data\\]$"))
  data_header_line <- data_start + 1
  data_start_line  <- data_start + 2
  metadata_lines   <- lines[1:(data_start - 1)]

  metadata <- parse_metadata_sections_long(
    metadata_lines,
    scan_id,
    filename
  )

  if (length(data_start) > 0 && data_start_line <= length(lines)) {
    header_line <- lines[data_header_line]
    headers <- stringr::str_split(stringr::str_remove(header_line, "^!"), ";")[[
      1
    ]]
    data_lines <- lines[data_start_line:length(lines)]
    data_lines <- data_lines[data_lines != ""]
    data_tables <- parse_data_section_long(
      data_lines,
      headers,
      scan_id,
      filename
    )
  } else {
    data_tables <- list(
      image_records = tibble::tibble(),
      image_attributes = tibble::tibble()
    )
  }

  return(
    list(
      image_metadata   = metadata,
      image_records    = data_tables$image_records,
      image_attributes = data_tables$image_attributes
    )
  )
}


#' @title Parse Data Section from PID File
#' @description Parses the data section of a PID file, converting semicolon-delimited
#'   data into structured tibbles for image records and attributes.
#'
#' @param data_lines Character vector. Lines containing the data section.
#' @param headers Character vector. Column headers for the data.
#' @param scan_id Character string. Unique identifier for this scan.
#' @param filename Character string. Original filename for reference.
#'
#' @return A list containing two tibbles:
#'   \item{image_records}{Basic image information with scan_id and image_number}
#'   \item{image_attributes}{Detailed attributes in long format with attribute names and values}
#'
#' @details
#' This function:
#' \itemize{
#'   \item Parses semicolon-delimited data lines
#'   \item Separates basic image info (Item, Label) from detailed attributes
#'   \item Converts wide format data to long format for analysis
#'   \item Handles missing or malformed data gracefully
#' }
#'
#' @importFrom purrr imap_dfr keep
#' @importFrom stringr str_trim str_split
#' @importFrom tibble tibble
#' @importFrom dplyr select mutate rename filter left_join first
#' @importFrom tidyr pivot_wider
#'
#' @keywords internal
parse_data_section_long <- function(data_lines, headers, scan_id, filename) {
  if (length(data_lines) == 0) {
    return(
      list(
        image_records    = tibble::tibble(),
        image_attributes = tibble::tibble()
      )
    )
  }

  parsed_data <- purrr::imap_dfr(
    purrr::keep(data_lines, ~ stringr::str_trim(.x) != ""),
    ~ {
      line <- stringr::str_trim(.x)
      values <- stringr::str_split(line, ";")[[1]]
      if (length(values) == length(headers)) {
        tibble::tibble(
          scan_id    = scan_id,
          row_number = .y,
          header     = headers,
          value      = as.character(values)
        )
      } else {
        tibble::tibble(
          scan_id    = character(0),
          row_number = integer(0),
          header     = character(0),
          value      = character(0)
        )
      }
    }
  )
  if (nrow(parsed_data) == 0) {
    return(
      list(
        image_records    = tibble::tibble(),
        image_attributes = tibble::tibble()
      )
    )
  }

  image_records <- dplyr::select(
    dplyr::mutate(
      dplyr::rename(
        tidyr::pivot_wider(
          dplyr::select(
            dplyr::filter(parsed_data, header %in% c("Item", "Label")),
            scan_id,
            row_number,
            header,
            value
          ),
          names_from  = header,
          values_from = value,
          values_fn   = dplyr::first,
          id_cols     = c(scan_id, row_number) # Keep this to preserve row_number for the join
        ),
        image_number = Item,
        image_label  = Label
      ),
      image_number = as.integer(image_number)
    ),
    scan_id,
    row_number,
    image_number
  )

  image_attributes <- dplyr::mutate(
    dplyr::select(
      dplyr::left_join(
        dplyr::filter(parsed_data, !header %in% c("Item", "Label")),
        dplyr::select(image_records, scan_id, row_number, image_number),
        by = c("scan_id", "row_number")
      ),
      scan_id,
      image_number,
      attribute_name = header,
      attribute_value = value
    ),
    attribute_name = gsub("\\.$", "", attribute_name)
  )

  return(
    list(
      image_records = if ("row_number" %in% colnames(image_records)) {
        dplyr::select(image_records, -row_number)
      } else {
        image_records
      },
      image_attributes = image_attributes
    )
  )
}


#' @title Parse Metadata Sections from PID File Header
#' @description Parses the metadata sections from PID file header lines,
#'   extracting key-value pairs organized by section headers into a long-format tibble.
#'   Also parses SampleId and Date fields from the Sample subsection to extract
#'   cruise, tow, net, and sample_date information.
#'
#' @param metadata_lines Character vector. Lines containing metadata from file header.
#' @param scan_id Character string. Unique identifier for this scan.
#' @param filename Character string. Original filename to include in metadata.
#'
#' @return A tibble with columns:
#'   \item{scan_id}{Character. Unique scan identifier}
#'   \item{section_name}{Character. Name of the metadata section}
#'   \item{key}{Character. Metadata key/parameter name}
#'   \item{value}{Character. Metadata value}
#'   \item{sample_id}{Character. Normalized SampleId value repeated for each metadata row;
#'     \code{NA} when SampleId is missing}
#'
#' @details
#' The function processes metadata by:
#' \itemize{
#'   \item Identifying section headers (lines with \code{Section Name} format)
#'   \item Parsing key=value pairs within each section
#'   \item Propagating section names to associated key-value pairs
#'   \item Adding filename as a special metadata record
#'   \item Parsing SampleId to extract cruise, tow, and net components
#'   \item Parsing Date field from Sample section to extract sample_date
#'   \item Adding normalized \code{sample_id} column to all metadata rows
#'   \item Warning when SampleId is missing (with \code{sample_id = NA})
#'   \item Returning data in long format for easy filtering and analysis
#' }
#'
#' @importFrom tibble tibble
#' @importFrom purrr imap_dfr map_chr
#' @importFrom stringr str_trim str_detect str_remove_all str_split
#' @importFrom dplyr filter select mutate bind_rows
#'
#' @keywords internal
parse_metadata_sections_long <- function(metadata_lines, scan_id, filename) {

  # add filename as a single record
  filename_record <- tibble::tibble(
    scan_id      = scan_id,
    section_name = "file_info",
    key          = "filename",
    value        = filename
  )

  # process metadata lines using purrr
  metadata_records <- purrr::imap_dfr(metadata_lines, function(line, index) {
    line <- stringr::str_trim(line)

    # Skip empty lines
    if (line == "") {
      return(tibble::tibble())
    }

    # Check if this is a section header
    if (stringr::str_detect(line, "^\\[.*\\]$")) {
      return(tibble::tibble(
        line_index   = index,
        type         = "section_header",
        section_name = stringr::str_remove_all(line, "\\[|\\]"),
        key          = NA_character_,
        value        = NA_character_
      ))
    } else {
      # Parse key-value pairs
      parts <- stringr::str_split(line, "=", n = 2)[[1]]
      if (length(parts) == 2) {
        return(
          tibble::tibble(
            line_index   = index,
            type         = "key_value",
            section_name = NA_character_,
            key          = stringr::str_trim(parts[1]),
            value        = stringr::str_trim(parts[2])
          )
        )
      }
    }

    return(tibble::tibble())
  })

  # Use cumulative maximum to propagate section names forward
  if (nrow(metadata_records) > 0) {
    # Create a vector of section names where each section header fills forward
    section_indices <- which(metadata_records$type == "section_header")
    section_names <- metadata_records$section_name[section_indices]

    # Use map to assign section names to each row
    metadata_records$section_name <- purrr::map_chr(
      seq_len(nrow(metadata_records)),
      function(i) {
        # Find the most recent section header before this row
        recent_section_idx <- max(c(0, section_indices[section_indices <= i]))
        if (recent_section_idx > 0) {
          return(metadata_records$section_name[recent_section_idx])
        } else {
          return(NA_character_)
        }
      }
    )

    # Filter to only key-value pairs and clean up
    metadata_records <- metadata_records |>
      dplyr::filter(type == "key_value", !is.na(key), !is.na(section_name)) |>
      dplyr::select(section_name, key, value) |>
      dplyr::mutate(scan_id = scan_id)
  } else {
    metadata_records <- tibble::tibble(
      scan_id      = character(0),
      section_name = character(0),
      key          = character(0),
      value        = character(0)
    )
  }

  # Parse SampleId and Date from Sample section
  parsed_fields <- parse_sample_fields(metadata_records, scan_id)

  # Derive normalized sample_id from Sample/SampleId metadata
  sample_id_rows <- metadata_records |>
    dplyr::filter(section_name == "Sample", key == "SampleId") |>
    dplyr::filter(!is.na(value), value != "")

  sample_id_value <- if (nrow(sample_id_rows) > 0) {
    sample_id_rows$value[[1]]
  } else {
    warning(
      "SampleId missing in PID metadata for file: ",
      filename,
      "; setting sample_id to NA"
    )
    NA_character_
  }

  # Combine all metadata records
  metadata <- dplyr::bind_rows(filename_record, metadata_records, parsed_fields) |>
    dplyr::mutate(sample_id = sample_id_value)

  return(metadata)
}


#' @title Parse Sample Fields from Metadata
#' @description Parses SampleId and Date fields from the Sample section of metadata
#'   to extract cruise, tow, net, and sample_date components using purrr functional programming.
#'
#' @param metadata_records Tibble. Processed metadata records.
#' @param scan_id Character string. Unique identifier for this scan.
#'
#' @return A tibble with parsed fields added as new metadata records.
#'
#' @importFrom tibble tibble
#' @importFrom dplyr filter pull bind_rows
#' @importFrom stringr str_extract str_sub str_match
#' @importFrom lubridate ymd
#' @importFrom purrr map_dfr pmap_dfr keep compact
#'
#' @keywords internal
parse_sample_fields <- function(metadata_records, scan_id) {
  
  # Define SampleId patterns (similar to parse_cruise_id approach)
  sample_id_patterns <- list(
    pattern1 = "^([a-zA-Z]+[0-9]+)_([a-zA-Z])([0-9]+)_([a-zA-Z])([0-9]+)$"  # e.g., sr2408_m13_n8
    # Add more patterns here as needed in the future
  )
  
  # Parse SampleId using purrr
  sample_id_records <- metadata_records |>
    dplyr::filter(section_name == "Sample", key == "SampleId") |>
    purrr::pmap_dfr(function(scan_id, section_name, key, value, ...) {
      
      # Try each pattern and return the first match
      pattern_results <- sample_id_patterns |>
        purrr::map(~ stringr::str_match(value, .x)) |>
        purrr::keep(~ !is.na(.x[1]))
      
      if (length(pattern_results) > 0) {
        # Use first successful match
        matches <- pattern_results[[1]]
        
        # Create records for parsed components
        # matches[1] = full match
        # matches[2] = cruise (e.g., "sr2408")
        # matches[3] = tow label letter (e.g., "m") 
        # matches[4] = tow number (e.g., "13")
        # matches[5] = net letter (e.g., "n")
        # matches[6] = net number (e.g., "8")
        tibble::tibble(
          scan_id = rep(scan_id, 3),
          section_name = rep("parsed_sample", 3),
          key = c("cruise", "tow", "net"),
          value = c(matches[2], matches[4], matches[6])
        )
      } else {
        # No pattern matched - return empty tibble
        tibble::tibble(
          scan_id = character(0),
          section_name = character(0),
          key = character(0),
          value = character(0)
        )
      }
    })
  
  # Parse Date field using purrr
  sample_date_records <- metadata_records |>
    dplyr::filter(section_name == "Sample", key == "Date") |>
    purrr::pmap_dfr(function(scan_id, section_name, key, value, ...) {
      
      # Extract date part (YYYYMMDD) before hyphen
      date_part <- stringr::str_extract(value, "^[0-9]{8}")
      
      if (!is.na(date_part)) {
        # Convert to ISO date format
        sample_date <- tryCatch({
          formatted_date <- paste0(
            stringr::str_sub(date_part, 1, 4), "-",    # YYYY
            stringr::str_sub(date_part, 5, 6), "-",    # MM  
            stringr::str_sub(date_part, 7, 8)          # DD
          )
          as.character(lubridate::ymd(formatted_date))
        }, error = function(e) NA_character_)
        
        if (!is.na(sample_date)) {
          tibble::tibble(
            scan_id = scan_id,
            section_name = "parsed_sample",
            key = "sample_date",
            value = sample_date
          )
        } else {
          # Date conversion failed
          tibble::tibble(
            scan_id = character(0),
            section_name = character(0),
            key = character(0),
            value = character(0)
          )
        }
      } else {
        # No valid date part found
        tibble::tibble(
          scan_id = character(0),
          section_name = character(0),
          key = character(0),
          value = character(0)
        )
      }
    })
  
  # Combine all parsed records
  parsed_records <- dplyr::bind_rows(sample_id_records, sample_date_records)
  
  return(parsed_records)
}
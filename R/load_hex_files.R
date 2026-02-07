#' @title Load Sea-Bird HEX headers
#'
#' @description
#' Read Sea-Bird `.hex` files (MOCNESS configuration captures) and extract
#' header metadata only. No scan payload is parsed. The function pair includes
#' a single-file ingester and a batch loader.
#'
#' @param file_path Character. Path to a single `.hex` file to ingest.
#' @param directory Character. Directory containing `.hex` files (non-recursive).
#' @param pattern Character. Regex used to identify hex files (default `"\\.hex$"`).
#' @param progress Logical. Emit progress messages when `TRUE` (default `TRUE`).
#'
#' @return
#' * `ingest_hex_file()` returns a one-row tibble with columns: `file_path`,
#'   `file_name`, `header_file_name` (as written in the file header), `ship`,
#'   `cruise`, `tow`, and `net_size`.
#' * `load_hex_files()` returns a tibble with rows bound across all successfully
#'   ingested files. Missing fields are filled with `NA` and trigger warnings.
#'
#' @details
#' - Header lines may begin with any number of leading asterisks; matching is
#'   case-insensitive and tolerant to extra whitespace.
#' - If required fields are absent, the ingester issues a warning and returns
#'   `NA` for those columns instead of failing.
#' - The batch loader scans the target directory only (no recursion), warns on
#'   per-file failures, and is silent unless `progress = TRUE`.
#'
#' @examples
#' \dontrun{
#' # Single file
#' ingest_hex_file("path/to/M28_01.hex")
#'
#' # Batch
#' load_hex_files(directory = "path/to/hex/")
#' }
#'
#' @importFrom readr read_lines
#' @importFrom stringr str_detect str_match str_trim
#' @importFrom purrr map compact
#' @importFrom dplyr bind_rows
#' @importFrom tibble tibble
#'
#' @export
ingest_hex_file <- function(file_path) {

  lines <- readr::read_lines(file_path)

  end_idx <- which(stringr::str_detect(lines, "^\\*END\\*"))
  header_lines <- if (length(end_idx) > 0) lines[seq_len(end_idx[1] - 1)] else lines
  header_lines <- stringr::str_trim(header_lines)

  extract_field <- function(keyword) {
    pattern <- stringr::regex(paste0("^\\*+\\s*", keyword, "\\s*[:=]\\s*(.+)$"),
                              ignore_case = TRUE)
    match_idx <- which(stringr::str_detect(header_lines, pattern))
    if (length(match_idx) == 0) {
      return(NA_character_)
    }
    value <- stringr::str_match(header_lines[match_idx[1]], pattern)[, 2]
    stringr::str_trim(value)
  }

  header_file_name <- extract_field("FileName")
  ship             <- extract_field("Ship")
  cruise           <- extract_field("Cruise")
  tow              <- extract_field("Tow")
  net_size_chr     <- extract_field("Net\\s*Size")

  net_size <- suppressWarnings(as.numeric(net_size_chr))

  missing_fields <- c(
    if (is.na(header_file_name)) "header_file_name" else NULL,
    if (is.na(ship))             "ship" else NULL,
    if (is.na(cruise))           "cruise" else NULL,
    if (is.na(tow))              "tow" else NULL,
    if (is.na(net_size_chr) || is.na(net_size)) "net_size" else NULL
  )

  if (length(missing_fields) > 0) {
    warning(
      "Missing fields in ", basename(file_path), ": ",
      paste(missing_fields, collapse = ", ")
    )
  }

  tibble::tibble(
    file_path        = file_path,
    file_name        = basename(file_path),
    header_file_name = header_file_name,
    ship             = ship,
    cruise           = cruise,
    tow              = tow,
    net_size         = net_size
  )
}


#' @rdname ingest_hex_file
#' @export
load_hex_files <- function(
  directory = ".",
  pattern   = "\\.hex$",
  progress  = TRUE
) {

  file_paths <- list.files(
    path       = directory,
    pattern    = pattern,
    full.names = TRUE,
    recursive  = FALSE
  )

  if (length(file_paths) == 0) {
    warning("No HEX files found in directory: ", directory)
    return(tibble::tibble())
  }

  if (progress) {
    message("Processing ", length(file_paths), " hex files...")
  }

  process_one <- function(path) {
    if (progress) {
      message("- ", basename(path))
    }
    tryCatch(
      ingest_hex_file(path),
      error = function(e) {
        warning("Failed to process ", basename(path), ": ", e$message)
        NULL
      }
    )
  }

  results <- purrr::map(file_paths, process_one) |> purrr::compact()

  if (length(results) == 0) {
    warning("No HEX files were successfully processed")
    return(tibble::tibble())
  }

  dplyr::bind_rows(results)
}